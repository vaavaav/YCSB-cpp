#include "cachelib-holpaca.h"
#include "core/db_factory.h"
#include <cachelib/allocator/HitsPerSlabStrategy.h>
#include <cachelib/allocator/MarginalHitsOptimizeStrategy.h>

namespace {

const std::string PROP_CACHE_NAME = "cachelib-holpaca.cache.name";
const std::string PROP_CACHE_NAME_DEFAULT = "CacheLib";

const std::string PROP_CONTROLLER_ADDRESS =
    "cachelib-holpaca.controller.address";
const std::string PROP_CONTROLLER_ADDRESS_DEFAULT = "";

const std::string PROP_STAGE_ADDRESS = "cachelib-holpaca.stage.address";
const std::string PROP_STAGE_ADDRESS_DEFAULT = "";

const std::string PROP_SIZE = "cachelib.size";
const std::string PROP_SIZE_DEFAULT = "1000000000";

const std::string PROP_POOL_NAME = "cachelib.pool.name";
const std::string PROP_POOL_NAME_DEFAULT = "default";

const std::string PROP_POOL_SIZE = "cachelib.pool.relsize";
const std::string PROP_POOL_SIZE_DEFAULT = "1";

const std::string PROP_POOL_OPTIMIZER = "cachelib.pooloptimizer";
const std::string PROP_POOL_OPTIMIZER_DEFAULT = "off";

const std::string PROP_POOL_RESIZER = "cachelib.poolresizer";
const std::string PROP_POOL_RESIZER_DEFAULT = "off";
} // namespace

namespace ycsbc {

std::mutex CacheLibHolpaca::mutex_;
thread_local facebook::cachelib::PoolId CacheLibHolpaca::poolId_;
int CacheLibHolpaca::ref_cnt_ = 0;
thread_local int CacheLibHolpaca::rocksdbIOPS_ = 0;
thread_local std::chrono::time_point<std::chrono::high_resolution_clock>
    CacheLibHolpaca::lastTime_ = std::chrono::high_resolution_clock::now();
std::unordered_map<std::string, RocksDB> CacheLibHolpaca::rocksdbs_;
std::unordered_map<std::string, std::shared_ptr<CacheLibHolpaca::Cache>>
    CacheLibHolpaca::caches_;
thread_local std::string CacheLibHolpaca::cacheName_;
thread_local std::shared_ptr<CacheLibHolpaca::Cache> CacheLibHolpaca::cache_;
thread_local RocksDB CacheLibHolpaca::rocksdb_;
thread_local int CacheLibHolpaca::threadId_;
thread_local static facebook::cachelib::PoolId poolId_;

void CacheLibHolpaca::Init() {

  cacheName_ = props_->GetProperty(
      PROP_CACHE_NAME + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_CACHE_NAME, PROP_CACHE_NAME_DEFAULT));

  std::lock_guard<std::mutex> lock(mutex_);
  if (auto it = caches_.find(cacheName_); it != caches_.end()) {
    // already initialized (two threads can point to the same cache)
    cache_ = it->second;
    rocksdb_ = rocksdbs_[cacheName_];
  } else {
    Cache::Config config;
    config
        .setControllerAddress(props_->GetProperty(
            PROP_CONTROLLER_ADDRESS + "." + std::to_string(threadId_),
            props_->GetProperty(PROP_CONTROLLER_ADDRESS,
                                PROP_CONTROLLER_ADDRESS_DEFAULT)))
        .setAddress(props_->GetProperty(
            PROP_STAGE_ADDRESS + "." + std::to_string(threadId_),
            props_->GetProperty(PROP_STAGE_ADDRESS,
                                PROP_STAGE_ADDRESS_DEFAULT)))
        .setCacheSize(std::stol(props_->GetProperty(
            PROP_SIZE + "." + std::to_string(threadId_),
            props_->GetProperty(PROP_SIZE, PROP_SIZE_DEFAULT))))
        .setCacheName(cacheName_)
        .setAccessConfig(
            {25 /* bucket power */, 10 /* lock power */}); // assuming caching
                                                           // 20 million items
    // Needed for pool resizing
    if (props_->GetProperty(PROP_POOL_RESIZER + "." + std::to_string(threadId_),
                            props_->GetProperty(PROP_POOL_RESIZER,
                                                PROP_POOL_RESIZER_DEFAULT)) ==
        "on") {
      config.enablePoolResizing(
          std::make_shared<facebook::cachelib::HitsPerSlabStrategy>(
              facebook::cachelib::HitsPerSlabStrategy::Config(
                  0.25, static_cast<unsigned int>(1))),
          std::chrono::milliseconds(100), 1);
    }
    if (props_->GetProperty(
            PROP_POOL_OPTIMIZER + "." + std::to_string(threadId_),
            props_->GetProperty(PROP_POOL_OPTIMIZER,
                                PROP_POOL_OPTIMIZER_DEFAULT)) == "on") {
      config.enableTailHitsTracking(); // needed for tracking tail hits
      config.enablePoolOptimizer(
          std::make_shared<facebook::cachelib::MarginalHitsOptimizeStrategy>(),
          std::chrono::seconds(1), std::chrono::seconds(1), 0);
    }
    config.validate(); // will throw if bad config
    cache_ = std::make_shared<Cache>(config);
    caches_[cacheName_] = cache_;
    rocksdb_.SetProps(props_);
    rocksdb_.Init();
    rocksdbs_[cacheName_] = rocksdb_;
  }
  CacheLibHolpaca::poolId_ = cache_->addPool(
      props_->GetProperty(PROP_POOL_NAME + "." + std::to_string(threadId_),
                          PROP_POOL_NAME_DEFAULT),
      static_cast<long>(cache_->getCacheMemoryStats().ramCacheSize *
                        std::stod(props_->GetProperty(
                            PROP_POOL_SIZE + "." + std::to_string(threadId_),
                            PROP_POOL_SIZE_DEFAULT))));
  lastTime_ = std::chrono::high_resolution_clock::now();
}

std::tuple<uint64_t, uint64_t> CacheLibHolpaca::OccupancyAndCapacity() {
  if (cache_ == nullptr) {
    return std::make_tuple(0, 0);
  }
  const auto &pool = cache_->getPool(poolId_);
  auto now = std::chrono::high_resolution_clock::now();
  cache_->registerMetrics(
      poolId_, rocksdbIOPS_ / std::chrono::duration_cast<std::chrono::seconds>(
                                  now - lastTime_)
                                  .count());
  lastTime_ = now;
  return std::make_tuple(pool.getCurrentAllocSize(), pool.getPoolUsableSize());
}

DB::Status CacheLibHolpaca::Read(const std::string &table,
                                 const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto handle = cache_->find(key);
  if (handle == nullptr) {
    rocksdbIOPS_++;
    if (rocksdb_.Read(table, key, fields, result) == kOK) {
      uint32_t size = result.front().value.size();
      auto new_handle = cache_->allocate(poolId_, key, size);
      if (handle == nullptr) {
        return kError;
      }
      std::memcpy(new_handle->getMemory(), result.front().value.data(), size);
      cache_->insertOrReplace(new_handle);
      cache_->registerAccess(poolId_, key, size, true, true, false);
    }
    return kNotFound;
  } else {
    auto size = handle->getSize();
    cache_->registerAccess(poolId_, key, size, true, false, false);
  }

  return kOK;
}

DB::Status CacheLibHolpaca::Scan(const std::string &table,
                                 const std::string &key, long len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {

  std::lock_guard<std::mutex> lock(mutex_);
  // TODO

  return kError;
}

DB::Status CacheLibHolpaca::Update(const std::string &table,
                                   const std::string &key,
                                   std::vector<Field> &values) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string data = values.front().value;
  auto key_ = key;
  uint32_t size = values.front().value.size();
  auto res = rocksdb_.Update(table, key, values);
  rocksdbIOPS_++;
  if (res == kOK) {
    cache_->registerAccess(poolId_, key, size, false, false, true);
  }

  return res;
}

DB::Status CacheLibHolpaca::Insert(const std::string &table,
                                   const std::string &key,
                                   std::vector<Field> &values) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string data = values.front().value;
  auto key_ = key;
  uint32_t size = values.front().value.size();
  auto res = rocksdb_.Insert(table, key, values);
  rocksdbIOPS_++;
  if (res == kOK) {
    cache_->registerAccess(poolId_, key, size, false, false, true);
  }
  return res;
}

DB::Status CacheLibHolpaca::Delete(const std::string &table,
                                   const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto key_ = key;
  return cache_->remove(key_) == Cache::RemoveRes::kSuccess ? kOK : kNotFound;
}

DB *NewCacheLibHolpaca() { return new CacheLibHolpaca(); }

const bool registered =
    DBFactory::RegisterDB("cachelib-holpaca", NewCacheLibHolpaca);

void CacheLibHolpaca::SetThreadId(int threadId) { threadId_ = threadId; }

} // namespace ycsbc
