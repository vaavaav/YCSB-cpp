#include "cachelib-holpaca.h"
#include "core/db_factory.h"
#include <cachelib/allocator/HitsPerSlabStrategy.h>
#include <cachelib/allocator/MarginalHitsOptimizeStrategy.h>

namespace {

const std::string PROP_CACHE_NAME = "cachelib.name";
const std::string PROP_CACHE_NAME_DEFAULT = "CacheLib";

const std::string PROP_CACHE_EVICTION = "cachelib.eviction";
const std::string PROP_CACHE_EVICTION_DEFAULT = "lru"; // or 2q

const std::string PROP_CONTROLLER_ADDRESS = "cachelib.controller.address";
const std::string PROP_CONTROLLER_ADDRESS_DEFAULT = "";

const std::string PROP_STAGE_ADDRESS = "cachelib.stage.address";
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

  std::lock_guard<std::mutex> lock(mutex_);
  cacheName_ = props_->GetProperty(
      PROP_CACHE_NAME + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_CACHE_NAME, PROP_CACHE_NAME_DEFAULT));

  if (auto it = caches_.find(cacheName_); it != caches_.end()) {
    // already initialized (two threads can point to the same cache)
    cache_ = it->second;
    rocksdb_ = rocksdbs_[cacheName_];
  } else {
    Config config;
    if (props_->GetProperty(
            PROP_CACHE_EVICTION + "." + std::to_string(threadId_),
            props_->GetProperty(PROP_CACHE_EVICTION,
                                PROP_CACHE_EVICTION_DEFAULT)) == "lru") {
      CacheLRU::Config configlru;
      config = configlru;
    } else if (props_->GetProperty(
                   PROP_CACHE_EVICTION + "." + std::to_string(threadId_),
                   props_->GetProperty(PROP_CACHE_EVICTION,
                                       PROP_CACHE_EVICTION_DEFAULT)) == "2q") {
      Cache2Q::Config config2q;
      config = config2q;
    } else {
      throw std::runtime_error("Unknown eviction policy");
    }
    std::visit(
        [&](auto &config) {
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
              .setAccessConfig({25 /* bucket power */,
                                10 /* lock power */}); // assuming caching
                                                       // 20 million items
          // Needed for pool resizing
          if (props_->GetProperty(
                  PROP_POOL_RESIZER + "." + std::to_string(threadId_),
                  props_->GetProperty(PROP_POOL_RESIZER,
                                      PROP_POOL_RESIZER_DEFAULT)) == "on") {
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
                std::make_shared<
                    facebook::cachelib::MarginalHitsOptimizeStrategy>(),
                std::chrono::seconds(1), std::chrono::seconds(1), 0);
          }
          config.validate(); // will throw if bad config
        },
        config);
    if (props_->GetProperty(
            PROP_CACHE_EVICTION + "." + std::to_string(threadId_),
            props_->GetProperty(PROP_CACHE_EVICTION,
                                PROP_CACHE_EVICTION_DEFAULT)) == "lru") {
      cache_ = std::make_shared<Cache>(std::get<CacheLRU::Config>(config));

    } else if (props_->GetProperty(
                   PROP_CACHE_EVICTION + "." + std::to_string(threadId_),
                   props_->GetProperty(PROP_CACHE_EVICTION,
                                       PROP_CACHE_EVICTION_DEFAULT)) == "2q") {
      cache_ = std::make_shared<Cache>(std::get<Cache2Q::Config>(config));
    }
    caches_[cacheName_] = cache_;
    rocksdb_.SetProps(props_);
    rocksdb_.Init();
    rocksdbs_[cacheName_] = rocksdb_;
  }
  std::string poolName = props_->GetProperty(
      PROP_POOL_NAME + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_POOL_NAME, PROP_POOL_NAME_DEFAULT));
  auto poolSize = std::stol(props_->GetProperty(
      PROP_POOL_SIZE + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_POOL_SIZE, PROP_POOL_SIZE_DEFAULT)));
  std::visit(
      [&poolName, &poolSize](auto &&cache) {
        CacheLibHolpaca::poolId_ = cache.addPool(
            poolName, static_cast<long>(
                          cache.getCacheMemoryStats().ramCacheSize * poolSize));
      },
      *cache_);
  lastTime_ = std::chrono::high_resolution_clock::now();
}

// std::tuple<uint64_t, uint64_t> CacheLibHolpaca::OccupancyAndCapacity() {
//   if (cache_ == nullptr) {
//     return std::make_tuple(0, 0);
//   }
//   const auto &pool = cache_->getPool(poolId_);
//   auto now = std::chrono::high_resolution_clock::now();
//   cache_->registerMetrics(
//       poolId_, rocksdbIOPS_ /
//       std::chrono::duration_cast<std::chrono::seconds>(
//                                   now - lastTime_)
//                                   .count());
//   lastTime_ = now;
//   return std::make_tuple(pool.getCurrentAllocSize(),
//   pool.getPoolUsableSize());
// }

DB::Status CacheLibHolpaca::Read(const std::string &table,
                                 const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  std::lock_guard<std::mutex> lock(mutex_);
  return std::visit(
      [&table, &key, &fields, &result](auto &&cache) {
        auto handle = cache.find(key);
        if (handle == nullptr) {
          rocksdbIOPS_++;
          if (rocksdb_.Read(table, key, fields, result) == kOK) {
            uint32_t size = result.front().value.size();
            auto new_handle = cache.allocate(poolId_, key, size);
            if (new_handle == nullptr) {
              return kError;
            }
            std::memcpy(new_handle->getMemory(), result.front().value.data(),
                        size);
            cache.insertOrReplace(new_handle);
            cache.registerAccess(poolId_, key, size, true, true, false);
          } else {
          }
          return kNotFound;
        } else {
          auto size = handle->getSize();
          cache.registerAccess(poolId_, key, size, true, false, false);
        }
        return kOK;
      },
      *cache_);
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
    std::visit(
        [&key, &size](auto &&cache) {
          cache.registerAccess(poolId_, key, size, false, true, false);
        },
        *cache_);
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
    std::visit(
        [&key, &size](auto &&cache) {
          cache.registerAccess(poolId_, key, size, false, false, true);
        },
        *cache_);
  }
  return res;
}

DB::Status CacheLibHolpaca::Delete(const std::string &table,
                                   const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto key_ = key;
  // return cache_->remove(key_) == Cache::RemoveRes::kSuccess ? kOK :
  // kNotFound;
  return kOK;
}

DB *NewCacheLibHolpaca() { return new CacheLibHolpaca(); }

const bool registered =
    DBFactory::RegisterDB("cachelib-holpaca", NewCacheLibHolpaca);

void CacheLibHolpaca::SetThreadId(int threadId) { threadId_ = threadId; }

} // namespace ycsbc
