#include "cachelib.h"
#include "cachelib/allocator/HitsPerSlabStrategy.h"
#include "cachelib/allocator/MarginalHitsOptimizeStrategy.h"
#include "cachelib/allocator/PoolOptimizeStrategy.h"
#include "core/db_factory.h"

namespace {
const std::string PROP_SIZE = "cachelib.size";
const std::string PROP_SIZE_DEFAULT = "1000000000";

const std::string PROP_POOL_NAME = "cachelib.pool.name";
const std::string PROP_POOL_NAME_DEFAULT = "default";

const std::string PROP_POOL_SIZE = "cachelib.pool.relsize";
const std::string PROP_POOL_SIZE_DEFAULT = "1000000000";

const std::string PROP_POOL_RESIZER = "cachelib.poolresizer";
const std::string PROP_POOL_RESIZER_DEFAULT = "off";

const std::string PROP_POOL_OPTIMIZER = "cachelib.pooloptimizer";
const std::string PROP_POOL_OPTIMIZER_DEFAULT = "off";

const std::string PROP_HIT_RATIO_MAXIMIZATION = "cachelib.hitratiomaximization";
const std::string PROP_HIT_RATIO_MAXIMIZATION_DEFAULT = "off";
} // namespace

namespace ycsbc {

std::mutex CacheLib::mutex_;
RocksDB CacheLib::rocksdb_;
thread_local facebook::cachelib::PoolId CacheLib::poolId_;
thread_local int CacheLib::threadId_;
std::shared_ptr<CacheLib::Cache> CacheLib::cache_ = nullptr;
int CacheLib::ref_cnt_ = 0;

void CacheLib::Init() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (cache_ == nullptr) {
    CacheConfig config;
    config
        .setCacheSize(
            std::stol(props_->GetProperty(PROP_SIZE, PROP_SIZE_DEFAULT)))
        .setCacheName("My Use Case")
        .setAccessConfig(
            {25 /* bucket power */, 10 /* lock power */}); // assuming caching
                                                           // 20 million items
    if (props_->GetProperty(PROP_POOL_RESIZER, PROP_POOL_RESIZER_DEFAULT) ==
        "on") {
      config.enablePoolResizing(
          std::make_shared<facebook::cachelib::HitsPerSlabStrategy>(
              facebook::cachelib::HitsPerSlabStrategy::Config(
                  0.25, static_cast<unsigned int>(1))),
          std::chrono::milliseconds(100), 1);
    }
    if (props_->GetProperty(PROP_POOL_OPTIMIZER, PROP_POOL_OPTIMIZER_DEFAULT) ==
        "on") {
      config.enableTailHitsTracking();
      config.enablePoolOptimizer(
          std::make_shared<facebook::cachelib::MarginalHitsOptimizeStrategy>(),
          std::chrono::seconds(1), std::chrono::seconds(1), 0);
    }
    if (props_->GetProperty(PROP_HIT_RATIO_MAXIMIZATION,
                            PROP_HIT_RATIO_MAXIMIZATION_DEFAULT) == "on") {
      // TODO
    }
    config.validate(); // will throw if bad config
    cache_ = std::make_unique<Cache>(config);
    rocksdb_.SetProps(props_);
    rocksdb_.Init();
  }

  poolId_ = cache_->addPool(
      props_->GetProperty(PROP_POOL_NAME + "." + std::to_string(threadId_),
                          PROP_POOL_NAME_DEFAULT),
      static_cast<long>(cache_->getCacheMemoryStats().ramCacheSize *
                        std::stod(props_->GetProperty(
                            PROP_POOL_SIZE + "." + std::to_string(threadId_),
                            PROP_POOL_SIZE_DEFAULT))));
}

DB::Status CacheLib::Read(const std::string &table, const std::string &key,
                          const std::vector<std::string> *fields,
                          std::vector<Field> &result) {
  auto value = cache_->find(key);
  if (value == nullptr) {
    if (rocksdb_.Read(table, key, fields, result) == kOK) {
      std::string newValue = result.front().value;
      auto handle = cache_->allocate(poolId_, key, newValue.size());
      if (handle == nullptr) {
        return kError;
      }
      std::memcpy(handle->getMemory(), newValue.data(), newValue.size());
      cache_->insert(handle);
    }
    return kNotFound;
  }

  volatile auto data = folly::StringPiece{
      reinterpret_cast<const char *>(value->getMemory()), value->getSize()};

  return kOK;
}

DB::Status CacheLib::Scan(const std::string &table, const std::string &key,
                          long len, const std::vector<std::string> *fields,
                          std::vector<std::vector<Field>> &result) {

  // TODO

  return kError;
}

DB::Status CacheLib::Update(const std::string &table, const std::string &key,
                            std::vector<Field> &values) {
  std::string data = values.front().value;
  auto value = cache_->find(key);
  if (value == nullptr) {
    return kNotFound;
  }
  auto handle = cache_->allocate(poolId_, key, data.size());
  if (handle == nullptr) {
    return kError;
  }
  std::memcpy(handle->getMemory(), data.data(), data.size());
  cache_->insertOrReplace(handle);
  return rocksdb_.Update(table, key, values);
}

DB::Status CacheLib::Insert(const std::string &table, const std::string &key,
                            std::vector<Field> &values) {
  std::string data = values.front().value;
  auto handle = cache_->allocate(poolId_, key, data.size());

  if (handle == nullptr) {
    return kError;
  }
  std::memcpy(handle->getMemory(), data.data(), data.size());
  cache_->insertOrReplace(handle);

  return rocksdb_.Insert(table, key, values);
}

DB::Status CacheLib::Delete(const std::string &table, const std::string &key) {
  auto success = cache_->remove(key);
  return success == Cache::RemoveRes::kSuccess ? kOK : kNotFound;
}

DB *NewCacheLib() { return new CacheLib(); }

const bool registered = DBFactory::RegisterDB("cachelib", NewCacheLib);
void CacheLib::SetThreadId(int threadId) { threadId_ = threadId; }

} // namespace ycsbc
