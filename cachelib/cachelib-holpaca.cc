#include "cachelib-holpaca.h"
#include "cachelib/allocator/HitsPerSlabStrategy.h"
#include "cachelib/allocator/MarginalHitsOptimizeStrategy.h"
#include "cachelib/allocator/PoolOptimizeStrategy.h"
#include "core/db_factory.h"

namespace {

const std::string PROP_CONTROLLER_ADDRESS =
    "cachelib-holpaca.controller.address";
const std::string PROP_STAGE_ADDRESS = "cachelib-holpaca.stage.address";

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

} // namespace

namespace ycsbc {

std::mutex CacheLibHolpaca::mutex_;
RocksDB CacheLibHolpaca::rocksdb_;
std::shared_ptr<CacheLibHolpaca::Cache> CacheLibHolpaca::cache_ = nullptr;
thread_local int CacheLibHolpaca::threadId_;
thread_local facebook::cachelib::PoolId CacheLibHolpaca::poolId_;
int CacheLibHolpaca::ref_cnt_ = 0;

void CacheLibHolpaca::Init() {

  std::lock_guard<std::mutex> lock(mutex_);
  if (cache_ == nullptr) {
    Cache::Config config;
    config
        .setControllerAddress(
            props_->GetProperty(PROP_CONTROLLER_ADDRESS, "localhost:11211"))
        .setAddress(
            props_->GetProperty(PROP_CONTROLLER_ADDRESS, "localhost:11212"))
        .m_config
        .setCacheSize(
            std::stol(props_->GetProperty(PROP_SIZE, PROP_SIZE_DEFAULT)))
        .setCacheName("My Use Case")
        .setAccessConfig(
            {25 /* bucket power */, 10 /* lock power */}); // assuming caching
                                                           // 20 million items
    if (props_->GetProperty(PROP_POOL_RESIZER, PROP_POOL_RESIZER_DEFAULT) ==
        "on") {
      config.m_config.enablePoolResizing(
          std::make_shared<facebook::cachelib::HitsPerSlabStrategy>(
              facebook::cachelib::HitsPerSlabStrategy::Config(
                  0.25, static_cast<unsigned int>(1))),
          std::chrono::milliseconds(100), 1);
    }
    if (props_->GetProperty(PROP_POOL_OPTIMIZER, PROP_POOL_OPTIMIZER_DEFAULT) ==
        "on") {
      config.m_config.enableTailHitsTracking();
      config.m_config.enablePoolOptimizer(
          std::make_shared<facebook::cachelib::MarginalHitsOptimizeStrategy>(),
          std::chrono::seconds(1), std::chrono::seconds(1), 0);
    }
    config.validate(); // will throw if bad config
    cache_ = std::make_unique<Cache>(config);
    rocksdb_.SetProps(props_);
    rocksdb_.Init();
  }
  CacheLibHolpaca::poolId_ = cache_->addPool(
      props_->GetProperty(PROP_POOL_NAME + "." + std::to_string(threadId_),
                          PROP_POOL_NAME_DEFAULT),
      static_cast<long>(cache_->getCacheMemoryStats().ramCacheSize *
                        std::stod(props_->GetProperty(
                            PROP_POOL_SIZE + "." + std::to_string(threadId_),
                            PROP_POOL_SIZE_DEFAULT))));
}

DB::Status CacheLibHolpaca::Read(const std::string &table,
                                 const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  auto key_ = key;
  auto value = cache_->get(CacheLibHolpaca::poolId_, key_);
  if (value.empty()) {
    if (rocksdb_.Read(table, key, fields, result) == kOK) {
      std::string newValue = result.front().value;
      if (!cache_->put(poolId_, key_, newValue)) {
        return kError;
      }
    }
    return kNotFound;
  }

  volatile std::string data = value;

  return kOK;
}

DB::Status CacheLibHolpaca::Scan(const std::string &table,
                                 const std::string &key, long len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {

  // TODO

  return kError;
}

DB::Status CacheLibHolpaca::Update(const std::string &table,
                                   const std::string &key,
                                   std::vector<Field> &values) {
  std::string data = values.front().value;
  auto key_ = key;

  bool success = cache_->put(CacheLibHolpaca::poolId_, key_, data);
  if (success) {
    rocksdb_.Update(table, key, values);
    return kOK;
  } else {
    return kError;
  }
  // TODO: confirm if this is the correct behavior
}

DB::Status CacheLibHolpaca::Insert(const std::string &table,
                                   const std::string &key,
                                   std::vector<Field> &values) {
  std::string data = values.front().value;
  auto key_ = key;

  if (!cache_->put(CacheLibHolpaca::poolId_, key_, data)) {
    return kError;
  }
  return rocksdb_.Insert(table, key, values);
}

DB::Status CacheLibHolpaca::Delete(const std::string &table,
                                   const std::string &key) {
  auto key_ = key;
  return cache_->remove(key_) == Cache::RemoveRes::kSuccess ? kOK : kNotFound;
}

DB *NewCacheLibHolpaca() { return new CacheLibHolpaca(); }

const bool registered =
    DBFactory::RegisterDB("cachelib-holpaca", NewCacheLibHolpaca);
void CacheLibHolpaca::SetThreadId(int threadId) { threadId_ = threadId; }

} // namespace ycsbc
