#include "cachelib-holpaca.h"
#include "core/db_factory.h"
#include <cachelib/allocator/HitsPerSlabStrategy.h>
#include <cachelib/allocator/MarginalHitsOptimizeStrategy.h>

namespace {

const std::string PROP_CACHE_NAME = "cachelib.name";
const std::string PROP_CACHE_NAME_DEFAULT = "CacheLib";

const std::string PROP_CACHE_EVICTION = "cachelib.eviction";
const std::string PROP_CACHE_EVICTION_DEFAULT = "lru"; // or 2q
                                                       //
const std::string PROP_SIZE = "cachelib.size";
const std::string PROP_SIZE_DEFAULT = "1000000000";

const std::string PROP_VIRTUAL_SIZE = "cachelib.virtualsize";

const std::string PROP_PROPORTION = "cachelib.proportion";
const std::string PROP_PROPORTION_DEFAULT = "1.0";

const std::string PROP_CONTROLLER_ADDRESS = "cachelib.controller.address";
const std::string PROP_CONTROLLER_ADDRESS_DEFAULT = "";

const std::string PROP_STAGE_ADDRESS = "cachelib.holpaca.address";
const std::string PROP_STAGE_ADDRESS_DEFAULT = "";

const std::string PROP_POOL_NAME = "cachelib.pool.name";
const std::string PROP_POOL_NAME_DEFAULT = "default";

const std::string PROP_POOL_SIZE = "cachelib.pool.relsize";
const std::string PROP_POOL_SIZE_DEFAULT = "1";

const std::string PROP_POOL_QOS_LEVEL = "cachelib.pool.qos";
const std::string PROP_POOL_QOS_LEVEL_DEFAULT = "0.0";

const std::string PROP_POOL_PROPORTION = "cachelib.pool.proportion";
const std::string PROP_POOL_PROPORTION_DEFAULT = "1.0";

const std::string PROP_POOL_NO_INITIAL_SIZE = "cachelib.pool.noinitialsize";
const std::string PROP_POOL_NO_INITIAL_SIZE_DEFAULT = "off";

const std::string PROP_POOL_OPTIMIZER = "cachelib.pooloptimizer";
const std::string PROP_POOL_OPTIMIZER_DEFAULT = "off";

const std::string PROP_POOL_RESIZER = "cachelib.poolresizer";
const std::string PROP_POOL_RESIZER_DEFAULT = "off";

const std::string PROP_POOL_REBALANCER = "cachelib.poolrebalancer";
const std::string PROP_POOL_REBALANCER_DEFAULT = "off";
} // namespace

namespace ycsbc {

std::mutex CacheLibHolpaca::mutex_;
thread_local facebook::cachelib::PoolId CacheLibHolpaca::poolId_;
int CacheLibHolpaca::ref_cnt_ = 0;
std::unordered_map<int, int> CacheLibHolpaca::rocksdbIOPSPerThread_;
std::unordered_map<int, std::pair<int, int>>
    CacheLibHolpaca::missesAndHitsPerThread_;
std::unordered_map<std::string, RocksDB> CacheLibHolpaca::rocksdbs_;
std::unordered_map<std::string, std::shared_ptr<CacheLibHolpaca::Cache>>
    CacheLibHolpaca::caches_;
std::unordered_map<int, std::tuple<std::shared_ptr<CacheLibHolpaca::Cache>,
                                   facebook::cachelib::PoolId>>
    CacheLibHolpaca::cachesPerThread_;
thread_local std::string CacheLibHolpaca::cacheName_;
thread_local std::shared_ptr<CacheLibHolpaca::Cache> CacheLibHolpaca::cache_;
thread_local int CacheLibHolpaca::threadId_;
thread_local static facebook::cachelib::PoolId poolId_;
thread_local RocksDB CacheLibHolpaca::rocksdb_;

void CacheLibHolpaca::Init() {

  std::lock_guard<std::mutex> lock(mutex_);
  ref_cnt_++;
  cacheName_ = props_->GetProperty(
      PROP_CACHE_NAME + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_CACHE_NAME, PROP_CACHE_NAME_DEFAULT));

  if (auto it = caches_.find(cacheName_); it != caches_.end()) {
    // already initialized (two threads can point to the same cache)
    cache_ = it->second;
    rocksdb_ = rocksdbs_[cacheName_];
    rocksdb_.Init();
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
              .setCacheSize(std::stol(props_->GetProperty(
                  PROP_SIZE + "." + std::to_string(threadId_),
                  props_->GetProperty(PROP_SIZE, PROP_SIZE_DEFAULT))))
              .setCacheName(cacheName_)
              .setAccessConfig({25 /* bucket power */,
                                15 /* lock power */}); // assuming caching
                                                       // 20 million items

          config.setProportion(std::stod(props_->GetProperty(
              PROP_PROPORTION + "." + std::to_string(threadId_),
              props_->GetProperty(PROP_PROPORTION, PROP_PROPORTION_DEFAULT))));

          if (props_->ContainsKey(PROP_VIRTUAL_SIZE + "." +
                                  std::to_string(threadId_)) ||
              props_->ContainsKey(PROP_VIRTUAL_SIZE)) {
            config.setVirtualSize(std::stol(props_->GetProperty(
                PROP_VIRTUAL_SIZE + "." + std::to_string(threadId_),
                props_->GetProperty(PROP_VIRTUAL_SIZE))));
          }
          auto address = props_->GetProperty(
              PROP_STAGE_ADDRESS + "." + std::to_string(threadId_),
              props_->GetProperty(PROP_STAGE_ADDRESS,
                                  PROP_STAGE_ADDRESS_DEFAULT));
          if (!address.empty()) {
            config.setAddress(address);
          }
          auto controllerAddress = props_->GetProperty(
              PROP_CONTROLLER_ADDRESS + "." + std::to_string(threadId_),
              props_->GetProperty(PROP_CONTROLLER_ADDRESS,
                                  PROP_CONTROLLER_ADDRESS_DEFAULT));
          if (!controllerAddress.empty()) {
            config.setControllerAddress(controllerAddress);
          }
          if (props_->GetProperty(
                  PROP_POOL_REBALANCER + "." + std::to_string(threadId_),
                  props_->GetProperty(PROP_POOL_REBALANCER,
                                      PROP_POOL_REBALANCER_DEFAULT)) == "on") {
            config.enablePoolRebalancing(
                std::make_shared<facebook::cachelib::HitsPerSlabStrategy>(
                    facebook::cachelib::HitsPerSlabStrategy::Config(
                        0.25, static_cast<unsigned int>(1))),
                std::chrono::milliseconds(100), 1);
          }
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
                std::chrono::seconds(1), std::chrono::seconds(0), 1);
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
    rocksdbIOPSPerThread_[threadId_] = 0;
    missesAndHitsPerThread_[threadId_] = {0, 0};
    rocksdb_.SetProps(props_);
    rocksdb_.Init();
    rocksdbs_[cacheName_] = rocksdb_;
    caches_[cacheName_] = cache_;
  }
  std::string poolName = props_->GetProperty(
      PROP_POOL_NAME + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_POOL_NAME, PROP_POOL_NAME_DEFAULT));
  auto poolSize = std::stod(props_->GetProperty(
      PROP_POOL_SIZE + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_POOL_SIZE, PROP_POOL_SIZE_DEFAULT)));
  bool dontSetPoolSize =
      props_->GetProperty(
          PROP_POOL_NO_INITIAL_SIZE + "." + std::to_string(threadId_),
          props_->GetProperty(PROP_POOL_NO_INITIAL_SIZE,
                              PROP_POOL_NO_INITIAL_SIZE_DEFAULT)) == "on";
  double qosLevel = std::stod(props_->GetProperty(
      PROP_POOL_QOS_LEVEL + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_POOL_QOS_LEVEL, PROP_POOL_QOS_LEVEL_DEFAULT)));
  double proportion = std::stod(props_->GetProperty(
      PROP_POOL_PROPORTION + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_POOL_PROPORTION, PROP_POOL_PROPORTION_DEFAULT)));
  std::visit(
      [&poolName, &poolSize, dontSetPoolSize, &qosLevel,
       &proportion](auto &&cache) {
        if (dontSetPoolSize) {
          CacheLibHolpaca::poolId_ =
              cache.addPool(poolName, 0, qosLevel, proportion);
        } else {
          CacheLibHolpaca::poolId_ = cache.addPool(
              poolName,
              static_cast<long>(cache.getCacheMemoryStats().ramCacheSize *
                                poolSize),
              qosLevel, proportion);
        }
      },
      *cache_);
  cachesPerThread_[threadId_] =
      std::make_tuple(cache_, CacheLibHolpaca::poolId_);
}

DB::Status CacheLibHolpaca::Read(const std::string &table,
                                 const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  //  std::lock_guard<std::mutex> lock(mutex_);
  return std::visit(
      [&table, &key, &fields, &result](auto &&cache) {
        auto handle = cache.find(key);
        auto status = handle != nullptr ? kOK : kNotFound;
        if (status == kNotFound) {
          rocksdbIOPSPerThread_[threadId_]++;
          missesAndHitsPerThread_[threadId_].first++;
          if (rocksdbs_[cacheName_].Read(table, key, fields, result) == kOK) {
            uint32_t size = result.front().value.size();
            auto new_handle = cache.allocate(poolId_, key, size);
            if (new_handle) {
              if (size > 0) {
                std::memcpy(new_handle->getMemory(),
                            result.front().value.data(), size);
              }
              cache.insertOrReplace(new_handle);
            } else {
              //              std::cerr << "Failed to allocate memory for key: "
              //              << key
              //                        << std::endl;
              return kError;
            }
          } else {
            std::cerr << "Key not found in RocksDB: " << key << std::endl;
            std::abort();
          }
        } else {
          missesAndHitsPerThread_[threadId_].second++;
          volatile auto data =
              std::string(reinterpret_cast<const char *>(handle->getMemory()),
                          handle->getSize());
        }
        return status;
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
  //  std::lock_guard<std::mutex> lock(mutex_);
  std::string data = values.front().value;
  uint32_t size = values.front().value.size();
  if (rocksdbs_[cacheName_].Update(table, key, values) == kOK) {
    return std::visit(
        [&data, &key, &size](auto &&cache) {
          auto handle = cache.find(key);
          if (cache.find(key) != nullptr) {
            auto new_handle = cache.allocate(poolId_, key, size);
            if (new_handle) {
              std::memcpy(new_handle->getMemory(), data.data(), size);
              cache.insertOrReplace(new_handle);
              return kOK;
            }
            return kError;
          }
          return kOK;
        },
        *cache_);
  }
  return kError;
}

DB::Status CacheLibHolpaca::Insert(const std::string &table,
                                   const std::string &key,
                                   std::vector<Field> &values) {
  // std::lock_guard<std::mutex> lock(mutex_);
  uint32_t size = values.front().value.size();
  std::string data = values.front().value;
  if (rocksdbs_[cacheName_].Insert(table, key, values) == kOK) {
    return std::visit(
        [&data, &key, &size](auto &&cache) {
          auto handle = cache.find(key);
          if (cache.find(key) != nullptr) {
            auto new_handle = cache.allocate(poolId_, key, size);
            if (new_handle) {
              std::memcpy(new_handle->getMemory(), data.data(), size);
              cache.insertOrReplace(new_handle);
              return kOK;
            }
            return kError;
          }
          return kOK;
        },
        *cache_);
  }
  return kError;
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

void CacheLibHolpaca::Cleanup() {
  std::lock_guard<std::mutex> lock(mutex_);
  rocksdb_.Cleanup();
  std::visit([](auto &&cache) { cache.removePool(poolId_); }, *cache_);
  if (--ref_cnt_) {
    return;
  }
  for (auto &cache : caches_) {
    cache.second.reset();
  }
  caches_.clear();
  rocksdbs_.clear();
  cachesPerThread_.clear();
  rocksdbIOPSPerThread_.clear();
}

} // namespace ycsbc
