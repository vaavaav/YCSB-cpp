#include "cachelib-holpaca-overhead.h"
#include "core/db_factory.h"
#include <cachelib/allocator/HitsPerSlabStrategy.h>
#include <cachelib/allocator/MarginalHitsOptimizeStrategy.h>

namespace {

const std::string PROP_CACHE_NAME = "cachelib.name";
const std::string PROP_CACHE_NAME_DEFAULT = "CacheLib";

const std::string PROP_SIZE = "cachelib.size";
const std::string PROP_SIZE_DEFAULT = "1000000000";

const std::string PROP_CONTROLLER_ADDRESS = "cachelib.controller.address";
const std::string PROP_CONTROLLER_ADDRESS_DEFAULT = "";

const std::string PROP_STAGE_ADDRESS = "cachelib.holpaca.address";
const std::string PROP_STAGE_ADDRESS_DEFAULT = "";

const std::string PROP_POOL_NAME = "cachelib.pool.name";
const std::string PROP_POOL_NAME_DEFAULT = "default";

const std::string PROP_POOL_SIZE = "cachelib.pool.relsize";
const std::string PROP_POOL_SIZE_DEFAULT = "1";

const std::string PROP_POOL_OPTIMIZER = "cachelib.pooloptimizer";
const std::string PROP_POOL_OPTIMIZER_DEFAULT = "off";

const std::string PROP_POOL_RESIZER = "cachelib.poolresizer";
const std::string PROP_POOL_RESIZER_DEFAULT = "off";

const std::string PROP_POOL_RESIZER_MILLISECONDS =
    "cachelib.poolresizer.milliseconds";
const std::string PROP_POOL_RESIZER_MILLISECONDS_DEFAULT = "1000";

const std::string PROP_POOL_RESIZER_SLABS = "cachelib.poolresizer.slabs";
const std::string PROP_POOL_RESIZER_SLABS_DEFAULT = "1";

const std::string PROP_POOL_REBALANCER = "cachelib.poolrebalancer";
const std::string PROP_POOL_REBALANCER_DEFAULT = "off";

const std::string PROP_POOL_REBALANCER_MILLISECONDS =
    "cachelib.poolrebalancer.milliseconds";
const std::string PROP_POOL_REBALANCER_MILLISECONDS_DEFAULT = "1000";

const std::string PROP_POOL_REBALANCER_SLABS = "cachelib.poolrebalancer.slabs";
const std::string PROP_POOL_REBALANCER_SLABS_DEFAULT = "1";

} // namespace

namespace ycsbc {

std::mutex CacheLibHolpacaOverhead::mutex_;
thread_local facebook::cachelib::PoolId CacheLibHolpacaOverhead::poolId_;
std::unordered_map<int, std::pair<int, int>>
    CacheLibHolpacaOverhead::missesAndHitsPerThread_;
std::unordered_map<int, std::pair<int, int>>
    CacheLibHolpacaOverhead::previousMissesAndHitsPerThread_;
std::unordered_map<std::string, CacheLibHolpacaOverhead::Cache>
    CacheLibHolpacaOverhead::caches_;
std::unordered_map<
    int, std::tuple<CacheLibHolpacaOverhead::Cache, facebook::cachelib::PoolId>>
    CacheLibHolpacaOverhead::cachesPerThread_;
std::unordered_map<std::string, int> CacheLibHolpacaOverhead::refCountPerCache_;
thread_local std::string CacheLibHolpacaOverhead::cacheName_;
thread_local CacheLibHolpacaOverhead::Cache CacheLibHolpacaOverhead::cache_;
thread_local int CacheLibHolpacaOverhead::threadId_;
thread_local facebook::cachelib::PoolId poolId_;

void CacheLibHolpacaOverhead::Init() {

  std::lock_guard<std::mutex> lock(mutex_);

  if (missesAndHitsPerThread_.empty()) {
    auto kThreads = std::stoi(props_->GetProperty("threadcount", "1"));
    missesAndHitsPerThread_.reserve(kThreads);
    previousMissesAndHitsPerThread_.reserve(kThreads);
    caches_.reserve(kThreads);
    cachesPerThread_.reserve(kThreads);
    refCountPerCache_.reserve(kThreads);
  }

  cacheName_ = props_->GetProperty(
      PROP_CACHE_NAME + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_CACHE_NAME, PROP_CACHE_NAME_DEFAULT));

  if (auto it = caches_.find(cacheName_); it != caches_.end()) {
    // already initialized (two threads can point to the same cache)
    cache_ = it->second;
    refCountPerCache_[cacheName_]++;
  } else {
    Config config;
    config
        .setCacheSize(std::stol(props_->GetProperty(
            PROP_SIZE + "." + std::to_string(threadId_),
            props_->GetProperty(PROP_SIZE, PROP_SIZE_DEFAULT))))
        .setCacheName(cacheName_)
        .setAccessConfig(
            {25 /* bucket power */, 15 /* lock power */}); // assuming caching
                                                           // 20 million items

    auto address = props_->GetProperty(
        PROP_STAGE_ADDRESS + "." + std::to_string(threadId_),
        props_->GetProperty(PROP_STAGE_ADDRESS, PROP_STAGE_ADDRESS_DEFAULT));
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

      auto ms = std::chrono::milliseconds(std::stol(props_->GetProperty(
          PROP_POOL_REBALANCER_MILLISECONDS + "." + std::to_string(threadId_),
          props_->GetProperty(PROP_POOL_REBALANCER_MILLISECONDS,
                              PROP_POOL_REBALANCER_MILLISECONDS_DEFAULT))));

      auto slabs = std::stol(props_->GetProperty(
          PROP_POOL_REBALANCER_SLABS + "." + std::to_string(threadId_),
          props_->GetProperty(PROP_POOL_REBALANCER_SLABS,
                              PROP_POOL_REBALANCER_SLABS_DEFAULT)));

      config.enablePoolRebalancing(
          std::make_shared<facebook::cachelib::HitsPerSlabStrategy>(
              facebook::cachelib::HitsPerSlabStrategy::Config(
                  0.25, static_cast<unsigned int>(1))),
          ms, slabs);
    }
    // Needed for pool resizing
    if (props_->GetProperty(PROP_POOL_RESIZER + "." + std::to_string(threadId_),
                            props_->GetProperty(PROP_POOL_RESIZER,
                                                PROP_POOL_RESIZER_DEFAULT)) ==
        "on") {

      auto ms = std::chrono::milliseconds(std::stol(props_->GetProperty(
          PROP_POOL_RESIZER_MILLISECONDS + "." + std::to_string(threadId_),
          props_->GetProperty(PROP_POOL_RESIZER_MILLISECONDS,
                              PROP_POOL_RESIZER_MILLISECONDS_DEFAULT))));

      auto slabs = std::stol(props_->GetProperty(
          PROP_POOL_RESIZER_SLABS + "." + std::to_string(threadId_),
          props_->GetProperty(PROP_POOL_RESIZER_SLABS,
                              PROP_POOL_RESIZER_SLABS_DEFAULT)));

      config.enablePoolResizing(
          std::make_shared<facebook::cachelib::HitsPerSlabStrategy>(
              facebook::cachelib::HitsPerSlabStrategy::Config(
                  0.25, static_cast<unsigned int>(1))),
          ms, slabs);
    }
    if (props_->GetProperty(
            PROP_POOL_OPTIMIZER + "." + std::to_string(threadId_),
            props_->GetProperty(PROP_POOL_OPTIMIZER,
                                PROP_POOL_OPTIMIZER_DEFAULT)) == "on") {
      config.enableTailHitsTracking(); // needed for tracking tail hits
      config.enablePoolOptimizer(
          std::make_shared<facebook::cachelib::MarginalHitsOptimizeStrategy>(),
          std::chrono::seconds(1), std::chrono::seconds(0), 1);
    }
    config.validate(); // will throw if bad config

    cache_ = std::make_shared<CacheLibHolpacaOverhead::CacheAllocator>(config);
    caches_[cacheName_] = cache_;
    refCountPerCache_[cacheName_] = 1;
  }
  missesAndHitsPerThread_[threadId_] = {0, 0};
  previousMissesAndHitsPerThread_[threadId_] = {0, 0};

  if (cachesPerThread_.find(threadId_) != cachesPerThread_.end()) {
    poolId_ = std::get<1>(cachesPerThread_.at(threadId_));
    refCountPerCache_[cacheName_]--;
    return;
  }

  std::string poolName = props_->GetProperty(
      PROP_POOL_NAME + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_POOL_NAME, PROP_POOL_NAME_DEFAULT));
  auto poolSize = std::stod(props_->GetProperty(
      PROP_POOL_SIZE + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_POOL_SIZE, PROP_POOL_SIZE_DEFAULT)));
  CacheLibHolpacaOverhead::poolId_ = cache_->addPool(
      poolName,
      static_cast<long>(cache_->getCacheMemoryStats().ramCacheSize * poolSize));

  cachesPerThread_.emplace(threadId_, std::make_tuple(cache_, poolId_));
}

DB::Status CacheLibHolpacaOverhead::Read(const std::string &table,
                                         const std::string &key,
                                         const std::vector<std::string> *fields,
                                         std::vector<Field> &result) {
  auto handle = cache_->find(key);
  // auto &[misses, hits] = missesAndHitsPerThread_[threadId_];
  if (handle != nullptr) {
    // hits++;
    return kOK;
  }
  // misses++;
  return kNotFound;
} // namespace ycsbc

DB::Status
CacheLibHolpacaOverhead::Scan(const std::string &table, const std::string &key,
                              long len, const std::vector<std::string> *fields,
                              std::vector<std::vector<Field>> &result) {

  // TODO

  return kError;
}

DB::Status CacheLibHolpacaOverhead::Update(const std::string &table,
                                           const std::string &key,
                                           std::vector<Field> &values) {
  uint32_t size = values.front().value.size();
  std::string data = values.front().value;
  auto new_handle = cache_->allocate(poolId_, key, size);
  if (new_handle) {
    std::memcpy(new_handle->getMemory(), data.data(), size);
    cache_->insertOrReplace(new_handle);
    return kOK;
  }
  std::cerr << "Failed to allocate memory for key: " << key << std::endl;
  std::abort();
}

DB::Status CacheLibHolpacaOverhead::Insert(const std::string &table,
                                           const std::string &key,
                                           std::vector<Field> &values) {
  uint32_t size = values.front().value.size();
  std::string data = values.front().value;
  auto new_handle = cache_->allocate(poolId_, key, size);
  if (new_handle) {
    std::memcpy(new_handle->getMemory(), data.data(), size);
    cache_->insert(new_handle);
    return kOK;
  }
  std::cerr << "Failed to allocate memory for key: " << key << std::endl;
  std::abort();
}

DB::Status CacheLibHolpacaOverhead::Delete(const std::string &table,
                                           const std::string &key) {
  auto key_ = key;
  // return cache_->remove(key_) == Cache::RemoveRes::kSuccess ? kOK :
  // kNotFound;
  return kOK;
}

DB *NewCacheLibHolpacaOverhead() { return new CacheLibHolpacaOverhead(); }

const bool registered = DBFactory::RegisterDB("cachelib-holpaca-overhead",
                                              NewCacheLibHolpacaOverhead);

void CacheLibHolpacaOverhead::SetThreadId(int threadId) {
  threadId_ = threadId;
}

void CacheLibHolpacaOverhead::Cleanup() {
  std::lock_guard<std::mutex> lock(mutex_);
  auto &[cache, poolId] = cachesPerThread_[threadId_];
  cache = nullptr;
  if (refCountPerCache_[cacheName_] == 1) {
    refCountPerCache_.erase(cacheName_);
    caches_.erase(cacheName_);
    cache_.reset();
  } else {
    refCountPerCache_[cacheName_]--;
  }

  if (refCountPerCache_.empty()) {
    caches_.clear();
    cachesPerThread_.clear();
  }
}

} // namespace ycsbc
