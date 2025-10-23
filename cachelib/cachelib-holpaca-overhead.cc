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
std::unordered_map<std::string, std::pair<CacheLibHolpacaOverhead::Cache, int>>
    CacheLibHolpacaOverhead::caches_;

void CacheLibHolpacaOverhead::Init() {

  std::lock_guard<std::mutex> lock(mutex_);

  if (caches_.empty()) {
    caches_.reserve(std::stoi(props_->GetProperty("threadcount", "1")));
  }

  auto const cacheName_ = props_->GetProperty(
      PROP_CACHE_NAME + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_CACHE_NAME, PROP_CACHE_NAME_DEFAULT));

  if (auto it = caches_.find(cacheName_); it != caches_.end()) {
    cache_ = it->second.first;
    ++it->second.second; // increment ref count
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
    caches_[cacheName_] = {cache_, 1};
  }

  if (poolName_.empty()) {

    poolName_ = props_->GetProperty(
        PROP_POOL_NAME + "." + std::to_string(threadId_),
        props_->GetProperty(PROP_POOL_NAME, PROP_POOL_NAME_DEFAULT));
    auto poolSize = std::stod(props_->GetProperty(
        PROP_POOL_SIZE + "." + std::to_string(threadId_),
        props_->GetProperty(PROP_POOL_SIZE, PROP_POOL_SIZE_DEFAULT)));

    poolId_ = cache_->addPool(
        poolName_, static_cast<long>(
                       cache_->getCacheMemoryStats().ramCacheSize * poolSize));
  }
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

DB *NewCacheLibHolpacaOverhead(int threadId) {
  return new CacheLibHolpacaOverhead(threadId);
}

const bool registered = DBFactory::RegisterDB("cachelib-holpaca-overhead",
                                              NewCacheLibHolpacaOverhead);

void CacheLibHolpacaOverhead::Cleanup() {
  std::lock_guard<std::mutex> lock(mutex_);
  cache_ = nullptr;
  auto &[_, refCount] = caches_[cacheName_];
  if (refCount == 1) {
    caches_.erase(cacheName_);
  } else {
    --refCount;
  }
}

} // namespace ycsbc
