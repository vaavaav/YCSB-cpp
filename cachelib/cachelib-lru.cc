#include "cachelib-lru.h"
#include "core/db_factory.h"
#include <cachelib/allocator/HitsPerSlabStrategy.h>

namespace {

const std::string PROP_CACHE_NAME = "cachelib.name";
const std::string PROP_CACHE_NAME_DEFAULT = "CacheLib";

const std::string PROP_SIZE = "cachelib.size";
const std::string PROP_SIZE_DEFAULT = "1000000000";

const std::string PROP_POOL_NAME = "cachelib.pool.name";
const std::string PROP_POOL_NAME_DEFAULT = "default";

const std::string PROP_POOL_SIZE = "cachelib.pool.relsize";
const std::string PROP_POOL_SIZE_DEFAULT = "1";

const std::string PROP_POOL_NO_INITIAL_SIZE = "cachelib.pool.noinitialsize";
const std::string PROP_POOL_NO_INITIAL_SIZE_DEFAULT = "off";

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

std::mutex CacheLibLRU::mutex_;
std::unordered_map<std::string, std::tuple<RocksDB, CacheLibLRU::Cache, int>>
    CacheLibLRU::rocksdbsAndCaches_;

void CacheLibLRU::Init() {

  std::lock_guard<std::mutex> lock(mutex_);

  if (rocksdbsAndCaches_.empty()) {
    rocksdbsAndCaches_.reserve(
        std::stoi(props_->GetProperty("threadcount", "1")));
  }

  cacheName_ = props_->GetProperty(
      PROP_CACHE_NAME + "." + std::to_string(threadId_),
      props_->GetProperty(PROP_CACHE_NAME, PROP_CACHE_NAME_DEFAULT));

  if (auto it = rocksdbsAndCaches_.find(cacheName_);
      it != rocksdbsAndCaches_.end()) {
    cache_ = std::get<1>(it->second);
    rocksdb_ = std::get<0>(it->second);
    rocksdb_.Init();
    ++std::get<2>(it->second); // increment ref count
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
    config.validate(); // will throw if bad config
    cache_ = std::make_shared<CacheLibLRU::CacheAllocator>(config);

    rocksdb_.SetProps(props_);
    rocksdb_.Init();
    rocksdbsAndCaches_[cacheName_] = std::make_tuple(rocksdb_, cache_, 1);
  }
  if (poolName_.empty()) {
    poolName_ = props_->GetProperty(
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
    poolId_ = cache_->addPool(
        poolName_,
        dontSetPoolSize
            ? 0
            : static_cast<long>(cache_->getCacheMemoryStats().ramCacheSize *
                                poolSize));
  }
}

DB::Status CacheLibLRU::Read(const std::string &table, const std::string &key,
                             const std::vector<std::string> *fields,
                             std::vector<Field> &result) {
  auto handle = cache_->find(key);
  auto status = handle != nullptr ? kOK : kNotFound;
  if (status == kNotFound) {
    if (rocksdb_.Read(table, key, fields, result) == kOK) {
      uint32_t size = result.front().value.size();
      auto new_handle = cache_->allocate(poolId_, key, size);
      if (new_handle) {
        if (size > 0) {
          std::memcpy(new_handle->getMemory(), result.front().value.data(),
                      size);
        }
        cache_->insertOrReplace(new_handle);
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
    volatile auto data = std::string(
        reinterpret_cast<const char *>(handle->getMemory()), handle->getSize());
  }
  return status;
}

DB::Status CacheLibLRU::Scan(const std::string &table, const std::string &key,
                             long len, const std::vector<std::string> *fields,
                             std::vector<std::vector<Field>> &result) {

  std::lock_guard<std::mutex> lock(mutex_);
  // TODO

  return kError;
}

DB::Status CacheLibLRU::Update(const std::string &table, const std::string &key,
                               std::vector<Field> &values) {
  //  std::lock_guard<std::mutex> lock(mutex_);
  std::string data = values.front().value;
  uint32_t size = values.front().value.size();
  if (rocksdb_.Update(table, key, values) == kOK) {
    auto handle = cache_->find(key);
    if (cache_->find(key) != nullptr) {
      auto new_handle = cache_->allocate(poolId_, key, size);
      if (new_handle) {
        std::memcpy(new_handle->getMemory(), data.data(), size);
        cache_->insertOrReplace(new_handle);
        return kOK;
      }
      return kError;
    }
    return kOK;
  }
  return kError;
}

DB::Status CacheLibLRU::Insert(const std::string &table, const std::string &key,
                               std::vector<Field> &values) {
  // std::lock_guard<std::mutex> lock(mutex_);
  uint32_t size = values.front().value.size();
  std::string data = values.front().value;
  if (rocksdb_.Insert(table, key, values) == kOK) {
    auto handle = cache_->find(key);
    if (cache_->find(key) != nullptr) {
      auto new_handle = cache_->allocate(poolId_, key, size);
      if (new_handle) {
        std::memcpy(new_handle->getMemory(), data.data(), size);
        cache_->insertOrReplace(new_handle);
        return kOK;
      }
      return kError;
    }
    return kOK;
  }
  return kError;
}

DB::Status CacheLibLRU::Delete(const std::string &table,
                               const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto key_ = key;
  // return cache_->remove(key_) == Cache::RemoveRes::kSuccess ? kOK :
  // kNotFound;
  return kOK;
}

DB *NewCacheLibLRU(int threadId) { return new CacheLibLRU(threadId); }

const bool registered = DBFactory::RegisterDB("cachelib-lru", NewCacheLibLRU);

void CacheLibLRU::Cleanup() {
  std::lock_guard<std::mutex> lock(mutex_);
  rocksdb_.Cleanup();
  cache_ = nullptr;
  auto &[x, y, refCount] = rocksdbsAndCaches_[cacheName_];
  if (refCount == 1) {
    rocksdbsAndCaches_.erase(cacheName_);
  } else {
    --refCount;
  }
}

} // namespace ycsbc
