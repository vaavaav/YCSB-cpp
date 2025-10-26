#pragma once

#include "rocksdb.h"
#include <cachelib/allocator/CacheAllocator.h>
#include <core/db.h>
#include <unordered_map>

namespace ycsbc {

class CacheLibLRU : public DB {

public:
  using CacheAllocator = facebook::cachelib::LruAllocator;
  using Cache = std::shared_ptr<CacheAllocator>;
  using Config = CacheAllocator::Config;

private:
  static std::mutex mutex_;
  static std::unordered_map<std::string, std::tuple<RocksDB, Cache, int>>
      rocksdbsAndCaches_;

  int const threadId_;
  Cache cache_ = nullptr;
  std::string cacheName_;
  std::string poolName_ = "";
  facebook::cachelib::PoolId poolId_;
  RocksDB rocksdb_;

public:
  explicit CacheLibLRU(int threadId) : threadId_(threadId) {}

  void Init();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields,
              std::vector<Field> &result);

  Status Scan(const std::string &table, const std::string &key, long len,
              const std::vector<std::string> *fields,
              std::vector<std::vector<Field>> &result);

  Status Update(const std::string &table, const std::string &key,
                std::vector<Field> &values);

  Status Insert(const std::string &table, const std::string &key,
                std::vector<Field> &values);

  Status Delete(const std::string &table, const std::string &key);

  static void SerializeRow(const std::vector<Field> &values, std::string &data);

  static void DeserializeRowFilter(std::vector<Field> &values, const char *p,
                                   const char *lim,
                                   const std::vector<std::string> &fields);

  static void DeserializeRowFilter(std::vector<Field> &values,
                                   const std::string &data,
                                   const std::vector<std::string> &fields);

  static void DeserializeRow(std::vector<Field> &values, const char *p,
                             const char *lim);

  static void DeserializeRow(std::vector<Field> &values,
                             const std::string &data);

  void Cleanup() override;

  std::tuple<std::string, std::string, uint64_t, uint64_t, uint64_t, uint64_t>
  OccupancyCapacityAndGlobal() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (cache_ == nullptr) {
      return std::make_tuple("", "", 0, 0, 0, 0);
    }
    const auto &pool = cache_->getPool(poolId_);
    auto cms = cache_->getCacheMemoryStats();
    return std::make_tuple(
        cacheName_, poolName_, pool.getCurrentAllocSize(), pool.getPoolSize(),
        cms.configuredRamCacheRegularSize - cms.unReservedSize,
        cms.configuredRamCacheRegularSize);
  }
}; // namespace ycsbc

DB *NewCacheLibLRU(int threadId);

} // namespace ycsbc
