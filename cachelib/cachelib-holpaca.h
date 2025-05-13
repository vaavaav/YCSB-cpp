#pragma once

#include "rocksdb.h"
#include <cachelib/holpaca/data-plane/CacheAllocator.h>
#include <core/db.h>
#include <unordered_map>
#include <variant>

namespace ycsbc {

class CacheLibHolpaca : public DB {

public:
  using CacheLRU = facebook::cachelib::holpaca::LruAllocator;
  using Cache2Q = facebook::cachelib::holpaca::Lru2QAllocator;
  using Cache = std::variant<CacheLRU, Cache2Q>;
  using Config = std::variant<CacheLRU::Config, Cache2Q::Config>;

private:
  static std::mutex mutex_;
  static std::unordered_map<std::string, RocksDB> rocksdbs_;
  static std::unordered_map<std::string, std::shared_ptr<Cache>> caches_;
  thread_local static std::string cacheName_;
  thread_local static std::shared_ptr<Cache> cache_;
  thread_local static RocksDB rocksdb_;
  thread_local static int threadId_;
  thread_local static facebook::cachelib::PoolId poolId_;
  static int ref_cnt_;
  thread_local static int rocksdbIOPS_;
  thread_local static std::chrono::high_resolution_clock::time_point lastTime_;

public:
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

  std::tuple<uint64_t, uint64_t> OccupancyAndCapacity();

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

  void SetThreadId(int threadId) override;

  std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>
  OccupancyCapacityAndGlobal() {
    if (cache_ == nullptr) {
      return std::make_tuple(0, 0, 0, 0);
    }
    auto value = std::visit(
        [](auto &&c) {
          c.registerMetrics(
              poolId_,
              rocksdbIOPS_ /
                  std::chrono::duration_cast<std::chrono::seconds>(
                      std::chrono::high_resolution_clock::now() - lastTime_)
                      .count());
          const auto &pool = c.getPool(poolId_);
          auto cms = c.getCacheMemoryStats();
          return std::make_tuple(
              pool.getCurrentAllocSize(), pool.getPoolUsableSize(),
              cms.configuredRamCacheRegularSize - cms.unReservedSize,
              cms.configuredRamCacheRegularSize);
        },
        *cache_);
    lastTime_ = std::chrono::high_resolution_clock::now();
    return value;
  }
};

DB *NewCacheLibHolpaca();

} // namespace ycsbc
