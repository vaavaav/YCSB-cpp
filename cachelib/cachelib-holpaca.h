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
  static std::unordered_map<
      int, std::tuple<std::shared_ptr<Cache>, facebook::cachelib::PoolId>>
      cachesPerThread_;
  thread_local static std::string cacheName_;
  thread_local static std::shared_ptr<Cache> cache_;
  thread_local static RocksDB rocksdb_;
  thread_local static int threadId_;
  thread_local static facebook::cachelib::PoolId poolId_;
  static int ref_cnt_;
  static std::unordered_map<int, int> rocksdbIOPSPerThread_;
  static std::unordered_map<int, std::pair<int, int>> missesAndHitsPerThread_;
  static std::unordered_map<int, std::pair<int, int>>
      previousMissesAndHitsPerThread_;

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

  void Cleanup() override;

  std::tuple<std::string, std::string, uint64_t, uint64_t, uint64_t, uint64_t>
  OccupancyCapacityAndGlobal(int i) {
    if (cachesPerThread_.find(i) == cachesPerThread_.end() ||
        std::get<0>(cachesPerThread_[i]) == nullptr) {
      return std::make_tuple("", "", 0, 0, 0, 0);
    }
    auto [cache, poolId] = cachesPerThread_[i];
    auto value = std::visit(
        [i, poolId](auto &&c) {
          auto accMissesAndHits = missesAndHitsPerThread_[i];

          int misses = missesAndHitsPerThread_[i].first -
                       previousMissesAndHitsPerThread_[i].first;

          int hits = missesAndHitsPerThread_[i].second -
                     previousMissesAndHitsPerThread_[i].second;

          c.registerMetrics(poolId, rocksdbIOPSPerThread_[i],
                            (misses + hits == 0) ? 0
                                                 : static_cast<double>(misses) /
                                                       (misses + hits));
          previousMissesAndHitsPerThread_[i] = accMissesAndHits;

          const auto &pool = c.getPool(poolId);
          auto cms = c.getCacheMemoryStats();
          return std::make_tuple(c.getCacheName(), c.getPoolName(poolId),
                                 pool.getCurrentAllocSize(), pool.getPoolSize(),
                                 cms.configuredRamCacheRegularSize -
                                     cms.unReservedSize,
                                 cms.configuredRamCacheRegularSize);
        },
        *cache);
    rocksdbIOPSPerThread_[i] = 0;
    return value;
  }
};

DB *NewCacheLibHolpaca();

} // namespace ycsbc
