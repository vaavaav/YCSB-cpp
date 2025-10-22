#pragma once

#include <cachelib/holpaca/data-plane/CacheAllocator.h>
#include <core/db.h>
#include <unordered_map>

namespace ycsbc {

class CacheLibHolpacaOverhead : public DB {

public:
  using CacheAllocator = facebook::cachelib::holpaca::LruAllocator;
  using Cache = std::shared_ptr<CacheAllocator>;
  using Config = CacheAllocator::Config;

private:
  static std::mutex mutex_;
  static std::unordered_map<std::string, Cache> caches_;
  static std::unordered_map<int, std::tuple<Cache, facebook::cachelib::PoolId>>
      cachesPerThread_;

  static std::unordered_map<std::string, int> refCountPerCache_;
  thread_local static std::string cacheName_;
  thread_local static Cache cache_;
  thread_local static int threadId_;
  thread_local static facebook::cachelib::PoolId poolId_;
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
    auto it = cachesPerThread_.find(i);
    if (it == cachesPerThread_.end()) {
      return std::make_tuple("", "", 0, 0, 0, 0);
    }
    auto [cache, poolId] = it->second;
    if (cache == nullptr) {
      return std::make_tuple("", "", 0, 0, 0, 0);
    }
    auto [misses, hits] = missesAndHitsPerThread_[i];
    auto &[pmisses, phits] = previousMissesAndHitsPerThread_[i];

    int const kMisses = misses - pmisses;
    int const kHits = hits - phits;
    pmisses = misses;
    phits = hits;
    cache->registerMetrics(poolId, 0,
                           (kMisses + kHits == 0)
                               ? 0
                               : static_cast<double>(kMisses) /
                                     (kMisses + kHits),
                           kHits + kMisses);
    const auto &pool = cache->getPool(poolId);
    auto cms = cache->getCacheMemoryStats();
    return std::make_tuple(cache->getCacheName(), cache->getPoolName(poolId),
                           pool.getCurrentAllocSize(), pool.getPoolSize(),
                           cms.configuredRamCacheRegularSize -
                               cms.unReservedSize,
                           cms.configuredRamCacheRegularSize);
  }
};

DB *NewCacheLibHolpacaOverhead();

} // namespace ycsbc
