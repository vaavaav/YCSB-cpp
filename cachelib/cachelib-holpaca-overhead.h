#pragma once

#include <atomic>
#include <cachelib/holpaca/data-plane/CacheAllocator.h>
#include <core/db.h>
#include <unordered_map>
#include <variant>

namespace ycsbc {

class CacheLibHolpacaOverhead : public DB {

public:
  using CacheHolpacaLRU = facebook::cachelib::holpaca::LruAllocator;
  using CacheBaselineLRU = facebook::cachelib::LruAllocator;
  using Cache = std::variant<std::shared_ptr<CacheHolpacaLRU>,
                             std::shared_ptr<CacheBaselineLRU>>;
  using Config =
      std::variant<CacheHolpacaLRU::Config, CacheBaselineLRU::Config>;

private:
  static std::mutex mutex_;
  static std::unordered_map<std::string, Cache> caches_;
  static std::unordered_map<int, std::tuple<Cache, facebook::cachelib::PoolId>>
      cachesPerThread_;

  struct missesAndHits {
    int misses;
    int hits;
  };

  static std::unordered_map<std::string, int> refCountPerCache_;
  thread_local static std::string cacheName_;
  thread_local static Cache cache_;
  thread_local static int threadId_;
  thread_local static facebook::cachelib::PoolId poolId_;
  static std::unordered_map<int, std::atomic<missesAndHits>>
      missesAndHitsPerThread_;
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
        std::visit([](auto &&cache) { return cache == nullptr; },
                   std::get<0>(cachesPerThread_[i]))) {
      return std::make_tuple("", "", 0, 0, 0, 0);
    }
    auto [cache, poolId] = cachesPerThread_[i];
    if (std::holds_alternative<std::shared_ptr<CacheHolpacaLRU>>(cache)) {
      auto c = std::get<std::shared_ptr<CacheHolpacaLRU>>(cache);
      auto const &accMissesAndHits = missesAndHitsPerThread_[i].load();

      int misses =
          accMissesAndHits.misses - previousMissesAndHitsPerThread_[i].first;

      int hits =
          accMissesAndHits.hits - previousMissesAndHitsPerThread_[i].second;

      c->registerMetrics(poolId, 0,
                         (misses + hits == 0)
                             ? 0
                             : static_cast<double>(misses) / (misses + hits),
                         hits + misses);

      previousMissesAndHitsPerThread_[i].first = accMissesAndHits.misses;
      previousMissesAndHitsPerThread_[i].second = accMissesAndHits.hits;
    }
    return std::visit(
        [i, poolId](auto &&c) {
          const auto &pool = c->getPool(poolId);
          auto cms = c->getCacheMemoryStats();
          return std::make_tuple(c->getCacheName(), c->getPoolName(poolId),
                                 pool.getCurrentAllocSize(), pool.getPoolSize(),
                                 cms.configuredRamCacheRegularSize -
                                     cms.unReservedSize,
                                 cms.configuredRamCacheRegularSize);
        },
        cache);
  }
};

DB *NewCacheLibHolpacaOverhead();

} // namespace ycsbc
