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
  static std::unordered_map<std::string, std::pair<Cache, int>> caches_;

  int const threadId_;
  Cache cache_ = nullptr;
  std::string cacheName_;
  std::string poolName_ = "";
  facebook::cachelib::PoolId poolId_;
  std::pair<int, int> missesAndHits_{0, 0};
  std::pair<int, int> previousMissesAndHits_{0, 0};

public:
  explicit CacheLibHolpacaOverhead(int threadId) : threadId_(threadId) {}

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
    if (cache_ == nullptr) {
      return std::make_tuple("", "", 0, 0, 0, 0);
    }

    auto [misses, hits] = missesAndHits_;
    auto &[pmisses, phits] = previousMissesAndHits_;

    int const kMisses = misses - pmisses;
    int const kHits = hits - phits;
    pmisses = misses;
    phits = hits;
    cache_->registerMetrics(poolId_, 0,
                            (kMisses + kHits == 0)
                                ? 0
                                : static_cast<double>(kMisses) /
                                      (kMisses + kHits),
                            kHits + kMisses);
    const auto &pool = cache_->getPool(poolId_);
    auto cms = cache_->getCacheMemoryStats();
    return std::make_tuple(
        cacheName_, poolName_, pool.getCurrentAllocSize(), pool.getPoolSize(),
        cms.configuredRamCacheRegularSize - cms.unReservedSize,
        cms.configuredRamCacheRegularSize);
  }
};

DB *NewCacheLibHolpacaOverhead(int threadId);

} // namespace ycsbc
