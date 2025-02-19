#pragma once

#include "cachelib/allocator/CacheAllocator.h"
#include "rocksdb.h"
#include <core/db.h>

namespace ycsbc {

class CacheLib : public DB {
public:
  void Init();
  void SetThreadId(int id) override;

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

  std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>
  OccupancyCapacityAndGlobal() {
    if (cache_ == nullptr) {
      return std::make_tuple(0, 0, 0, 0);
    }
    auto ps = cache_->getPoolStats(poolId_);
    auto cms = cache_->getCacheMemoryStats();
    return std::make_tuple(ps.poolSize - ps.freeMemoryBytes(), ps.poolSize,
                           cms.configuredRamCacheRegularSize -
                               cms.unReservedSize,
                           cms.configuredRamCacheRegularSize);
  }

private:
  using Cache = facebook::cachelib::Lru2QAllocator; // or Lru2QAllocator, or
                                                    // TinyLFUAllocator
  using CacheConfig = typename Cache::Config;
  using CacheKey = typename Cache::Key;
  using CacheReadHandle = typename Cache::ReadHandle;
  //
  static std::mutex mutex_;
  static RocksDB rocksdb_;
  thread_local static facebook::cachelib::PoolId poolId_;
  thread_local static int threadId_;
  static std::shared_ptr<Cache> cache_;
  static int ref_cnt_;
};

DB *NewCacheLib();

} // namespace ycsbc
