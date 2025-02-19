#pragma once

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <mutex>

#include "core/db.h"
#include "core/properties.h"

namespace ycsbc {
class RocksDB : public DB {
  static std::mutex db_mutex_;
  static rocksdb::DB *db_;
  static int ref_cnt_;
  void SerializeRow(const std::vector<Field> &values, std::string &data);

  void DeserializeRowFilter(std::vector<Field> &values, const char *p,
                            const char *lim,
                            const std::vector<std::string> &fields);

  void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                            const std::vector<std::string> &fields);

  void DeserializeRow(std::vector<Field> &values, const char *p,
                      const char *lim);

  void DeserializeRow(std::vector<Field> &values, const std::string &data);

public:
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

  void Init();
  void Cleanup();

  void GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                  std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs);

  std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>
  OccupancyCapacityAndGlobal() {
    return std::make_tuple(0, 0, 0, 0);
  }
};

DB *NewRocksDB();
} // namespace ycsbc
