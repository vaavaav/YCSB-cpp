//
//  core_workload.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "acknowledged_counter_generator.h"
#include "counter_generator.h"
#include "db.h"
#include "discrete_generator.h"
#include "generator.h"
#include "properties.h"
#include "utils.h"

#include <fstream>

namespace ycsbc {

enum Operation {
  INSERT = 0,
  READ,
  UPDATE,
  SCAN,
  READMODIFYWRITE,
  DELETE,
  INSERT_PASSED,
  READ_PASSED,
  UPDATE_PASSED,
  SCAN_PASSED,
  READMODIFYWRITE_PASSED,
  DELETE_PASSED,
  INSERT_FAILED,
  READ_FAILED,
  UPDATE_FAILED,
  SCAN_FAILED,
  READMODIFYWRITE_FAILED,
  DELETE_FAILED,
  ALL,
  MAXOPTYPE
};

extern const char *kOperationString[MAXOPTYPE];

class CoreWorkload {
public:
  ///
  /// The name of the database table to run queries against.
  ///
  static const std::string TABLENAME_PROPERTY;
  static const std::string TABLENAME_DEFAULT;

  static const std::string OPERATION_COUNT_PROPERTY;

  static const std::string FILENAME_PROPERTY;
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void Init(std::string const property_suffix,
                    const utils::Properties &p);

  virtual bool DoTransaction(DB &db);
  virtual bool DoInsert(DB &db);

  CoreWorkload() {};

  virtual ~CoreWorkload() {}

  void request_stop() {
    stop_requested_.store(true, std::memory_order_release);
  }

  bool is_stop_requested() { return stop_requested_.load(); }

protected:
  uint64_t NextTransactionKeyNum();
  std::string NextFieldName();

  DB::Status TransactionRead(DB &db, std::string const &key);
  DB::Status TransactionUpdate(DB &db, std::string const &key, size_t size);
  DB::Status TransactionInsert(DB &db, std::string const &key, size_t size);

  // for productiont traces
  std::string BuildValue(size_t size);

  std::tuple<Operation, std::string, size_t> NextOperation();

  std::string table_name_;
  long field_count_;
  std::string field_prefix_;
  // file buffer
  std::ifstream file_buffer_;

private:
  std::atomic_bool stop_requested_ = {false};
};

} // namespace ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
