#pragma once

#ifndef YCSB_C_TRACE_REPLAYER_H_
#define YCSB_C_TRACE_REPLAYER_H_

#include "core_workload.h"

#include <fstream>

namespace ycsbc {

extern const char *kOperationString[MAXOPTYPE];

class TraceReplayer : public CoreWorkload {
public:
  static const std::string SCALE_VALUE_SIZE_PROPERTY;
  static const std::string SCALE_VALUE_SIZE_DEFAULT;

  static const std::string FILENAME_PROPERTY;

  /// Called once, in the main client thread, before any operations are started.
  ///
  void Init(std::string const property_suffix,
            const utils::Properties &p) override final;

  bool DoTransaction(DB &db) override final;
  bool DoInsert(DB &db) override final;

protected:
  DB::Status TransactionRead(DB &db, std::string const &key) override final;
  DB::Status TransactionUpdate(DB &db, std::string const &key,
                               size_t size) override final;
  DB::Status TransactionInsert(DB &db, std::string const &key,
                               size_t size) override final;

  // for productiont traces
  std::string BuildValue(size_t size) override final;
  std::string BuildKeyName(std::string const &k);

  std::tuple<Operation, std::string, size_t> NextOperation();

  // file buffer
  std::ifstream file_buffer_;
  double scale_value_size{1.0};
  std::string request_key_prefix_;
};

} // namespace ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
