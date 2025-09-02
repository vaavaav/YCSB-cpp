//
//  core_workload.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "trace_replayer.h"

#include <algorithm>
#include <iostream>
#include <string>

#include "random_byte_generator.h"
#include "utils.h"

using std::string;
using ycsbc::TraceReplayer;

const string TraceReplayer::FILENAME_PROPERTY = "trace.file";

const string TraceReplayer::OVERRIDE_VALUE_SIZE_PROPERTY =
    "trace.override_value_size";

namespace ycsbc {

void TraceReplayer::Init(std::string const property_suffix,
                         const utils::Properties &p) {
  table_name_ =
      p.GetProperty(TABLENAME_PROPERTY + property_suffix,
                    p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT));

  std::string file_name = p.GetProperty(FILENAME_PROPERTY + property_suffix);
  file_buffer_.open(file_name, std::ifstream::in);

  request_key_prefix_ = p.GetProperty(
      REQUEST_KEY_PREFIX_PROPERTY + property_suffix,
      p.GetProperty(REQUEST_KEY_PREFIX_PROPERTY, REQUEST_KEY_PREFIX_DEFAULT));

  if (p.ContainsKey(OVERRIDE_VALUE_SIZE_PROPERTY + property_suffix)) {
    override_value_size_set = true;
    override_value_size = std::stoul(
        p.GetProperty(OVERRIDE_VALUE_SIZE_PROPERTY + property_suffix));
  } else if (p.ContainsKey(OVERRIDE_VALUE_SIZE_PROPERTY)) {
    override_value_size_set = true;
    override_value_size =
        std::stoul(p.GetProperty(OVERRIDE_VALUE_SIZE_PROPERTY));
  } else {
    override_value_size_set = false;
  }

  ops_ = 0;

  operation_count_ =
      std::stol(p.GetProperty(OPERATION_COUNT_PROPERTY + property_suffix,
                              p.GetProperty(OPERATION_COUNT_PROPERTY, "0")));
}

std::string TraceReplayer::BuildValue(size_t size) {
  std::string result;
  RandomByteGenerator byteGenerator;
  std::generate_n(std::back_inserter(result), size,
                  [&]() { return byteGenerator.Next(); });
  return result;
}

std::tuple<Operation, std::string, size_t> TraceReplayer::NextOperation() {
  std::string line;
  if (!std::getline(file_buffer_, line)) {
    request_stop();
    return std::make_tuple(MAXOPTYPE, "", 0);
  }

  // split the line by commas
  std::vector<std::string> parts;
  size_t pos = 0;
  while ((pos = line.find(',')) != std::string::npos) {
    parts.push_back(line.substr(0, pos));
    line.erase(0, pos + 1);
  }
  parts.push_back(line);

  std::string key{parts[1]};
  size_t valuesize = std::stoul(std::string(parts[3]));
  std::string operation{parts[5]};

  if (operation == "get") {
    return std::make_tuple(READ, key, valuesize);
  } else if (operation == "set" || operation == "replace") {
    return std::make_tuple(UPDATE, key, valuesize);
  } else if (operation == "add") {
    return std::make_tuple(INSERT, key, valuesize);
  } else {
    return std::make_tuple(MAXOPTYPE, key, valuesize);
  }
}

std::string TraceReplayer::BuildKeyName(const std::string &k) {
  std::string key;
  return key.append(request_key_prefix_).append(k);
}

bool TraceReplayer::DoInsert(DB &db) {
  auto [_, k_, size] = NextOperation();
  if (k_.empty()) {
    return DB::kOK;
  }
  if (override_value_size_set) {
    size = override_value_size;
  }
  const std::string key = BuildKeyName(k_);
  std::vector<DB::Field> fields;
  auto field = DB::Field();
  field.value = BuildValue(size);
  fields.push_back(field);
  return db.Insert(table_name_, key, fields);
}

bool TraceReplayer::DoTransaction(DB &db) {
  DB::Status status;
  auto [op, k_, size] = NextOperation();
  if (override_value_size_set) {
    size = override_value_size;
  }
  const std::string key = BuildKeyName(k_);
  switch (op) {
  case READ:
    status = TransactionRead(db, key);
    ++ops_;
    break;
  case UPDATE:
    status = TransactionUpdate(db, key, size);
    ++ops_;
    break;
  case INSERT:
    status = TransactionInsert(db, key, size);
    ++ops_;
    break;
  default:
    // throw utils::Exception("Operation request is not recognized!");
    status = DB::kError;
    break;
  }

  if (ops_ >= operation_count_) {
    request_stop();
  }

  return (status == DB::kOK);
}

DB::Status TraceReplayer::TransactionRead(DB &db, std::string const &key) {
  std::vector<DB::Field> result;
  return db.Read(table_name_, key, NULL, result);
}

DB::Status TraceReplayer::TransactionUpdate(DB &db, std::string const &key,
                                            size_t size) {
  std::vector<DB::Field> fields;
  auto field = DB::Field();
  field.value = BuildValue(size);
  fields.push_back(field);
  return db.Update(table_name_, key, fields);
}

DB::Status TraceReplayer::TransactionInsert(DB &db, std::string const &key,
                                            size_t size) {
  std::vector<DB::Field> fields;
  auto field = DB::Field();
  field.value = BuildValue(size);
  fields.push_back(field);
  return db.Insert(table_name_, key, fields);
}

} // namespace ycsbc
