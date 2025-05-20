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

const string TraceReplayer::SCALE_VALUE_SIZE_PROPERTY = "trace.scale_value";
const string TraceReplayer::SCALE_VALUE_SIZE_DEFAULT = "1.0";

const string TraceReplayer::FILENAME_PROPERTY = "trace.file";

namespace ycsbc {

void TraceReplayer::Init(std::string const property_suffix,
                         const utils::Properties &p) {
  table_name_ =
      p.GetProperty(TABLENAME_PROPERTY + property_suffix,
                    p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT));

  std::string file_name = p.GetProperty(FILENAME_PROPERTY + property_suffix);
  file_buffer_.open(file_name, std::ifstream::in);

  scale_value_size = std::stod(p.GetProperty(
      SCALE_VALUE_SIZE_PROPERTY + property_suffix,
      p.GetProperty(SCALE_VALUE_SIZE_PROPERTY, SCALE_VALUE_SIZE_DEFAULT)));
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
  file_buffer_ >> line;
  if (line.empty()) {
    return std::make_tuple(MAXOPTYPE, "", 0);
  }

  std::string del = ",";
  auto pos = line.find(del);
  line.erase(0, pos + del.length());
  pos = line.find(del);
  std::string key = line.substr(0, pos);
  line.erase(0, pos + del.length());
  pos = line.find(del);
  line.erase(0, pos + del.length());
  pos = line.find(del);
  size_t valuesize = std::stoi(line.substr(0, pos)) * scale_value_size;
  line.erase(0, pos + del.length());
  pos = line.find(del);
  line.erase(0, pos + del.length());
  pos = line.find(del);
  std::string operation = line.substr(0, pos);

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

bool TraceReplayer::DoInsert(DB &db) {
  auto [_, key, size] = NextOperation();
  std::vector<DB::Field> fields;
  auto field = DB::Field();
  field.value = BuildValue(size);
  fields.push_back(field);
  return db.Insert(table_name_, key, fields);
}

bool TraceReplayer::DoTransaction(DB &db) {
  DB::Status status;
  auto [op, key, size] = NextOperation();
  switch (op) {
  case READ:
    status = TransactionRead(db, key);
    break;
  case UPDATE:
    status = TransactionUpdate(db, key, size);
    break;
  case INSERT:
    status = TransactionInsert(db, key, size);
    break;
  default:
    // throw utils::Exception("Operation request is not recognized!");
    status = DB::kOK;
    break;
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
