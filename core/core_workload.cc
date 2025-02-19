//
//  core_workload.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "core_workload.h"

#include <algorithm>
#include <iostream>
#include <string>

#include "random_byte_generator.h"
#include "utils.h"

using std::string;
using ycsbc::CoreWorkload;

const char *ycsbc::kOperationString[ycsbc::MAXOPTYPE] = {
    "INSERT",
    "READ",
    "UPDATE",
    "SCAN",
    "READMODIFYWRITE",
    "DELETE",
    "INSERT-PASSED",
    "READ-PASSED",
    "UPDATE-PASSED",
    "SCAN-PASSED",
    "READMODIFYWRITE-PASSED",
    "DELETE-PASSED",
    "INSERT-FAILED",
    "READ-FAILED",
    "UPDATE-FAILED",
    "SCAN-FAILED",
    "READMODIFYWRITE-FAILED",
    "DELETE-FAILED",
    "ALL"};

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";
const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";

const string CoreWorkload::SCALE_VALUE_SIZE_PROPERTY = "scalevaluesize";
const string CoreWorkload::SCALE_VALUE_SIZE_DEFAULT = "1.0";

const string CoreWorkload::FILENAME_PROPERTY = "file";

namespace ycsbc {

void CoreWorkload::Init(std::string const property_suffix,
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

std::string CoreWorkload::BuildValue(size_t size) {
  std::string result;
  RandomByteGenerator byteGenerator;
  std::generate_n(std::back_inserter(result), size,
                  [&]() { return byteGenerator.Next(); });
  return result;
}

std::tuple<Operation, std::string, size_t> CoreWorkload::NextOperation() {
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
  size_t keysize = std::stoi(line.substr(0, pos)) * scale_value_size;
  line.erase(0, pos + del.length());
  pos = line.find(del);
  size_t valuesize = std::stoi(line.substr(0, pos));
  line.erase(0, pos + del.length());
  pos = line.find(del);
  line.erase(0, pos + del.length());
  pos = line.find(del);
  std::string operation = line.substr(0, pos);

  if (operation == "get") {
    return std::make_tuple(READ, key, keysize);
  } else if (operation == "set" || operation == "replace") {
    return std::make_tuple(UPDATE, key, keysize);
  } else if (operation == "add") {
    return std::make_tuple(INSERT, key, keysize);
  } else {
    return std::make_tuple(MAXOPTYPE, key, keysize);
  }
}

bool CoreWorkload::DoInsert(DB &db) {
  auto [_, key, size] = NextOperation();
  std::vector<DB::Field> fields;
  auto field = DB::Field();
  field.value = BuildValue(size);
  fields.push_back(field);
  return db.Insert(table_name_, key, fields);
}

bool CoreWorkload::DoTransaction(DB &db) {
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

DB::Status CoreWorkload::TransactionRead(DB &db, std::string const &key) {
  std::vector<DB::Field> result;
  return db.Read(table_name_, key, NULL, result);
}

DB::Status CoreWorkload::TransactionUpdate(DB &db, std::string const &key,
                                           size_t size) {
  std::vector<DB::Field> fields;
  auto field = DB::Field();
  field.value = BuildValue(size);
  fields.push_back(field);
  return db.Update(table_name_, key, fields);
}

DB::Status CoreWorkload::TransactionInsert(DB &db, std::string const &key,
                                           size_t size) {
  std::vector<DB::Field> fields;
  auto field = DB::Field();
  field.value = BuildValue(size);
  fields.push_back(field);
  return db.Insert(table_name_, key, fields);
}

} // namespace ycsbc
