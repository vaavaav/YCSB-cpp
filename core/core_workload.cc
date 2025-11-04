//
//  core_workload.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "core_workload.h"
#include <iostream>

#include <algorithm>
#include <random>
#include <string>

#include "const_generator.h"
#include "random_byte_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "uniform_generator.h"
#include "utils.h"
#include "zipfian_generator.h"

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

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT = "10";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY =
    "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT = "100";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY =
    "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY =
    "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::ZERO_PADDING_PROPERTY = "zeropadding";
const string CoreWorkload::ZERO_PADDING_DEFAULT = "1";

const string CoreWorkload::MIN_SCAN_LENGTH_PROPERTY = "minscanlength";
const string CoreWorkload::MIN_SCAN_LENGTH_DEFAULT = "1";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY =
    "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT = "hashed";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

const std::string CoreWorkload::FIELD_NAME_PREFIX = "fieldnameprefix";
const std::string CoreWorkload::FIELD_NAME_PREFIX_DEFAULT = "field";

const std::string CoreWorkload::ZIPFIAN_CONST_PROPERTY = "zipfian_const";
const std::string CoreWorkload::ZIPFIAN_CONST_DEFAULT = "0.99";

const std::string CoreWorkload::REQUEST_KEY_DOMAIN_START_PROPERTY =
    "request_key_domain_start";
const std::string CoreWorkload::REQUEST_KEY_DOMAIN_START_DEFAULT = "0";

const std::string CoreWorkload::REQUEST_KEY_DOMAIN_END_PROPERTY =
    "request_key_domain_end";

const std::string CoreWorkload::REQUEST_KEY_PREFIX_PROPERTY =
    "request_key_prefix";
const std::string CoreWorkload::REQUEST_KEY_PREFIX_DEFAULT = "key";

namespace ycsbc {

void CoreWorkload::Init(std::string const property_suffix,
                        const utils::Properties &p) {
  table_name_ =
      p.GetProperty(TABLENAME_PROPERTY + property_suffix,
                    p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT));
  field_count_ = std::stol(
      p.GetProperty(FIELD_COUNT_PROPERTY + property_suffix,
                    p.GetProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_DEFAULT)));
  field_prefix_ = p.GetProperty(
      FIELD_NAME_PREFIX + property_suffix,
      p.GetProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT));
  field_len_generator_ = GetFieldLenGenerator(property_suffix, p);

  request_key_prefix_ = p.GetProperty(
      REQUEST_KEY_PREFIX_PROPERTY + property_suffix,
      p.GetProperty(REQUEST_KEY_PREFIX_PROPERTY, REQUEST_KEY_PREFIX_DEFAULT));

  double read_proportion = std::stod(p.GetProperty(
      READ_PROPORTION_PROPERTY + property_suffix,
      p.GetProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_DEFAULT)));
  double update_proportion = std::stod(p.GetProperty(
      UPDATE_PROPORTION_PROPERTY + property_suffix,
      p.GetProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_DEFAULT)));
  double insert_proportion = std::stod(p.GetProperty(
      INSERT_PROPORTION_PROPERTY + property_suffix,
      p.GetProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_DEFAULT)));
  double scan_proportion = std::stod(p.GetProperty(
      SCAN_PROPORTION_PROPERTY + property_suffix,
      p.GetProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_DEFAULT)));
  double readmodifywrite_proportion = std::stod(
      p.GetProperty(READMODIFYWRITE_PROPORTION_PROPERTY + property_suffix,
                    p.GetProperty(READMODIFYWRITE_PROPORTION_PROPERTY,
                                  READMODIFYWRITE_PROPORTION_DEFAULT)));

  record_count_ =
      std::stol(p.GetProperty(RECORD_COUNT_PROPERTY + property_suffix,
                              p.GetProperty(RECORD_COUNT_PROPERTY)));

  inserts_ = 0;

  operation_count_ =
      std::stol(p.GetProperty(OPERATION_COUNT_PROPERTY + property_suffix,
                              p.GetProperty(OPERATION_COUNT_PROPERTY)));

  ops_ = 0;

  long min_scan_len = std::stol(p.GetProperty(
      MIN_SCAN_LENGTH_PROPERTY + property_suffix,
      p.GetProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_DEFAULT)));
  long max_scan_len = std::stol(p.GetProperty(
      MAX_SCAN_LENGTH_PROPERTY + property_suffix,
      p.GetProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_DEFAULT)));
  std::string scan_len_dist =
      p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY + property_suffix,
                    p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                  SCAN_LENGTH_DISTRIBUTION_DEFAULT));
  long insert_start = std::stol(p.GetProperty(
      INSERT_START_PROPERTY + property_suffix,
      p.GetProperty(INSERT_START_PROPERTY, INSERT_START_DEFAULT)));

  zero_padding_ = std::stol(p.GetProperty(
      ZERO_PADDING_PROPERTY + property_suffix,
      p.GetProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_DEFAULT)));

  read_all_fields_ = utils::StrToBool(p.GetProperty(
      READ_ALL_FIELDS_PROPERTY + property_suffix,
      p.GetProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_DEFAULT)));
  write_all_fields_ = utils::StrToBool(p.GetProperty(
      WRITE_ALL_FIELDS_PROPERTY + property_suffix,
      p.GetProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_DEFAULT)));

  if (p.GetProperty(INSERT_ORDER_PROPERTY + property_suffix,
                    p.GetProperty(INSERT_ORDER_PROPERTY,
                                  INSERT_ORDER_DEFAULT)) == "hashed") {
    ordered_inserts_ = false;
  } else {
    ordered_inserts_ = true;
  }

  if (read_proportion > 0) {
    op_chooser_.AddValue(READ, read_proportion);
  }
  if (update_proportion > 0) {
    op_chooser_.AddValue(UPDATE, update_proportion);
  }
  if (insert_proportion > 0) {
    op_chooser_.AddValue(INSERT, insert_proportion);
  }
  if (scan_proportion > 0) {
    op_chooser_.AddValue(SCAN, scan_proportion);
  }
  if (readmodifywrite_proportion > 0) {
    op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion);
  }

  insert_key_sequence_ = new CounterGenerator(insert_start);
  transaction_insert_key_sequence_ =
      new AcknowledgedCounterGenerator(record_count_);

  // If the number of keys changes, we don't want to change popular keys.
  // So we construct the scrambled zipfian generator with a keyspace
  // that is larger than what exists at the beginning of the test.
  // If the generator picks a key that is not inserted yet, we just ignore it
  // and pick another key.
  long op_count =
      std::stol(p.GetProperty(OPERATION_COUNT_PROPERTY + property_suffix,
                              p.GetProperty(OPERATION_COUNT_PROPERTY)));
  long new_keys = (long)(op_count * insert_proportion); // a fudge factor
  long request_key_domain_start = std::stol(
      p.GetProperty(REQUEST_KEY_DOMAIN_START_PROPERTY + property_suffix,
                    p.GetProperty(REQUEST_KEY_DOMAIN_START_PROPERTY,
                                  REQUEST_KEY_DOMAIN_START_DEFAULT)));
  if (request_key_domain_start < 0) {
    throw utils::Exception("Request key domain start is lesser than 0: " +
                           std::to_string(request_key_domain_start));
  }
  long request_key_domain_end = std::stol(p.GetProperty(
      REQUEST_KEY_DOMAIN_END_PROPERTY + property_suffix,
      p.GetProperty(REQUEST_KEY_DOMAIN_END_PROPERTY,
                    std::to_string(record_count_ + new_keys - 1))));
  if (request_key_domain_end > record_count_ + new_keys - 1) {
    throw utils::Exception("Request key domain end is greater than " +
                           std::to_string(record_count_ + new_keys - 1) + ": " +
                           std::to_string(request_key_domain_end));
  }
  std::string request_dist =
      p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY + property_suffix,
                    p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                  REQUEST_DISTRIBUTION_DEFAULT));
  if (request_dist == "uniform") {
    key_chooser_ =
        new UniformGenerator(request_key_domain_start, request_key_domain_end);
  } else if (request_dist == "zipfian") {
    double zipfian_const = std::stod(p.GetProperty(
        ZIPFIAN_CONST_PROPERTY + property_suffix,
        p.GetProperty(ZIPFIAN_CONST_PROPERTY, ZIPFIAN_CONST_DEFAULT)));
    key_chooser_ = new ZipfianGenerator(request_key_domain_start,
                                        request_key_domain_end, zipfian_const);
  } else {
    throw utils::Exception("Distribution not allowed for request: " +
                           request_dist);
  }

  field_chooser_ = new UniformGenerator(0, field_count_ - 1);

  if (scan_len_dist == "uniform") {
    scan_len_chooser_ = new UniformGenerator(min_scan_len, max_scan_len);
  } else if (scan_len_dist == "zipfian") {
    scan_len_chooser_ = new ZipfianGenerator(min_scan_len, max_scan_len);
  } else {
    throw utils::Exception("Distribution not allowed for scan length: " +
                           scan_len_dist);
  }
}

ycsbc::Generator<uint64_t> *
CoreWorkload::GetFieldLenGenerator(std::string property_suffix,
                                   const utils::Properties &p) {
  string field_len_dist =
      p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY + property_suffix,
                    p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                  FIELD_LENGTH_DISTRIBUTION_DEFAULT));
  long field_len = std::stol(p.GetProperty(
      FIELD_LENGTH_PROPERTY + property_suffix,
      p.GetProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_DEFAULT)));
  if (field_len_dist == "constant") {
    return new ConstGenerator(field_len);
  } else if (field_len_dist == "uniform") {
    return new UniformGenerator(1, field_len);
  } else if (field_len_dist == "zipfian") {
    return new ZipfianGenerator(1, field_len);
  } else {
    throw utils::Exception("Unknown field length distribution: " +
                           field_len_dist);
  }
}

std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
  if (!ordered_inserts_) {
    key_num = utils::Hash(key_num);
  }
  std::string value = std::to_string(key_num);
  long fill = std::max(0l, zero_padding_ - static_cast<long>(value.size()));
  std::string key;
  return key.append(request_key_prefix_)
      .append("+")
      .append(fill, '0')
      .append(value);
}

void CoreWorkload::BuildValues(std::vector<ycsbc::DB::Field> &values) {
  for (long i = 0; i < field_count_; ++i) {
    values.push_back(DB::Field());
    ycsbc::DB::Field &field = values.back();
    field.name.append(field_prefix_).append(std::to_string(i));
    uint64_t len = field_len_generator_->Next();
    field.value.reserve(len);
    RandomByteGenerator byte_generator;
    std::generate_n(std::back_inserter(field.value), len,
                    [&]() { return byte_generator.Next(); });
  }
}

std::string CoreWorkload::BuildValue(size_t size) {
  std::string result;
  RandomByteGenerator byteGenerator;
  std::generate_n(std::back_inserter(result), size,
                  [&]() { return byteGenerator.Next(); });
  return result;
}

void CoreWorkload::BuildSingleValue(std::vector<ycsbc::DB::Field> &values) {
  values.push_back(DB::Field());
  ycsbc::DB::Field &field = values.back();
  field.name.append(NextFieldName());
  uint64_t len = field_len_generator_->Next();
  field.value.reserve(len);
  RandomByteGenerator byte_generator;
  std::generate_n(std::back_inserter(field.value), len,
                  [&]() { return byte_generator.Next(); });
}

uint64_t CoreWorkload::NextTransactionKeyNum() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > transaction_insert_key_sequence_->Last());
  return key_num;
}

std::string CoreWorkload::NextFieldName() {
  return std::string(field_prefix_)
      .append(std::to_string(field_chooser_->Next()));
}

bool CoreWorkload::DoInsert(DB &db) {
  const std::string key = BuildKeyName(insert_key_sequence_->Next());
  std::vector<DB::Field> fields;
  BuildSingleValue(fields);
  if (++inserts_ >= record_count_) {
    stop_inserts();
  }
  return db.Insert(table_name_, key, fields) == DB::kOK;
}

bool CoreWorkload::DoTransaction(DB &db) {
  DB::Status status;
  switch (op_chooser_.Next()) {
  case READ:
    status = TransactionRead(db);
    break;
  case UPDATE:
    status = TransactionUpdate(db);
    break;
  case INSERT:
    status = TransactionInsert(db);
    break;
  case SCAN:
    status = TransactionScan(db);
    break;
  case READMODIFYWRITE:
    status = TransactionReadModifyWrite(db);
    break;
  default:
    throw utils::Exception("Operation request is not recognized!");
  }
  if (++ops_ >= operation_count_) {
    stop_operations();
  }

  return (status == DB::kOK);
}

DB::Status CoreWorkload::TransactionRead(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> result;
  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    return db.Read(table_name_, key, &fields, result);
  } else {
    return db.Read(table_name_, key, NULL, result);
  }
}

DB::Status CoreWorkload::TransactionRead(DB &db, std::string const &key) {
  std::vector<DB::Field> result;
  return db.Read(table_name_, key, NULL, result);
}

DB::Status CoreWorkload::TransactionReadModifyWrite(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> result;

  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    db.Read(table_name_, key, &fields, result);
  } else {
    db.Read(table_name_, key, NULL, result);
  }

  std::vector<DB::Field> values;
  if (write_all_fields()) {
    BuildValues(values);
  } else {
    BuildSingleValue(values);
  }
  return db.Update(table_name_, key, values);
}

DB::Status CoreWorkload::TransactionReadModifyWrite(DB &db,
                                                    std::string const &key,
                                                    size_t objectSize) {
  std::vector<DB::Field> result;
  db.Read(table_name_, key, NULL, result);
  std::vector<DB::Field> values;
  DB::Field field;
  field.value = BuildValue(objectSize);
  values.push_back(field);
  return db.Update(table_name_, key, values);
}

DB::Status CoreWorkload::TransactionScan(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  const std::string key = BuildKeyName(key_num);
  long len = scan_len_chooser_->Next();
  std::vector<std::vector<DB::Field>> result;
  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    return db.Scan(table_name_, key, len, &fields, result);
  } else {
    return db.Scan(table_name_, key, len, NULL, result);
  }
}

DB::Status CoreWorkload::TransactionUpdate(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> values;
  if (write_all_fields()) {
    BuildValues(values);
  } else {
    BuildSingleValue(values);
  }
  return db.Update(table_name_, key, values);
}

DB::Status CoreWorkload::TransactionUpdate(DB &db, std::string const &key,
                                           size_t objectSize) {
  std::vector<DB::Field> values;
  DB::Field field;
  field.value = BuildValue(objectSize);
  return db.Update(table_name_, key, values);
}

DB::Status CoreWorkload::TransactionInsert(DB &db) {
  uint64_t key_num = transaction_insert_key_sequence_->Next();
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> values;
  BuildValues(values);
  DB::Status s = db.Insert(table_name_, key, values);
  transaction_insert_key_sequence_->Acknowledge(key_num);
  return s;
}

DB::Status CoreWorkload::TransactionInsert(DB &db, std::string const &key,
                                           size_t objectSize) {
  std::vector<DB::Field> values;
  DB::Field field;
  field.value = BuildValue(objectSize);
  return db.Insert(table_name_, key, values);
}

} // namespace ycsbc
