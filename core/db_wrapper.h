//
//  db_wrapper.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_DB_WRAPPER_H_
#define YCSB_C_DB_WRAPPER_H_

#include <string>
#include <vector>

#include "db.h"
#include "measurements.h"
#include "timer.h"
#include "utils.h"

namespace ycsbc {

class DBWrapper : public DB {
 public:
  DBWrapper(DB* db, Measurements* measurements, Measurements* gMeasurements)
      : db_(db), measurements_(measurements), gMeasurements_(gMeasurements) {}
  ~DBWrapper() { delete db_; }
  void Init() { db_->Init(); }
  void Cleanup() { db_->Cleanup(); }
  Status Read(const std::string& table,
              const std::string& key,
              const std::vector<std::string>* fields,
              std::vector<Field>& result) {
    timer_.Start();
    Status s = db_->Read(table, key, fields, result);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(READ_PASSED, elapsed);
      gMeasurements_->Report(READ_PASSED, elapsed);
    } else {
      measurements_->Report(READ_FAILED, elapsed);
      gMeasurements_->Report(READ_FAILED, elapsed);
    }
    measurements_->Report(READ, elapsed);
    gMeasurements_->Report(READ, elapsed);
    measurements_->Report(ALL, elapsed);
    gMeasurements_->Report(ALL, elapsed);
    return s;
  }
  Status Scan(const std::string& table,
              const std::string& key,
              long record_count,
              const std::vector<std::string>* fields,
              std::vector<std::vector<Field>>& result) {
    timer_.Start();
    Status s = db_->Scan(table, key, record_count, fields, result);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(SCAN_PASSED, elapsed);
      gMeasurements_->Report(SCAN_PASSED, elapsed);
    } else {
      measurements_->Report(SCAN_FAILED, elapsed);
      gMeasurements_->Report(SCAN_FAILED, elapsed);
    }
    measurements_->Report(SCAN, elapsed);
    gMeasurements_->Report(SCAN, elapsed);
    measurements_->Report(ALL, elapsed);
    gMeasurements_->Report(ALL, elapsed);
    return s;
  }
  Status Update(const std::string& table,
                const std::string& key,
                std::vector<Field>& values) {
    timer_.Start();
    Status s = db_->Update(table, key, values);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(UPDATE_PASSED, elapsed);
      gMeasurements_->Report(UPDATE_PASSED, elapsed);
    } else {
      measurements_->Report(UPDATE_FAILED, elapsed);
      gMeasurements_->Report(UPDATE_FAILED, elapsed);
    }
    measurements_->Report(UPDATE, elapsed);
    gMeasurements_->Report(UPDATE, elapsed);
    measurements_->Report(ALL, elapsed);
    gMeasurements_->Report(ALL, elapsed);
    return s;
  }
  Status Insert(const std::string& table,
                const std::string& key,
                std::vector<Field>& values) {
    timer_.Start();
    Status s = db_->Insert(table, key, values);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(INSERT_PASSED, elapsed);
      gMeasurements_->Report(INSERT_PASSED, elapsed);
    } else {
      measurements_->Report(INSERT_FAILED, elapsed);
      gMeasurements_->Report(INSERT_FAILED, elapsed);
    }
    measurements_->Report(INSERT, elapsed);
    gMeasurements_->Report(INSERT, elapsed);
    measurements_->Report(ALL, elapsed);
    gMeasurements_->Report(ALL, elapsed);
    return s;
  }
  Status Delete(const std::string& table, const std::string& key) {
    timer_.Start();
    Status s = db_->Delete(table, key);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(DELETE_PASSED, elapsed);
      gMeasurements_->Report(DELETE_PASSED, elapsed);
    } else {
      measurements_->Report(DELETE_FAILED, elapsed);
      gMeasurements_->Report(DELETE_FAILED, elapsed);
    }
    measurements_->Report(DELETE, elapsed);
    gMeasurements_->Report(DELETE, elapsed);
    measurements_->Report(ALL, elapsed);
    gMeasurements_->Report(ALL, elapsed);
    return s;
  }

  void SetThreadId(int id) { db_->SetThreadId(id); }

 private:
  DB* db_;
  Measurements* measurements_;
  Measurements* gMeasurements_;
  utils::Timer<uint64_t, std::nano> timer_;
};

} // namespace ycsbc

#endif // YCSB_C_DB_WRAPPER_H_
