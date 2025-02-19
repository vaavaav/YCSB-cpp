//
//  measurements.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_MEASUREMENTS_H_
#define YCSB_C_MEASUREMENTS_H_

#include <atomic>

#include "core_workload.h"
#include "properties.h"

#ifdef HDRMEASUREMENT
#include <hdr/hdr_histogram.h>
#endif

typedef unsigned int uint;

namespace ycsbc {

class Measurements {
public:
  virtual void Report(Operation op, uint64_t latency) = 0;
  virtual std::string
  GetStatusMsg(std::vector<Operation> const &operations) = 0;
  virtual std::string GetCDF() = 0;
  virtual void Reset() = 0;
};

class BasicMeasurements : public Measurements {
public:
  BasicMeasurements();
  void Report(Operation op, uint64_t latency) override;
  std::string GetStatusMsg(std::vector<Operation> const &operations) override;
  std::string GetCDF() override { return ""; }
  void Reset() override;

private:
  std::atomic<uint> count_[MAXOPTYPE];
  std::atomic<uint64_t> latency_sum_[MAXOPTYPE];
  std::atomic<uint64_t> latency_min_[MAXOPTYPE];
  std::atomic<uint64_t> latency_max_[MAXOPTYPE];
};

#ifdef HDRMEASUREMENT
class HdrHistogramMeasurements : public Measurements {
public:
  HdrHistogramMeasurements();
  void Report(Operation op, uint64_t latency) override;
  std::string GetStatusMsg(std::vector<Operation> const &operations) override;
  std::string GetCDF() override;
  void Reset() override;

private:
  hdr_histogram *histogram_[MAXOPTYPE];
};
#endif

Measurements *CreateMeasurements(utils::Properties *props);

} // namespace ycsbc

#endif // YCSB_C_MEASUREMENTS
