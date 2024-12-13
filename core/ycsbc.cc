//
//  ycsbc.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <atomic>
#include <chrono>
#include <cstring>
#include <ctime>
#include <future>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "client.h"
#include "core_workload.h"
#include "db_factory.h"
#include "measurements.h"
#include "timer.h"
#include "utils.h"

using namespace std::chrono_literals;

static const std::unordered_set<std::string> kOperationTypes = {
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

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[],
                      ycsbc::utils::Properties &props);

void StatusThread(std::vector<ycsbc::Measurements *> *measurements,
                  ycsbc::Measurements *gMeasurements,
                  std::vector<ycsbc::Operation> *operations,
                  std::atomic_bool *done, std::chrono::seconds interval = 1s) {
  using namespace std::chrono;

  auto start = high_resolution_clock::now();
  while (1) {
    auto now = high_resolution_clock::now();
    auto elapsed_time = duration_cast<std::chrono::seconds>(now - start);

    std::cout << elapsed_time.count() << " sec: ";

    std::cout << "global { " + gMeasurements->GetStatusMsg(*operations) + " } ";
    for (long i = 0; i < measurements->size(); i++) {
      std::cout << "worker-" + std::to_string(i) + " { " +
                       measurements->at(i)->GetStatusMsg(*operations) + " } ";
    }
    std::cout << std::endl;
    if (done->load()) {
      break;
    }
    std::this_thread::sleep_until(now + 1s);
  };
}

int main(const int argc, const char *argv[]) {
  ycsbc::utils::Properties props;
  ParseCommandLine(argc, argv, props);

  const bool do_load = (props.GetProperty("doload", "false") == "true");
  const bool do_transaction =
      (props.GetProperty("dotransaction", "false") == "true");
  if (!do_load && !do_transaction) {
    std::cerr << "No operation to do" << std::endl;
    exit(1);
  }
  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  std::vector<ycsbc::Operation> operationsForStatus;
  for (int i = 0; i < ycsbc::Operation::MAXOPTYPE; i++) {
    if (props.ContainsKey("status." +
                          std::string(ycsbc::kOperationString[i]))) {
      operationsForStatus.push_back(static_cast<ycsbc::Operation>(i));
    }
  }

  std::vector<ycsbc::DB *> dbs;
  std::vector<ycsbc::Measurements *> measurements;
  ycsbc::Measurements *gMeasurements = ycsbc::CreateMeasurements(&props);
  for (int i = 0; i < num_threads; i++) {
    measurements.push_back(ycsbc::CreateMeasurements(&props));
    if (measurements[i] == nullptr) {
      std::cerr << "Unknown measurements name" << std::endl;
      exit(1);
    }
    ycsbc::DB *db =
        ycsbc::DBFactory::CreateDB(&props, measurements[i], gMeasurements);
    if (db == nullptr) {
      std::cerr << "Unknown database name " << props["dbname"] << std::endl;
      exit(1);
    }
    dbs.push_back(db);
  }

  std::vector<ycsbc::CoreWorkload *> wls;
  for (int i = 0; i < num_threads; i++) {
    ycsbc::CoreWorkload *wl = new ycsbc::CoreWorkload();
    wl->Init("." + std::to_string(i), props);
    wls.push_back(wl);
  }

  const bool show_status = (props.GetProperty("status", "false") == "true");
  const std::chrono::seconds status_interval = std::chrono::seconds(
      std::stoi(props.GetProperty("status.interval", "10")));

  if (do_transaction) {
    const long total_ops =
        std::stol(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

    std::atomic_bool done(false);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::thread status_thread;
    if (show_status) {
      status_thread = std::thread(StatusThread, &measurements, gMeasurements,
                                  &operationsForStatus, &done, status_interval);
    }
    std::vector<std::future<long>> client_threads;
    for (int i = 0; i < num_threads; ++i) {
      long thread_ops = stol(props.GetProperty(
          ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY + "." +
              std::to_string(i),
          props.GetProperty(ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY,
                            "0")));
      std::chrono::seconds maxexecutiontime = std::chrono::seconds(
          stoi(props.GetProperty("maxexecutiontime." + std::to_string(i),
                                 props.GetProperty("maxexecutiontime", "0"))));
      std::chrono::seconds sleepafterload = std::chrono::seconds(
          stoi(props.GetProperty("sleepafterload." + std::to_string(i),
                                 props.GetProperty("sleepafterload", "0"))));
      if (i < total_ops % num_threads) {
        thread_ops++;
      }
      client_threads.emplace_back(
          std::async(std::launch::async, ycsbc::ClientThread, sleepafterload,
                     maxexecutiontime, i, dbs[i], wls[i], thread_ops, false,
                     !do_load, true));
    }
    assert((int)client_threads.size() == num_threads);

    long sum = 0;
    for (auto &n : client_threads) {
      assert(n.valid());
      sum += n.get();
    }
    done.store(true);
    double runtime = timer.End();

    if (show_status) {
      status_thread.join();
    }

    std::cout << "Run runtime(sec): " << runtime << std::endl;
    std::cout << "Run operations(ops): " << sum << std::endl;
    std::cout << "Run throughput(ops/sec): " << sum / runtime << std::endl;
    std::cout << gMeasurements->GetCDF() << std::endl;
  }

  for (int i = 0; i < num_threads; i++) {
    delete dbs[i];
    delete wls[i];
  }
}

void ParseCommandLine(int argc, const char *argv[],
                      ycsbc::utils::Properties &props) {
  int argindex = 1;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -threads" << std::endl;
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -db" << std::endl;
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-p") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -p" << std::endl;
        exit(0);
      }
      std::string prop(argv[argindex]);
      size_t eq = prop.find('=');
      if (eq == std::string::npos) {
        std::cerr << "Argument '-p' expected to be in key=value format "
                     "(e.g., -p operationcount=99999)"
                  << std::endl;
        exit(0);
      }
      props.SetProperty(ycsbc::utils::Trim(prop.substr(0, eq)),
                        ycsbc::utils::Trim(prop.substr(eq + 1)));
      argindex++;
    } else if (strcmp(argv[argindex], "-s") == 0) {
      props.SetProperty("status", "true");
      argindex++;
      // get list of operations to print in the status message
      // (space-separated), name must be in uppercase like in the Operation
      // enum
      while (argindex < argc && !StrStartWith(argv[argindex], "-")) {
        if (kOperationTypes.find(std::string(argv[argindex])) !=
            kOperationTypes.end()) {
          props.SetProperty("status." + std::string(argv[argindex]), "true");
          argindex++;
        } else {
          UsageMessage(argv[0]);
          std::cerr << "Unknown operation '" << argv[argindex] << "'"
                    << std::endl;
          exit(0);
        }
      }
    } else {
      UsageMessage(argv[0]);
      std::cerr << "Unknown option '" << argv[argindex] << "'" << std::endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char *command) {
  std::cout
      << "Usage: " << command
      << " [options]\n"
         "Options:\n"
         "  -threads n: execute using n threads (default: 1)\n"
         "  -db dbname: specify the name of the DB to use (default: basic)\n"
         "  -p name=value: specify a property to be passed to the DB and "
         "workloads\n"
         "                 multiple properties can be specified, and override "
         "any\n"
         "                 values in the propertyfile\n"
         "  -s: print status every 10 seconds (use status.interval prop to "
         "override)"
      << std::endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}
