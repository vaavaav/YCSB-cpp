#pragma once
#include "core_workload.h"
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

namespace ycsbc {
inline void TerminatorThread(std::chrono::seconds max_execution_time,
                             ycsbc::CoreWorkload *wl,
                             std::condition_variable &cv, std::mutex &m) {
  std::unique_lock<std::mutex> lock(m);
  if (!cv.wait_for(lock, max_execution_time,
                   [wl]() { return wl->operations_done(); })) {
    wl->stop_operations();
  }
}

} // namespace ycsbc
