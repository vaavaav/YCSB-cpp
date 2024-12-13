//
//  countdown_latch.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>
//

#ifndef YCSB_C_COUNTDOWN_LATCH_H_
#define YCSB_C_COUNTDOWN_LATCH_H_

#include <condition_variable>
#include <mutex>

class CountDownLatch {
 public:
  CountDownLatch(long count) : count_(count) {}
  void Await() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this] { return count_ <= 0; });
  }
  bool AwaitFor(std::chrono::seconds timeout_sec) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout_sec, [this] { return count_ <= 0; });
  }
  void CountDown() {
    std::unique_lock<std::mutex> lock(mu_);
    if (--count_ <= 0) {
      cv_.notify_all();
    }
  }

 private:
  long count_;
  std::mutex mu_;
  std::condition_variable cv_;
};

#endif // YCSB_C_COUNTDOWN_LATCH_H_
