//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <iostream>
#include <string>

#include "core_workload.h"
#include "countdown_latch.h"
#include "db.h"
#include "terminator_thread.h"
#include "utils.h"
#include <condition_variable>
#include <mutex>

using namespace std::chrono_literals;

namespace ycsbc {

void ClientThread(std::chrono::seconds sleepafterload,
                  std::chrono::seconds maxexecutiontime, int threadId,
                  ycsbc::DB *db, ycsbc::CoreWorkload *wl, const long num_ops,
                  bool load, bool cleanup_db) {
  try {
    if (sleepafterload > 0s) {
      std::this_thread::sleep_for(sleepafterload);
    }

    db->Init();

    std::condition_variable cv;
    std::mutex m;
    std::thread terminator;

    if (maxexecutiontime > 0s) {
      terminator = std::thread(TerminatorThread, maxexecutiontime, wl,
                               std::ref(cv), std::ref(m));
    }

    if (load) {
      while (!wl->inserts_done()) {
        wl->DoInsert(*db);
      }
    } else {
      while (!wl->operations_done()) {
        wl->DoTransaction(*db);
      }
    }

    if (cleanup_db) {
      db->Cleanup();
    }

    {
      std::lock_guard<std::mutex> lock(m);
      cv.notify_one();
    }

    if (maxexecutiontime > 0s) {
      terminator.join();
    }

  } catch (const utils::Exception &e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

} // namespace ycsbc

#endif // YCSB_C_CLIENT_H_
