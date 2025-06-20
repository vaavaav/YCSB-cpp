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

using namespace std::chrono_literals;

namespace ycsbc {

inline long ClientThread(std::chrono::seconds sleepafterload,
                         std::chrono::seconds maxexecutiontime, int threadId,
                         ycsbc::DB *db, ycsbc::CoreWorkload *wl,
                         const long num_ops, bool load, bool cleanup_db) {
  try {
    db->SetThreadId(threadId);

    if (sleepafterload > 0s) {
      std::this_thread::sleep_for(sleepafterload);
    }

    db->Init();

    std::future<void> terminator;
    if (maxexecutiontime > 0s) {
      std::cout << "Max execution time: " << maxexecutiontime.count()
                << " seconds" << std::endl;
      std::cout << "TESTE " << std::endl;
      terminator = std::async(std::launch::async, ycsbc::TerminatorThread,
                              maxexecutiontime, wl);
    }

    long ops = 0;
    if (load) {
      while (!wl->is_stop_requested()) {
        wl->DoInsert(*db);
        ops++;
      }
    } else {
      while (!wl->is_stop_requested()) {
        wl->DoTransaction(*db);
        ops++;
      }
    }

    if (cleanup_db) {
      db->Cleanup();
    }

    if (maxexecutiontime.count() > 0) {
      terminator.wait();
    }

    return ops;
  } catch (const utils::Exception &e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

} // namespace ycsbc

#endif // YCSB_C_CLIENT_H_
