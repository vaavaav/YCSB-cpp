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

namespace ycsbc {

inline long ClientThread(std::chrono::seconds sleepafterload,
                         std::chrono::seconds maxexecutiontime, int threadId,
                         ycsbc::DB *db, ycsbc::CoreWorkload *wl,
                         const long num_ops, bool load, bool cleanup_db) {
  try {
    db->SetThreadId(threadId);
    db->Init();
    if (sleepafterload.count() > 0) {
      std::this_thread::sleep_for(sleepafterload);
    }
    std::future<void> terminator;
    if (maxexecutiontime.count() > 0) {
      terminator = std::async(std::launch::async, ycsbc::TerminatorThread,
                              maxexecutiontime, wl);
    }

    long ops = 0;

    if (load) {
      for (; ops < num_ops; ++ops) {
        wl->DoInsert(*db);
      }
    } else {
      for (; ops < num_ops; ++ops) {
        if (wl->is_stop_requested()) {
          break;
        }
        wl->DoTransaction(*db);
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
