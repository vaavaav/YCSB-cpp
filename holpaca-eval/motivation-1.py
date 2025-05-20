#!/usr/bin/env python3

from RunYCSB import RunYCSB

import subprocess
import shutil
import sys
import os
import time

name = f"motivation-1-{int(time.time()*1e9)}"
runs = 3
status = 'READ-FAILED READ-PASSED ALL'

if __name__ == '__main__':
    threads = int(sys.argv[1])
    maxexecutiontime = int(sys.argv[2])
    sourceDir = os.path.abspath(sys.argv[3])
    outputDir = os.path.join(os.path.abspath(sys.argv[4]), name)
    sifDir = os.path.abspath(sys.argv[5]) if len(sys.argv) > 5 else None
    db_backup = os.path.join(sourceDir, 'db-backup', name)
    db = os.path.join(sourceDir, 'db', name)

    zipf = [0.6, 0.9, 1.2]
    phases = threads*2-1

    ycsb = {
        'threadcount': threads,
        'sleepafterload': 0,
        'maxexecutiontime': maxexecutiontime,
        'operationcount': 1_000_000_000,
        'recordcount': 20_000_000,
        'request_key_domain_end': 19_999_999,
        'status.interval': 1,
        'readallfields': 'false',
        'fieldcount': 1,
        'fieldlength': 1000,
        'insertorder': 'nothashed',
        'requestdistribution.0': 'uniform',
        'requestdistribution': 'zipfian',
        **{f'zipfian_const.{i}': zipf[i] for i in range(threads)},
        **{f'sleepafterload.{i}': int(i*(maxexecutiontime/phases)) for i in range(threads)},
        **{f'maxexecutiontime.{i}': int((1 - i*2/phases)*maxexecutiontime) for i in range(threads)},
        'cachelib.size': 2_000_000_000*threads,
        'cachelib.name': 'instance-0',
        'cachelib.eviction': 'lru',
        'cachelib.pool.relsize': 1/threads,
        **{f'cachelib.pool.name.{i}': f'p{i}' for i in range(threads)},
        **{f'request_key_prefix.{i}': f'p{i}' for i in range(threads)},
        # rocksdb
        'rocksdb.compression': 'no',
        'rocksdb.write_buffer_size': 134217728,
        'rocksdb.max_write_buffer_number': 2,
        'rocksdb.level0_file_number_compaction_trigger': 4,
        'rocksdb.max_background_flushes': 1,
        'rocksdb.max_background_compactions': 3,
        'rocksdb.use_direct_reads': 'true',
        'rocksdb.no_block_cache': 'true',
        'rocksdb.use_direct_io_for_flush_compaction': 'true',
        'rocksdb.dbname': db,
        # workload
        'workload.type': 'synthetic',
        'readproportion': 1,
        'updateproportion':0,
        'scanproportion':0,
        'insertproportion':0,
    }

    load = {
        **ycsb,
        'rocksdb.dbname': db_backup,
        'rocksdb.destroy': 'true',
    }

    setups = {
        'CacheLib-Optimizer': {
            **ycsb,
            'cachelib.eviction': '2q',
            'cachelib.pooloptimizer': 'on',
            'cachelib.poolresizer': 'on',
        },
        'CacheLib': {
            **ycsb,
            'cachelib.pool_optimizer': 'off',
        }
    }

    RunYCSB(name, sourceDir, outputDir, load, setups, runs, status, sifDir)
