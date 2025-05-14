#!/usr/bin/env python3

from RunYCSB import RunYCSB

import subprocess
import shutil
import sys
import os
from datetime import datetime

if __name__ == '__main__':
    sourceDir = os.path.abspath(sys.argv[1])
    workloadsDir = os.path.abspath(sys.argv[2])
    outputDir = os.path.join(os.path.abspath(sys.argv[3]), f"{datetime.now().strftime('%m-%d-%H-%M-%S')}")
    sifDir = os.path.abspath(sys.argv[4]) if len(sys.argv) > 5 else None

    runs = 3
    workload_type = 'synthetic'
    workloads = [ f"{workloadsDir}/{x}" for x in ['read-only'] ]
    status = 'READ-FAILED READ-PASSED ALL'

    ycsb = {
        'threadcount': 2,
        'sleepafterload': 0,
        'workload.type': workload_type,
        'maxexecutiontime': 500,
        'operationcount': 1_000_000_000,
        'recordcount.0': 200_000_000,
        'recordcount.1': 200_000_000,
        'status.interval': 1,
        'readallfields': 'false',
        'fieldcount': 1,
        'fieldlength': 100,
        'rocksdb.write_buffer_size': 134217728,
        'rocksdb.max_write_buffer_number': 2,
        'rocksdb.level0_file_number_compaction_trigger': 4,
        'rocksdb.compression': 'no',
        'rocksdb.max_background_flushes': 1,
        'rocksdb.max_background_compactions': 3,
        'rocksdb.use_direct_reads': 'true',
        'rocksdb.use_direct_io_for_flush_compaction': 'true',
        'insertorder': 'nothashed',
        'requestdistribution.0': 'uniform',
        'requestdistribution.1': 'zipfian',
        'zipfian_const.1': '0.9',
        'sleepafterload.0': 0,
        'maxexecutiontime.0': 500,
        'sleepafterload.1': 125,
        'maxexecutiontime.1': 250,
        'cachelib.size': 4_000_000_000,
        'cachelib.size.0': 2_000_000_000,
        'cachelib.size.1': 2_000_000_000,
        'cachelib.name.0': 'instance-0',
        'cachelib.name.1': 'instance-1',
        'cachelib.eviction': 'lru',
        'cachelib.pool.relsize.0': 1,
        'cachelib.pool.relsize.1': 1,
        'request_key_prefix.0': 'p0',
        'request_key_prefix.1': 'p1',
        'cachelib.pool.name.0': 'p0',
        'cachelib.pool.name.1': 'p1',
    }

    load = {
        **ycsb,
        'rocksdb.destroy': 'true',
    }

    setups = {
        'CacheLib-Optimizer': {
            'title': 'CacheLib-Optimizer',
            'resultsDir': f'{outputDir}/cachelib_optimizer',
            'config' : {
                **ycsb,
                'cachelib.eviction': '2q',
                'cachelib.pooloptimizer': 'on',
                'cachelib.poolresizer': 'on',
            }
        },
        'CacheLib': {
            'title': 'CacheLib',
            'resultsDir': f'{outputDir}/cachelib',
            'config': {
                **ycsb,
                'cachelib.pool_optimizer': 'off',
            }
        }
    }

    RunYCSB(sourceDir, workloads, outputDir, load, setups, runs, sifDir, status)

