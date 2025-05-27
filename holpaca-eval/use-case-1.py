#!/usr/bin/env python3

from RunYCSB import RunYCSB

import subprocess
import shutil
import sys
import os
import time

name = f"use-case-1-{int(time.time()*1e9)}"
runs = 3
status = 'READ-FAILED READ-PASSED INSERT-FAILED INSERT-PASSED UPDATE-FAILED UPDATE-PASSED ALL'

if __name__ == '__main__':
    sourceDir = os.path.abspath(sys.argv[1])
    # traces as the second argument, separated by colon
    loadTraces = sys.argv[2].split(':')
    traces = sys.argv[3].split(':')
    tracesDir = os.path.abspath(sys.argv[4])
    outputDir = os.path.join(os.path.abspath(sys.argv[5]), name)
    sifDir = os.path.abspath(sys.argv[6]) if len(sys.argv) > 6 else None
    db_backup = os.path.join(sourceDir, 'db-backup', name)
    db = os.path.join(sourceDir, 'db', name)

    ycsb = {
        'threadcount': len(traces),
        'status.interval': 1,
        #tmp
        'maxexecutiontime': 1800,
        # Cachelib
        'cachelib.size': 2_000_000_000*len(traces),
        'cachelib.name': 'instance-0',
        'cachelib.eviction': 'lru',
        'cachelib.pool.relsize': 1 / len(traces),
        **{f'request_key_prefix.{i}': f'p{i}' for i in range(len(traces))},
        **{f'cachelib.pool.name.{i}': f'p{i}' for i in range(len(traces))},
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
        'workload.type': 'trace',
        **{f'trace.file.{i}': os.path.join(tracesDir, trace) for i, trace in enumerate(traces)},
    }

    load = {
        **ycsb,
        'rocksdb.dbname': db_backup,
        'rocksdb.destroy': 'true',
        **{f'trace.file.{i}': os.path.join(tracesDir, loadTrace) for i, loadTrace in enumerate(loadTraces)},
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
        },
        'CacheLib-Holpaca': {
            **ycsb,
            'cachelib.controller.address': 'localhost:11111',
            'cachelib.holpaca.address': 'localhost:22222',
            }
    }

    RunYCSB(name, sourceDir, outputDir, load, setups, runs, status, sifDir)




