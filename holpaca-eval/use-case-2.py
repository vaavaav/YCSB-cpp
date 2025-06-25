#!/usr/bin/env python3

from RunYCSB import RunYCSB, Load, Setup

import subprocess
import shutil
import sys
import os
import time

name = f"use-case-2-{int(time.time()*1e9)}"
runs = 1
status = 'READ-FAILED READ-PASSED INSERT-FAILED INSERT-PASSED UPDATE-FAILED UPDATE-PASSED ALL'

if __name__ == '__main__':
    sourceDir = os.path.abspath(sys.argv[1])
    maxexecutiontime = int(sys.argv[2])
    # traces as the third argument, separated by colon
    loadTraces = sys.argv[3].split(':')
    traces = sys.argv[4].split(':')
    tracesDir = os.path.abspath(sys.argv[5])
    outputDir = os.path.join(os.path.abspath(sys.argv[6]), name)
    sifPath = os.path.abspath(sys.argv[7]) if len(sys.argv) > 7 else None
    db_backup = os.path.join(sourceDir, 'db-backup', name)
    db = os.path.join(sourceDir, 'db', name)

    ycsb_executable = os.path.join(sourceDir, 'build-ycsb/ycsb')  # Update this path
    threads = len(traces)
    phases = threads * 2 - 1

    ycsb_config = {
        'threadcount': len(traces),
        'status.interval': 1,
        'operationcount.0': 35_300_000,
        'operationcount.1': 6_644_507,
        'operationcount.2': 14_265_238,
        'operationcount.3': 11_759_781,
        **{f'sleepafterload.{i}': int(i*(maxexecutiontime/phases)) for i in range(threads)},
        # Cachelib
        'cachelib.size': 8_000_000_000,
        'cachelib.virtualsize': 2_000_000_000,
        **{f'cachelib.name.{i}': f'instance-{i}' for i in range(len(traces))},
        'cachelib.eviction': 'lru',
        'cachelib.pool.relsize': 1, 
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
    # Load configuration (for database initialization)
    load_setup = Load(ycsb_executable, {
        **ycsb_config,
        'rocksdb.dbname': db_backup,
        'rocksdb.destroy': 'true',
        **{f'trace.file.{i}': os.path.join(tracesDir, trace) for i, trace in enumerate(loadTraces)},
    })
    
    # Create Setup objects for different configurations
    setups = [
        Setup('CacheLib-Optimizer', ycsb_executable, {
            **ycsb_config,
            'cachelib.eviction': '2q',
            'cachelib.pooloptimizer': 'on',
            'cachelib.poolresizer': 'on',
        }),
        Setup('CacheLib', ycsb_executable, {
            **ycsb_config,
            'cachelib.pooloptimizer': 'off',  
            'cachelib.poolresizer': 'off',
        }),
        Setup('CacheLib-Holpaca-HR', ycsb_executable, {
            **ycsb_config,
            'cachelib.poolresizer': 'on',
            'cachelib.pool.noinitialsize': 'on',
            }, controller_exec=os.path.join(sourceDir, 'opt/ycsb/bin/cachelib_holpaca_controller'),
              controller_args=f'HitRatioMaximization 1000:0.05:{ycsb_config["cachelib.virtualsize"]}:false'),
        Setup('CacheLib-Holpaca-T', ycsb_executable, {
            **ycsb_config,
            'cachelib.poolresizer': 'on',
            'cachelib.pool.noinitialsize': 'on',
            }, controller_exec=os.path.join(sourceDir, 'opt/ycsb/bin/cachelib_holpaca_controller'),
              controller_args=f'ThroughputMaximization 1000:0.05:{ycsb_config["cachelib.virtualsize"]}:false'),
    ]
    
    # Run the benchmark
    RunYCSB(name, runs, outputDir, load_setup, setups, "READ-PASSED READ-FAILED ALL", sifPath, binds=[sourceDir])

