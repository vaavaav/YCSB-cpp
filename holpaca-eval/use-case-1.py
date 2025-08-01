#!/usr/bin/env python3

from RunYCSB import RunYCSB, Load, Setup, Case

import subprocess
import shutil
import sys
import os
import time

runs = 1
status = 'READ-FAILED READ-PASSED INSERT-FAILED INSERT-PASSED UPDATE-FAILED UPDATE-PASSED ALL'

if __name__ == '__main__':
    sourceDir = os.path.abspath(sys.argv[1])
    maxexecutiontime = int(sys.argv[2])
    # traces as the third argument, separated by colon
    loadTraces = sys.argv[3].split(':')
    traces = sys.argv[4].split(':')
    tracesDir = os.path.abspath(sys.argv[5])
    outputDir = os.path.abspath(sys.argv[6])
    sifPath = os.path.abspath(sys.argv[7]) if len(sys.argv) > 7 else None
    db_backup = os.path.join(sourceDir, 'db-backup', 'use-case-1')
    db = os.path.join(sourceDir, 'db', 'use-case-1')

    ycsb_executable = os.path.join(sourceDir, 'build-ycsb/ycsb')  # Update this path
    threads = len(traces)
    phases = threads * 2 - 1

    ycsb_config = {
        'threadcount': threads,
        'maxexecutiontime': 60*60, 
        'status.interval': 1,
        'operationcount.0': 5_000_000,
        'operationcount.1': 5_000_000,
        'operationcount.2': 36_000_000,
        'operationcount.3': 33_000_000,
        'sleepafterload': 0,
        # **{f'sleepafterload.{i}': int(i*(maxexecutiontime/phases)) for i in range(threads)},
        # Cachelib
        'cachelib.size': 2_500_000_000,
        'cachelib.virtualsize': 2_500_000_000,
        'cachelib.name': 'instance-0',
        'cachelib.eviction': 'lru',
        'cachelib.pool.relsize.0': 0.609,
        'cachelib.pool.relsize.1': 0.235,
        'cachelib.pool.relsize.2': 0.089,
        'cachelib.pool.relsize.3': 0.067,
        **{f'request_key_prefix.{i}': f'p{i}' for i in range(threads)},
        **{f'cachelib.pool.name.{i}': f'p{i}' for i in range(threads)},
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
        'trace.override_value_size': 1000,
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
        Setup('optimized', ycsb_executable, {
            **ycsb_config,
            'cachelib.eviction': '2q',
            'cachelib.pooloptimizer': 'on',
            'cachelib.poolresizer': 'on',
            #'cachelib.poolrebalancer': 'on',
        }),
        Setup('baseline', ycsb_executable, {
            **ycsb_config,
            'cachelib.pooloptimizer': 'off',  
            'cachelib.poolresizer': 'off',
        }),
        Setup('holpaca', ycsb_executable, {
            **ycsb_config,
            'cachelib.poolresizer': 'on',
            'cachelib.pool.noinitialsize': 'on',
            #'cachelib.poolrebalancer': 'on',
            }, controller_exec=os.path.join(sourceDir, 'opt/ycsb/bin/cachelib_holpaca_controller'),
              controller_args=f'ThroughputMaximization 1000:0.05:{ycsb_config["cachelib.virtualsize"]}:false:false'),

        Setup('holpaca A', ycsb_executable, {
            **ycsb_config,
            'cachelib.poolresizer': 'on',
            'cachelib.pool.noinitialsize': 'on',
            #'cachelib.poolrebalancer': 'on',
            }, controller_exec=os.path.join(sourceDir, 'opt/ycsb/bin/cachelib_holpaca_controller'),
              controller_args=f'ThroughputMaximization 1000:0.05:{ycsb_config["cachelib.virtualsize"]}:false:true'),
    ]

    cases = [
            Case('use-case-1', runs, load_setup, setups, "READ-PASSED READ-FAILED READ INSERT-PASSED INSERT-FAILED UPDATE-PASSED UPDATE-FAILED ALL")
            ]

    # Run the benchmark
    RunYCSB(cases, outputDir, timeout='01:00:00', sif_path=sifPath, binds=[sourceDir])

