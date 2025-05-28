#!/usr/bin/env python3

from RunYCSB import RunYCSB, Load, Setup
import subprocess
import shutil
import sys
import os
import time

name = f"motivation-2-{int(time.time()*1e9)}"
runs = 3

if __name__ == '__main__':
    threads = int(sys.argv[1])
    maxexecutiontime = int(sys.argv[2])
    sourceDir = os.path.abspath(sys.argv[3])
    outputDir = os.path.join(os.path.abspath(sys.argv[4]), name)
    sifPath = os.path.abspath(sys.argv[5]) if len(sys.argv) > 5 else None
    
    # Assuming YCSB executable path - adjust as needed
    ycsb_executable = os.path.join(sourceDir, 'build-ycsb/ycsb')  # Update this path
    
    db_backup = os.path.join(sourceDir, 'db-backup', name)
    db = os.path.join(sourceDir, 'db', name)
    zipf = [0.6, 0.9, 1.2]
    phases = threads*2-1
    
    # Base YCSB configuration
    ycsb_config = {
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
        **{f'zipfian_const.{i}': zipf[i-1] for i in range(1,threads)},
        **{f'sleepafterload.{i}': int(i*(maxexecutiontime/phases)) for i in range(threads)},
        **{f'maxexecutiontime.{i}': int((1 - i*2/phases)*maxexecutiontime) for i in range(threads)},
        **{f'cachelib.size.{i}': 2_000_000_000 for i in range(threads)},
        **{f'cachelib.name.{i}': f'instance-{i}' for i in range(threads)},
        'cachelib.eviction': 'lru',
        'cachelib.pool.relsize': 1,
        'cachelib.pool.name': 'p0',
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
        'updateproportion': 0,
        'scanproportion': 0,
        'insertproportion': 0,
    }
    
    # Load configuration (for database initialization)
    load_setup = Load(ycsb_executable, {
        **ycsb_config,
        'rocksdb.dbname': db_backup,
        'rocksdb.destroy': 'true',
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
            'cachelib.pooloptimizer': 'off',  # Fixed typo: was 'pool_optimizer'
            'cachelib.poolresizer': 'off',
        }),
        Setup('CacheLib-Holpaca-HR', ycsb_executable, {
            **ycsb_config,
            'cachelib.poolresizer': 'on',
            }, controller_exec=os.path.join(sourceDir, 'opt/ycsb/bin/cachelib_holpaca_controller'),
              controller_args='HitRatioMaximization 1000:0.05'),
        Setup('CacheLib-Holpaca-T', ycsb_executable, {
            **ycsb_config,
            'cachelib.poolresizer': 'on',
            }, controller_exec=os.path.join(sourceDir, 'opt/ycsb/bin/cachelib_holpaca_controller'),
              controller_args='ThroughputMaximization 1000:0.05')
    ]
    
    # Run the benchmark
    RunYCSB(name, runs, outputDir, load_setup, setups, "READ-PASSED READ-FAILED ALL", sifPath, binds=[sourceDir])

