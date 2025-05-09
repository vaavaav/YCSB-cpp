#!/user/bin/env python3

import os
from datetime import datetime

runs = 3
outputDir = f"motivation-1/{datetime.now().strftime('%m-%d-%H-%M-%S')}"
db = os.path('/tmp/db')
dbBackup = os.path('/tmp/db-backup')
workloads = ['read-only']

ycsb = {
        'sleepafterload': 0,
        'threadcount': 2,
        'maxexecutiontime': 500,
        'operationcount': 1_000_000_000,
        'recordcount': 20_000_000,
        'status.interval': 1,
        'readallfields': 'false',
        'fieldcount': 1,
        'fieldlength': 1000,
        'rocksdb.dbname': db,
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
        'request_key_prefix.0': 'p0',
        'request_key_prefix.1': 'p1',
        'cachelib.cachesize': 2_000_000_000,
        'cachelib.pool.relsize.0': 0.5,
        'cachelib.pool.relsize.1': 0.5,
        'cachelib.pool.name.0': 'p0',
        'cachelib.pool.name.1': 'p1',
        }

load = {
        **ycsb,
        'rocksdb.dbname': dbBackup,
        'rocksdb.destroy': 'true',
        }

setups = {
        'CacheLib-Optimizer': {
            'title': 'CacheLib-Optimizer',
            'resultsDir': f'{outputDir}/cachelib_optimizer',
            'overrideConfigs': {
                'cachelib.pooloptimizer': 'on',
                'cachelib.poolresizer': 'on',
            }
        },
        'CacheLib': {
            'title': 'CacheLib',
            'resultsDir': f'{outputDir}/cachelib',
            'overrideConfigs': {
                'cachelib.pool_optimizer': 'off',
                }
            }
        }

if __name__ == '__main__':
    executableDir = os.path.dirname(os.path.abspath(executable))
    workloadsDir = os.path.abspath(sys.argv[1])
    outputDir = os.path.abspath(sys.argv[2])
    executable = os.path.join(executableDir, 'ycsb')
    for workload in workloads:
        # Load
        subprocess.run(f'{executable} -load -db cachelib-holpaca -P {workloadsDir}/{workload} -threads {ycsb["threadcount"]} {" ".join([f"-p {k}={v}" for k,v in load.items()])}', shell=True, stdout=subprocess.DEVNULL)
        for setup, config in setups.items():
            for run in range(runs):
                # restore db
                if os.path.exists(db):
                    shutil.rmtree(db)
                shutil.copytree(dbBackup, db)
                print("Cleaning heap")
                subprocess.call([util_script, 'clean-heap'], stdout=subprocess.DEVNULL)
                # create output dir
                dir = f'{outputDir}/{config["resultsDir"]}/{workload}/{run+1}'
                os.makedirs(dir, exist_ok=True)
                with open(f'{dir}/ycsb.txt', 'w') as outputFile:
                    # run ycsb
                    command = f'systemd-run --scope -p MemoryMax={ycsb["cachelib.cachesize"]*1.2} --user {executable} -run -db cachelib-holpaca -P {workloadsDir}/{workload} -s {" ".join([f"-p {k}={v}" for k,v in config.items()])}'
                    print(f'[RUN] Running: {command}')
                    subprocess.run(command, shell=True, text=True, stdout=outputFile)
