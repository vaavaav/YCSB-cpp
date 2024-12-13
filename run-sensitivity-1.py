#!/usr/bin/env python3

import os
import signal
import subprocess
import shutil
from datetime import datetime
import json
from pathlib import Path

# Configs

workspace = os.getcwd()

project_source_dir = f'{workspace}'
executable_dir = f'{workspace}/build'
scripts_dir = '.'
util_script = f'{scripts_dir}/utils.sh'

results_dir = f'{workspace}/profiling_{datetime.now().strftime("%m-%d-%H-%M-%S")}'
db = f'{project_source_dir}/db'
db_backup = f'{project_source_dir}/db_backup'

workloads_dir = f'{workspace}/workloads'
workload = 'read-only'
threads = 4
runs = 3
status = 'READ READ-PASSED ALL'

# Benchmark settings
ycsb = {
    'sleepafterload' : 0,
    'threadcount': threads,
    'maxexecutiontime' : 350,
    'operationcount' : 1_000_000_000,
    'recordcount': 20_000_000,
    'cachelib.cachesize': 2_000_000_000, # in bytes
    'status.interval': 1,
    'readallfields': 'false',
    'fieldcount' : 1,
    'fieldlength': 1000,
    'cachelib.pool_resizer' : 'on',
    'cachelib.tail_hits_tracking': 'on',
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
    'requestdistribution': 'uniform',
    'cachelib.trail_hits_tracking': 'off',
    'requestdistribution.0' : 'uniform',
    'requestdistribution.1' : 'zipfian',
    'zipfian_const.1' : '0.7',
    'requestdistribution.2' : 'zipfian',
    'zipfian_const.2' : '0.9',
    'requestdistribution.3' : 'zipfian',
    'zipfian_const.3' : '1.2',
    'sleepafterload.0' : 0,
    'maxexecutiontime.0' : 350,
    'sleepafterload.1' : 50,
    'maxexecutiontime.1' : 50,
    'sleepafterload.2': 150,
    'maxexecutiontime.2': 50,
    'sleepafterload.3': 250,
    'maxexecutiontime.3': 50,
}

def keyRangePerThread(threads):
    result = {}
    fractionSize = int(ycsb['recordcount'] / threads)
    for worker in range(threads):
        result[f'insertstart.{worker}'] = worker * fractionSize
        result[f'request_key_domain_start.{worker}'] = worker * fractionSize
        result[f'request_key_domain_end.{worker}'] = fractionSize * (worker + 1) - 1
    return result

setups = {
    'CacheLib-Holpaca (T=20s)' : {
        'name': 'T: 20s',
        'controllerArgs': '20000 hit_ratio_maximization 0.05',
        'resultsDir': f'cachelib_holpaca_20000',
        'overrideConfigs' : {
            'cachelib.holpaca': 'on',
        }
    },
    'CacheLib-Holpaca (T=10s)' : {
        'name': 'T: 10s',
        'controllerArgs': '10000 hit_ratio_maximization 0.05',
        'resultsDir': f'cachelib_holpaca_10000',
        'overrideConfigs' : {
            'cachelib.holpaca': 'on',
        }
    },
    'CacheLib-Holpaca (T=1s)' : {
        'name': 'T: 1s',
        'controllerArgs': '1000 hit_ratio_maximization 0.05',
        'resultsDir': f'cachelib_holpaca_1000',
        'overrideConfigs' : {
            'cachelib.holpaca': 'on',
        }
    },
    'CacheLib-Holpaca (T=0.1s)' : {
        'name': 'T: 100ms',
        'controllerArgs': '100 hit_ratio_maximization 0.05',
        'resultsDir': f'cachelib_holpaca_100',
        'overrideConfigs' : {
            'cachelib.holpaca': 'on',
        }
    },
    'CacheLib-Holpaca (T=0.01s)' : {
        'name': 'T: 10ms',
        'controllerArgs': '10 hit_ratio_maximization 0.05',
        'resultsDir': f'cachelib_holpaca_10',
        'overrideConfigs' : {
            'cachelib.holpaca': 'on',
        }
    },
    'CacheLib-Holpaca (T=0.001s)' : {
        'name': 'T: 1ms',
        'controllerArgs': '1 hit_ratio_maximization 0.05',
        'resultsDir': f'cachelib_holpaca_1',
        'overrideConfigs' : {
            'cachelib.holpaca': 'on',
        }
    },
}

def getPhases(ycsbConfig, threads):
    return [ycsbConfig[f'sleepafterload.{i}'] for i in range(threads)] + [ycsbConfig[f'sleepafterload.{i}'] + ycsbConfig[f'maxexecutiontime.{i}'] for i in range(threads)]

ycsb = {**ycsb, **keyRangePerThread(threads)}

config = {
    'ycsb': ycsb,
    'runs': runs,
    'setups': setups,
    'load': True,
    'threads': threads,
    'load_config': { 
        **ycsb,
        'rocksdb.dbname': db_backup,
        'rocksdb.destroy': 'true',
        'threadcount': 4
    },
    'workloads': workload,
    'phases': getPhases(ycsb, threads)
}

def loadDB(workload, settings):
    command = f'{executable_dir}/ycsb -load -db cachelib -P {workloads_dir}/{workload} -threads {settings["threadcount"]} {" ".join([f"-p {k}={v}" for k,v in settings.items()])}'
    print(f'[LOAD] Running: {command}')
    subprocess.run(command, shell=True)
    print('[LOAD] Done')

def runBenchmark(workload, settings, output_file, outputDir):
    print('\tLoading db backup')
    if os.path.exists(db):
        shutil.rmtree(db)
    shutil.copytree(db_backup, db)
    ycsb_settings = {**ycsb, **settings['overrideConfigs']}
    command = f'systemd-run --scope -p MemoryMax={ycsb_settings["cachelib.cachesize"]*1.1} --user {executable_dir}/ycsb -run -db cachelib -P {workloads_dir}/{workload} -s {status} -threads {threads} {" ".join([f"-p {k}={v}" for k,v in ycsb_settings.items()])}'
    print('\tCleaning heap')
    subprocess.call([util_script, 'clean-heap'], stdout=subprocess.DEVNULL)
    controller_pid = None
    if 'controllerArgs' in settings:
        print('\tStarting controller')
        controller_pid = subprocess.Popen(f"{project_source_dir}/../build-holpaca/bin/controller {settings['controllerArgs']}", stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid).pid
    print(f'\tRunning: {command}')
    dstat_pid = subprocess.Popen(f'dstat -rcdgmn > {outputDir}/dstat.csv', shell=True, preexec_fn=os.setsid).pid
    subprocess.run(command, shell=True, text=True, stdout=output_file)
    os.killpg(os.getpgid(dstat_pid), signal.SIGTERM)
    if controller_pid is not None:
        print('\tKilling controller')
        os.killpg(os.getpgid(controller_pid), signal.SIGTERM)

os.mkdir(results_dir)
report = open(f'{results_dir}/config.json', 'wt')
report.write(json.dumps(config, indent=2))
report.close()

os.system('killall -9 controller') # Kill all controller instances to avoid coordination issues
if config['load']:
    loadDB(workload, config['load_config'])
for setup, settings in setups.items():
    for run in range(runs):
        print(f'[WORKLOAD: {workload}] [SETUP: {setup}] [RUN: {run+1}/{runs}] ')
        dir = f'{results_dir}/{settings["resultsDir"]}/{run}'
        os.makedirs(dir, exist_ok=True)
        settings['overrideConfigs']['cachelib.tracker'] = f'{dir}/mem.txt'
        with open(f'{dir}/ycsb.txt', 'w') as f:
            runBenchmark(workload, settings, f, dir)
            f.close()
