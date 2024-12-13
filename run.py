#!/bin/python3
import os
import json
from datetime import datetime
from config import * 
import subprocess
import shutil
import signal

pwd = os.getcwd()
outputDir = f'profiling-{datetime.now().strftime("%m-%d-%H-%M-%S")}'
executable = 'build/ycsb'
cacheSize = 2_000_000
db = f'{pwd}/db'
dbBackup = f'{pwd}/db-backup'
workloadsDir = f'{pwd}/workloads'
workloads = ['read-only']

ycsbDefault = YCSBConfig(executable, 4).setDelayAfterLoad([0, 0, 0, 0]).setMaxExecutionTime([60, 60, 60, 60]).setNumberOfOperation(1_000_000).setRecordCount(2_000_000, [0.25, 0.25, 0.25, 0.25]).setStatusinterval(1).setRequestDistribution(['uniform', 'zipfian', 'zipfian', 'zipfian']).setZipfianCoefficients([0, 0.7, 0.9, 1.2]).setObjectSize(1000).setStatusToPrint(['READ', 'READ-PASSED']).setLoad(False).setRocksDB(db, False).enableCacheLib(2_000_000_000, {'1': 0.25, '2': 0.25, '3': 0.25, '4': 0.25}, False, False)


#.enableCacheLibHolpaca(['192.168.112.124:6000', '192.168.112.124:6001', '192.168.112.124:6002', '192.168.112.124:6003']).setRocksDB(db, False)

ycsbLoad = ycsbDefault.copy().setLoad(True).setRocksDB(dbBackup, True).setStatusToPrint([])
setups = [
    Setup('CacheLib', f'{outputDir}/cachelib', 'echo TODO', './utils.sh clean-heap', workloads, ycsbDefault.copy(), 1),
    Setup('CacheLib-Holpaca', f'{outputDir}/cachelib-holpaca', 'echo TODO', '/bin/echo TODO', workloads, ycsbDefault.copy(), 1)
]

def load(ycsb, workload):
    command = ycsb.toCommand()
    print(f'[LOAD] Workload: {workload}')
    print(f'[LOAD] Populating DB')
    subprocess.run(command, shell=True, stdout=subprocess.DEVNULL)
    print('[LOAD] Done')

def run(setup, workload):
    for r in range(setup.runs):
        outputDir = f"{setup.output}/{workload}/{r+1}"
        os.makedirs(outputDir, exist_ok=True)
        print(f'[RUN ({setup.name}) ({r+1}/{setup.runs})] Loading DB')
        if os.path.exists(db):
            shutil.rmtree(db)
        shutil.copytree(dbBackup, db)
        command = f'systemd-run --scope -p MemoryMax={cacheSize*1.1} --user {setup.ycsb.toCommand()}'
        print(f'[RUN ({setup.name}) ({r+1}/{setup.runs})] Setting up')
        subprocess.run(setup.setup_script, shell=True, stdout=subprocess.DEVNULL)
        with open(f'{outputDir}/ycsb.txt', 'w') as outputFile:
            print(f'[RUN ({setup.name}) ({r+1}/{setup.runs})] Running')
            dstat = subprocess.Popen(f'dstat -rcdgmn > {outputDir}/dstat.csv', shell=True, preexec_fn=os.setsid, stdout=subprocess.DEVNULL).pid
            subprocess.run(command, shell=True, text=True, stdout=outputFile)
            print(f'[RUN ({setup.name}) ({r+1}/{setup.runs})] Cleaning up')
            subprocess.run(setup.cleanup_script, shell=True, stdout=subprocess.DEVNULL)
            os.killpg(os.getpgid(dstat), signal.SIGTERM)
            outputFile.close()
        print(f'[RUN ({setup.name}) ({r+1}/{setup.runs})] Done')

os.makedirs(dbBackup, exist_ok=True)
for workload in workloads:
    ycsbLoad.setWorkload(f'{workloadsDir}/{workload}')
    load(ycsbLoad, workload)
    for setup in setups:
        setup.ycsb = setup.ycsb.setWorkload(f'{workloadsDir}/{workload}')
        run(setup, workload)


