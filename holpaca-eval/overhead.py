#!/usr/bin/env python3

import copy
import os
import shutil
import subprocess
import sys
import time

from RunYCSBOverhead import Setup

KEYS = 2_000_000
ITEMSIZE = 1_000
OVERHEAD_MARGIN = 1.2


def baseConfig(threads):
    return {
        "threadcount": threads,
        "operationcount": 100_000_000_000,
        "maxexecutiontime": 600,  # 10 minutes
        "sleepafterload": 0,
        "recordcount": KEYS,
        **{f"request_key_prefix.{i}": f"p{i}" for i in range(threads)},
        "status.interval": 1,
        "readallfields": "false",
        "fieldcount": 1,
        "fieldlength": ITEMSIZE,
        "insertorder": "nothashed",
        "requestdistribution": "uniform",
    }


def configTenants(threads=1):
    return {
        "cachelib.size": KEYS * ITEMSIZE * threads * OVERHEAD_MARGIN,
        "cachelib.name": "instance-0",
        "cachelib.pool.relsize": 1 / threads,
        **{f"cachelib.pool.name.{i}": f"p{i}" for i in range(threads)},
    }


def configInstances(threads=1):
    return {
        "cachelib.size": KEYS * ITEMSIZE * OVERHEAD_MARGIN,
        **{f"cachelib.name.{i}": f"instance-{i}" for i in range(threads)},
        "cachelib.pool.relsize": 1,
        "cachelib.pool.name": "p0",
    }


def readonlyConfig():
    return {
        "workload.type": "synthetic",
        "readproportion": 1,
        "updateproportion": 0,
        "scanproportion": 0,
        "insertproportion": 0,
    }


def mixedConfig():
    return {
        "workload.type": "synthetic",
        "readproportion": 0.5,
        "updateproportion": 0.5,
        "scanproportion": 0,
        "insertproportion": 0,
    }


def writeheavyConfig():
    return {
        "workload.type": "synthetic",
        "readproportion": 0.1,
        "updateproportion": 0.9,
        "scanproportion": 0,
        "insertproportion": 0,
    }


if __name__ == "__main__":
    sourceDir = os.path.abspath(sys.argv[1])
    outputDir = os.path.join(os.path.abspath(sys.argv[2]))
    sifPath = os.path.abspath(sys.argv[3]) if len(sys.argv) > 3 else None
    ycsb_executable = os.path.join(sourceDir, "build-ycsb/ycsb")  # Update this path
    runs = int(sys.argv[4]) if len(sys.argv) > 4 else 1

    baseline = Setup(
        "baseline",
        ycsb_executable,
        {
            "cachelib.eviction": "lru",
            "cachelib.poolresizer": "on",
            "cachelib.poolresizer.milliseconds": 1000,
            "cachelib.poolresizer.slabs": 1000,
            "cachelib.type": "baseline",
        },
    )

    holpaca = Setup(
        "holpaca",
        ycsb_executable,
        {
            "cachelib.eviction": "lru",
            "cachelib.poolresizer": "on",
            "cachelib.poolresizer.milliseconds": 1000,
            "cachelib.poolresizer.slabs": 1000,
            "cachelib.type": "holpaca",
        },
        with_controller=True,
        controller_exec=os.path.join(
            sourceDir, "opt/ycsb/bin/cachelib_holpaca_controller"
        ),
        controller_args="ThroughputMaximization 1000:0.01:true",
    )

    holpaca_cce = Setup(
        "holpaca-cce",
        ycsb_executable,
        {
            "cachelib.eviction": "lru",
            "cachelib.poolresizer": "on",
            "cachelib.poolresizer.milliseconds": 1000,
            "cachelib.poolresizer.slabs": 1000,
            "cachelib.type": "holpaca",
        },
        with_controller=True,
        controller_exec=os.path.join(
            sourceDir, "opt/ycsb/bin/cachelib_holpaca_controller"
        ),
        controller_args="ThroughputMaximization 1:0.01:true:1000",
    )

    setups = []
    for workload_name, workload in [
        ("readonly", readonlyConfig),
        ("mixed", mixedConfig),
        ("writeheavy", writeheavyConfig),
    ]:
        for setupTypeName, setupType in [
            ("tenants", configTenants),
            ("instances", configInstances),
        ]:
            for threads in [1, 2, 4, 8, 16, 32, 64]:
                for setupName, setup in [
                    ("baseline", baseline),
                    # ("holpaca", holpaca),
                    # ("holpaca-cce", holpaca_cce),
                    # ("holpaca-freq", holpaca),
                ]:
                    for run in range(runs):
                        case = f"{workload_name}-{setupTypeName}-{threads}"
                        setup = copy.deepcopy(setup)
                        setup.status = (
                            "READ-PASSED READ-FAILED UPDATE-PASSED UPDATE-FAILED ALL"
                        )
                        setup.threads = threads
                        setup.config = {
                            **baseConfig(threads),
                            **setupType(threads),
                            **workload(),
                            **setup.config,
                        }
                        if setupName == "holpaca-freq":
                            for freq in [1, 10, 100, 1000, 10000]:
                                setup.name = f"{case}-{setupName}-{freq}-{run + 1}"
                                setup.controller_args = (
                                    f"ThroughputMaximization {freq}:0.01:true"
                                )
                                setup.out = os.path.join(
                                    outputDir, case, setupName, str(freq), str(run + 1)
                                )
                                setup.run(sifPath, binds=[sourceDir])
                        else:
                            setup.name = f"{case}-{setupName}-{run + 1}"
                            setup.out = os.path.join(
                                outputDir, case, setupName, str(run + 1)
                            )
                            setup.run(sifPath, binds=[sourceDir])
