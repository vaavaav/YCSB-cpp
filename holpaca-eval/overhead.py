#!/usr/bin/env python3

import copy
import os
import shutil
import subprocess
import sys
import time

from RunYCSBOverhead import loadPhase

runs = 1
KEYS = 2_000_000
ITEMSIZE = 1_000


def baseConfig(name, threads):
    return {
        "threadcount": threads,
        "sleepafterload": 0,
        "recordcount": KEYS * threads,
        **{f"request_key_domain_start.{i}": KEYS * i for i in range(threads)},
        **{f"request_key_domain_end.{i}": KEYS * (i + 1) - 1 for i in range(threads)},
        "status.interval": 1,
        "readallfields": "false",
        "fieldcount": 1,
        "fieldlength": ITEMSIZE,
        "insertorder": "nothashed",
        "requestdistribution": "uniform",
    }


def configTenants(name, threads=1):
    return {
        "cachelib.size": KEYS * ITEMSIZE * threads,
        "cachelib.name": "instance-0",
        "cachelib.pool.relsize": 1 / threads,
        **{f"cachelib.pool.name.{i}": f"p{i}" for i in range(threads)},
    }


def configInstances(name, threads=1):
    return {
        "cachelib.size": KEYS * ITEMSIZE,
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
        "updateproportion": 0,
        "scanproportion": 0,
        "insertproportion": 0.5,
    }


def writeheavyConfig():
    return {
        "workload.type": "synthetic",
        "readproportion": 0.1,
        "updateproportion": 0,
        "scanproportion": 0,
        "insertproportion": 0.9,
    }


if __name__ == "__main__":
    sourceDir = os.path.abspath(sys.argv[1])
    outputDir = os.path.join(os.path.abspath(sys.argv[2]))
    sifPath = os.path.abspath(sys.argv[3]) if len(sys.argv) > 3 else None
    ycsb_executable = os.path.join(sourceDir, "build-ycsb/ycsb")  # Update this path

    loadPhase(
        "overhead",
        ycsb_executable,
        loadConfig("overhead"),
        sifPath,
        status="INSERT",
        sifBinds=[sourceDir],
        rehearse=True,
    )

    baseline = Setup(
        "baseline",
        ycsb_executable,
        {
            "operationcount": 1_000_000_000,
            "cachelib.eviction": "lru",
            "maxexecutiontime": 600,
            "cachelib.poolresizer": "on",
            "cachelib.poolresizer.milliseconds": 1000,
            "cachelib.poolresizer.slabs": 1000,
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
        },
        controller_exec=os.path.join(
            sourceDir, "opt/ycsb/bin/cachelib_holpaca_controller"
        ),
        controller_args="ThroughputMaximization 1000:0.01",
    )

    holpaca_cce = Setup(
        "holpaca-cce",
        ycsb_executable,
        {
            "cachelib.eviction": "lru",
            "cachelib.poolresizer": "on",
            "cachelib.poolresizer.milliseconds": 1000,
            "cachelib.poolresizer.slabs": 1000,
        },
        controller_exec=os.path.join(
            sourceDir, "opt/ycsb/bin/cachelib_holpaca_controller"
        ),
        controller_args="ThroughputMaximization 1:0.01:1000000",
    )

    cases = []
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
                setups = copy.deepcopy([baseline])
                name = f"{workload_name}-{setupTypeName}-{threads}"
                for setup in setups:
                    setup.config = {
                        **baseConfig(name, threads),
                        **setupType(name, threads),
                        **workload(),
                        **setup.config,
                    }
                cases.append(
                    Case(
                        name,
                        runs,
                        setups,
                        "READ-PASSED READ-FAILED UPDATE-PASSED UPDATE-FAILED INSERT-PASSED INSERT-FAILED",
                    )
                )

    # Run the benchmark
    RunYCSB(
        cases,
        outputDir,
        sif_path=sifPath,
        binds=[sourceDir],
        timeout="0:40:00",
        dry_run=True,
    )
