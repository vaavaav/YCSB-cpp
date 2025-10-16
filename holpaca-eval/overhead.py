#!/usr/bin/env python3

import copy
import os
import shutil
import subprocess
import sys
import time

from RunYCSB import Case, Load, RunYCSB, Setup

runs = 1


def getRecordCount(threads):
    if threads == 1:
        return 32_000_000
    elif threads == 2:
        return 16_000_000
    elif threads == 4:
        return 8_000_000
    elif threads == 8:
        return 4_000_000
    elif threads == 16:
        return 2_000_000
    elif threads == 32:
        return 1_000_000
    elif threads == 64:
        return 500_000


def getOperationCount(threads, distribution):
    if distribution == "zipfian":
        if threads == 1:
            return 12_000_000
        elif threads == 2:
            return 5_000_000
        elif threads == 4:
            return 2_000_000
        elif threads == 8:
            return 1_500_000
        elif threads == 16:
            return 700_000
        elif threads == 32:
            return 500_000
        elif threads == 64:
            return 300_000
    else:  # uniform
        if threads == 1:
            return 7_000_000
        elif threads == 2:
            return 5_600_000
        elif threads == 4:
            return 2_800_000
        elif threads == 8:
            return 1_400_000
        elif threads == 16:
            return 700_000
        elif threads == 32:
            return 350_000
        elif threads == 64:
            return 175_000


def baseConfig(name, threads):
    return {
        "threadcount": threads,
        "sleepafterload": 0,
        "recordcount": getRecordCount(threads),
        **{f"request_key_prefix.{i}": f"p{i}" for i in range(threads)},
        "request_key_domain_end": getRecordCount(threads) - 1,
        "status.interval": 1,
        "readallfields": "false",
        "fieldcount": 1,
        "fieldlength": 1000,
        "insertorder": "nothashed",
        # rocksdb
        "rocksdb.compression": "no",
        "rocksdb.write_buffer_size": 134217728,
        "rocksdb.max_write_buffer_number": 2,
        "rocksdb.level0_file_number_compaction_trigger": 4,
        "rocksdb.max_background_flushes": 1,
        "rocksdb.max_background_compactions": 3,
        "rocksdb.use_direct_reads": "true",
        "rocksdb.no_block_cache": "true",
        "rocksdb.use_direct_io_for_flush_compaction": "true",
        "rocksdb.dbname": os.path.join(sourceDir, "db", name),
    }


def instanceConfig(threads):
    return {
        "cachelib.size": getRecordCount(threads) * 100,
        **{f"cachelib.name.{i}": f"instance-{i}" for i in range(threads)},
        "cachelib.pool.relsize": 1,
        "cachelib.pool.name": "p0",
    }


def tenantConfig(threads):
    return {
        "cachelib.size": getRecordCount(threads) * 100 * threads,
        "cachelib.name": "instance-0",
        "cachelib.pool.relsize": 1 / threads,
        **{f"cachelib.pool.name.{i}": f"p{i}" for i in range(threads)},
    }


def loadConfig(name, base_config, sourceDir):
    return Load(
        ycsb_executable,
        {
            **base_config,
            "rocksdb.dbname": os.path.join(sourceDir, "db-backup", name),
            "rocksdb.destroy": "true",
        },
        status="INSERT",
    )


def zipfianConfig(threads):
    return {
        "operationcount": getOperationCount(threads, "zipfian"),
        "requestdistribution": "zipfian",
        "zipfian_const": 0.9,
    }


def uniformConfig(threads):
    return {
        "operationcount": getOperationCount(threads, "uniform"),
        "requestdistribution": "uniform",
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


# TODO: multiplas instancias


if __name__ == "__main__":
    sourceDir = os.path.abspath(sys.argv[1])
    outputDir = os.path.join(os.path.abspath(sys.argv[2]))
    sifPath = os.path.abspath(sys.argv[3]) if len(sys.argv) > 3 else None
    ycsb_executable = os.path.join(sourceDir, "build-ycsb/ycsb")  # Update this path

    optimized = Setup(
        "optimized",
        ycsb_executable,
        {
            "cachelib.eviction": "2q",
            "cachelib.pooloptimizer": "on",
            "cachelib.poolresizer": "on",
            "cachelib.poolrebalancer": "off",
        },
    )

    holpaca = Setup(
        "holpaca",
        ycsb_executable,
        {
            "cachelib.eviction": "2q",
            "cachelib.pooloptimizer": "off",
            "cachelib.poolresizer": "on",
            "cachelib.poolrebalancer": "off",
        },
        controller_exec=os.path.join(
            sourceDir, "opt/ycsb/bin/cachelib_holpaca_controller"
        ),
        controller_args="MarginalHits 1000",
    )

    holpaca_cce = Setup(
        "holpaca-cce",
        ycsb_executable,
        {
            "cachelib.eviction": "2q",
            "cachelib.pooloptimizer": "off",
            "cachelib.poolresizer": "on",
            "cachelib.poolrebalancer": "off",
        },
        controller_exec=os.path.join(
            sourceDir, "opt/ycsb/bin/cachelib_holpaca_controller"
        ),
        controller_args="MarginalHits 1:100000",
    )

    cases = []
    for workload_name, workload in [
        ("readonly", readonlyConfig),
        ("mixed", mixedConfig),
        ("writeheavy", writeheavyConfig),
    ]:
        for dist_name, dist in [("zipfian", zipfianConfig), ("uniform", uniformConfig)]:
            for typ_name, typ in [
                ("instance", instanceConfig),
                ("tenant", tenantConfig),
            ]:
                for threads in [1, 2, 4, 8, 16, 32, 64]:
                    setups = copy.deepcopy([optimized, holpaca, holpaca_cce])
                    name = f"{workload_name}-{dist_name}-{typ_name}-{threads}"
                    config = baseConfig(name, threads)
                    load = loadConfig(
                        name,
                        {**config, **typ(threads), **workload(), **dist(threads)},
                        sourceDir,
                    )
                    for setup in setups:
                        setup.config = {
                            **config,
                            **typ(threads),
                            **workload(),
                            **dist(threads),
                            **setup.config,
                        }
                    cases.append(
                        Case(
                            name,
                            runs,
                            load,
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
