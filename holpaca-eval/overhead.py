#!/usr/bin/env python3

import os
import shutil
import subprocess
import sys
import time

from RunYCSB import Case, Load, RunYCSB, Setup

runs = 1


def constructConfig(name):
    return {
        "sleepafterload": 0,
        "operationcount": 20_000_000,
        "recordcount": 20_000_000,
        "request_key_domain_end": 19_999_999,
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


def constructInstanceConfig(threads):
    return {
        "cachelib.size": 2_000_000_000,
        **{f"cachelib.name.{i}": "instance-{i}" for i in range(threads)},
        "cachelib.pool.relsize": 1,
        "cachelib.pool.name": "p0",
        **{f"request_key_prefix.{i}": f"p{i}" for i in range(threads)},
    }


def constructTenantConfig(threads):
    return {
        "cachelib.size": 2_000_000_000 * threads,
        "cachelib.name": "instance-0",
        "cachelib.pool.relsize": 1 / threads,
        **{f"cachelib.pool.name.{i}": f"p{i}" for i in range(threads)},
        **{f"request_key_prefix.{i}": f"p{i}" for i in range(threads)},
    }


def constructLoadConfig(name, base_config, sourceDir):
    return Load(
        ycsb_executable,
        {
            **base_config,
            "rocksdb.dbname": os.path.join(sourceDir, "db-backup", name),
            "rocksdb.destroy": "true",
        },
    )


# TODO: multiplas instancias


if __name__ == "__main__":
    sourceDir = os.path.abspath(sys.argv[1])
    outputDir = os.path.join(os.path.abspath(sys.argv[2]))
    sifPath = os.path.abspath(sys.argv[3]) if len(sys.argv) > 3 else None
    ycsb_executable = os.path.join(sourceDir, "build-ycsb/ycsb")  # Update this path

    # Assuming YCSB executable path - adjust as needed

    readonly = {
        "workload.type": "synthetic",
        "readproportion": 1,
        "updateproportion": 0,
        "scanproportion": 0,
        "insertproportion": 0,
    }

    mixed = {
        "workload.type": "synthetic",
        "readproportion": 0.5,
        "updateproportion": 0,
        "scanproportion": 0,
        "insertproportion": 0.5,
    }

    writeheavy = {
        "workload.type": "synthetic",
        "readproportion": 0.1,
        "updateproportion": 0,
        "scanproportion": 0,
        "insertproportion": 0.9,
    }

    zipfian = {
        "operationcount": 40_000_000,
        "requestdistribution": "zipfian",
        "zipfian_const": 0.9,
    }

    uniform = {
        "operationcount": 20_000_000,
        "requestdistribution": "uniform",
    }

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

    cases = []
    node = 357
    for workload_name, workload in [
        ("readonly", readonly),
        ("mixed", mixed),
        ("writeheavy", writeheavy),
    ]:
        for dist_name, dist in [("zipfian", zipfian), ("uniform", uniform)]:
            for typ_name, typ in [
                ("instance", constructInstanceConfig),
                ("tenant", constructTenantConfig),
            ]:
                for threads in [1, 2, 4, 8, 16, 32, 64]:
                    setups = [optimized, holpaca]
                    name = f"{workload_name}-{dist_name}-{typ_name}-{threads}-{node}"
                    for setup in setups:
                        setup.config = {
                            **constructConfig(name),
                            **typ(threads),
                            **workload,
                            **dist,
                            **setup.config,
                        }
                    load = constructLoadConfig(
                        name,
                        {
                            **constructConfig(name),
                            **workload,
                            **dist,
                        },
                        sourceDir,
                    )
                    cases.append(
                        Case(
                            name,
                            runs,
                            load,
                            setups,
                            "READ-PASSED READ-FAILED UPDATE-PASSED UPDATE-FAILED INSERT-PASSED INSERT-FAILED",
                            node=f'cnx{node}',
                            controller_node=f'cnx{node+1}'
                        )
                    )
                    node += 2

    # Run the benchmark
    RunYCSB(cases, outputDir, sif_path=sifPath, binds=[sourceDir], timeout="0:30:00", dry_run=True)
