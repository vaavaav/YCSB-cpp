#!/usr/bin/env python3

import os
import shutil
import subprocess
import sys
import time

from RunYCSB import Case, Load, RunYCSB, Setup

runs = 1

if __name__ == "__main__":
    threads = int(sys.argv[1])
    maxexecutiontime = int(sys.argv[2])
    sourceDir = os.path.abspath(sys.argv[3])
    outputDir = os.path.join(os.path.abspath(sys.argv[4]))
    sifPath = os.path.abspath(sys.argv[5]) if len(sys.argv) > 5 else None

    # Assuming YCSB executable path - adjust as needed
    ycsb_executable = os.path.join(sourceDir, "build-ycsb/ycsb")  # Update this path

    phases = threads * 2 - 1

    # Base YCSB configuration
    ycsb_config_motivation_1 = {
        "threadcount": threads,
        "cleanupafterload": "true",
        "sleepafterload": 0,
        "maxexecutiontime": maxexecutiontime,
        "operationcount": 1_000_000_000,
        "recordcount": 20_000_000,
        "request_key_domain_end": 19_999_999,
        "status.interval": 1,
        "readallfields": "false",
        "fieldcount": 1,
        "fieldlength": 1000,
        "insertorder": "nothashed",
        "requestdistribution": "zipfian",
        "zipfian_const.0": 1.2,
        "zipfian_const.1": 0.9,
        "zipfian_const.2": 0.6,
        "requestdistribution.3": "uniform",
        "sleepafterload": 0,
        "maxexecutiontime": maxexecutiontime,
        "cachelib.size": 2_000_000_000 * threads,
        "cachelib.name": "instance-0",
        "cachelib.pool.relsize": 1 / threads,
        **{f"cachelib.pool.name.{i}": f"p{i}" for i in range(threads)},
        **{f"request_key_prefix.{i}": f"p{i}" for i in range(threads)},
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
        "rocksdb.dbname": os.path.join(sourceDir, "db", "motivation-1"),
        # workload
        "workload.type": "synthetic",
        "readproportion": 1,
        "updateproportion": 0,
        "scanproportion": 0,
        "insertproportion": 0,
    }

    # Load configuration (for database initialization)
    load_setup_motivation_1 = Load(
        ycsb_executable,
        {
            **ycsb_config_motivation_1,
            "rocksdb.dbname": os.path.join(sourceDir, "db-backup", "motivation-1"),
            "rocksdb.destroy": "true",
        },
    )

    # Create Setup objects for different configurations
    setups_motivation_1 = [
        #       Setup(
        #           "baseline",
        #           "cachelib-lru",
        #           ycsb_executable,
        #           ycsb_config_motivation_1,
        #       ),
        Setup(
            "custom",
            "cachelib-holpaca",
            ycsb_executable,
            {
                **ycsb_config_motivation_1,
                "cachelib.pool.proportion.0": 0.91,
                "cachelib.pool.proportion.1": 0.03,
                "cachelib.pool.proportion.2": 0.03,
                "cachelib.pool.proportion.3": 0.03,
                "operationcount.0": 452_566_125,
                "operationcount.1": 37_095_511,
                "operationcount.2": 19_523_431,
                "operationcount.3": 17_148_591,
            },
            controller_exec=os.path.join(
                sourceDir, "opt/ycsb/bin/cachelib_holpaca_controller"
            ),
            controller_args="Motivation 1000",
        ),
        Setup(
            "optimizer-enabled",
            "cachelib-lru2q",
            ycsb_executable,
            {
                **ycsb_config_motivation_1,
                "cachelib.pooloptimizer": "on",
                "cachelib.poolresizer": "on",
                "operationcount.0": 452_566_125,
                "operationcount.1": 37_095_511,
                "operationcount.2": 19_523_431,
                "operationcount.3": 17_148_591,
            },
        ),
    ]

    ycsb_config_motivation_2 = {
        **ycsb_config_motivation_1,
        **{
            f"sleepafterload.{i}": int(i * (maxexecutiontime / phases))
            for i in range(threads)
        },
        **{
            f"maxexecutiontime.{i}": int((1 - i * 2 / phases) * maxexecutiontime)
            for i in range(threads)
        },
    }

    ycsb_config_motivation_2 = {
        **ycsb_config_motivation_1,
        "cachelib.size": 2_000_000_000,
        "cachelib.pool.relsize": 1,
        **{f"cachelib.name.{i}": f"instance-{i}" for i in range(threads)},
        **{
            f"sleepafterload.{i}": int(i * (maxexecutiontime / phases))
            for i in range(threads)
        },
        **{
            f"maxexecutiontime.{i}": int((1 - i * 2 / phases) * maxexecutiontime)
            for i in range(threads)
        },
    }

    load_setup_motivation_2 = Load(
        ycsb_executable,
        {
            **ycsb_config_motivation_2,
            "rocksdb.dbname": os.path.join(sourceDir, "db-backup", "motivation-3"),
            "rocksdb.destroy": "true",
        },
    )

    setups_motivation_2 = [
        Setup(
            "optimizer-enabled",
            "cachelib-lru2q",
            ycsb_executable,
            {
                **ycsb_config_motivation_2,
                "cachelib.pooloptimizer": "on",
                "cachelib.poolresizer": "on",
                "cachelib.poolrebalancer": "off",
                "operationcount.0": 452_566_125,
                "operationcount.1": 37_095_511,
                "operationcount.2": 19_523_431,
                "operationcount.3": 17_148_591,
            },
        ),
        Setup(
            "custom",
            "cachelib-holpaca",
            ycsb_executable,
            {
                **ycsb_config_motivation_2,
                "cachelib.size": 10_000_000_000,
                "cachelib.virtualsize": 2_000_000_000,
                "cachelib.poolresizer": "on",
                "cachelib.poolresizer.milliseconds": 1000,
                "cachelib.poolresizer.slabs": 1000,
                "cachelib.pool.noinitialsize": "on",
                "cachelib.proportion.0": 0.91,
                "cachelib.pool.proportion.0": 1.0,
                "cachelib.proportion.1": 0.03,
                "cachelib.pool.proportion.1": 1.0,
                "cachelib.proportion.2": 0.03,
                "cachelib.pool.proportion.2": 1.0,
                "cachelib.proportion.3": 0.03,
                "cachelib.pool.proportion.3": 1.0,
                "operationcount.0": 452_566_125,
                "operationcount.1": 37_095_511,
                "operationcount.2": 19_523_431,
                "operationcount.3": 17_148_591,
            },
            controller_exec=os.path.join(
                sourceDir, "opt/ycsb/bin/cachelib_holpaca_controller"
            ),
            controller_args="Motivation 1000",
        ),
    ]

    ## Cases
    cases = [
        Case(
            "motivation-1",
            runs,
            load_setup_motivation_1,
            setups_motivation_1,
            "READ-PASSED READ-FAILED ALL",
        ),
        Case(
            "motivation-2",
            runs,
            load_setup_motivation_3,
            setups_motivation_2,
            "READ-PASSED READ-FAILED ALL",
        ),
    ]

    # Run the benchmark
    RunYCSB(cases, outputDir, sif_path=sifPath, binds=[sourceDir], timeout="01:30:00")
