#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import time

from RunYCSB import Case, Load, RunYCSB, Setup

runs = 1
status = "READ-FAILED READ-PASSED INSERT-FAILED INSERT-PASSED UPDATE-FAILED UPDATE-PASSED ALL"

if __name__ == "__main__":
    sourceDir = os.path.abspath(sys.argv[1])
    maxexecutiontime = int(sys.argv[2])
    # traces as the third argument, separated by colon
    loadTraces = sys.argv[3].split(":")
    traces = sys.argv[4].split(":")
    tracesDir = os.path.abspath(sys.argv[5])
    outputDir = os.path.abspath(sys.argv[6])
    sifPath = os.path.abspath(sys.argv[7]) if len(sys.argv) > 7 else None
    db_backup = os.path.join(sourceDir, "db-backup", "use-case-2")
    db = os.path.join(sourceDir, "db", "use-case-2")

    ycsb_executable = os.path.join(sourceDir, "build-ycsb/ycsb")  # Update this path
    threads = len(traces)

    ycsb_config = {
        "threadcount": threads,
        "maxexecutiontime": maxexecutiontime,
        "operationcount": 1_000_000_000_000,
        "status.interval": 1,
        "sleepafterload": 0,
        # Cachelib
        "cachelib.size.0": int(4_500_000_000 * 0.048),
        "cachelib.size.1": int(4_500_000_000 * 0.078),
        "cachelib.size.2": int(4_500_000_000 * 0.249),
        "cachelib.size.3": int(4_500_000_000 * 0.625),
        **{f"cachelib.name.{i}": f"instance-{i}" for i in range(len(traces))},
        "cachelib.eviction": "lru",
        "cachelib.pool.relsize": 1,
        **{f"request_key_prefix.{i}": f"p{i}" for i in range(len(traces))},
        **{f"cachelib.pool.name.{i}": f"p{i}" for i in range(len(traces))},
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
        "rocksdb.dbname": db,
        # workload
        "workload.type": "trace",
        "trace.override_value_size": 1000,
        **{
            f"trace.file.{i}": os.path.join(tracesDir, trace)
            for i, trace in enumerate(traces)
        },
    }
    # Load configuration (for database initialization)
    load_setup = Load(
        ycsb_executable,
        {
            **ycsb_config,
            "rocksdb.dbname": db_backup,
            "rocksdb.destroy": "true",
            **{
                f"trace.file.{i}": os.path.join(tracesDir, trace)
                for i, trace in enumerate(loadTraces)
            },
        },
    )

    # Create Setup objects for different configurations
    setups = [
        # Setup(
        #     "baseline",
        #     ycsb_executable,
        #     {
        #         **ycsb_config,
        #         "cachelib.pooloptimizer": "off",
        #         "cachelib.poolresizer": "off",
        #     },
        # ),
        Setup(
            "optimized",
            ycsb_executable,
            {
                **ycsb_config,
                "cachelib.eviction": "2q",
                "cachelib.pooloptimizer": "on",
                "cachelib.poolresizer": "on",
                "cachelib.poolresizer.milliseconds": 1000,
                "cachelib.poolresizer.slabs": 1000,
                # based on baseline results
                "operationcount.0": 37412757,
                "operationcount.1": 42304570,
                "operationcount.2": 8095184,
                "operationcount.3": 6756425,
            },
        ),
        Setup(
            "holpaca",
            ycsb_executable,
            {
                **ycsb_config,
                "cachelib.poolresizer": "on",
                "cachelib.poolresizer.milliseconds": 1000,
                "cachelib.poolresizer.slabs": 1000,
                "cachelib.size.0": 20_000_000_000,  # 20 GB
                "cachelib.size.1": 20_000_000_000,
                "cachelib.size.2": 20_000_000_000,
                "cachelib.size.3": 20_000_000_000,
                "cachelib.virtualsize.0": int(4_500_000_000 * 0.048),
                "cachelib.virtualsize.1": int(4_500_000_000 * 0.078),
                "cachelib.virtualsize.2": int(4_500_000_000 * 0.249),
                "cachelib.virtualsize.3": int(4_500_000_000 * 0.625),
                "cachelib.pool.relsize.0": 4_500_000_000 * 0.048 / 20_000_000_000,
                "cachelib.pool.relsize.1": 4_500_000_000 * 0.078 / 20_000_000_000,
                "cachelib.pool.relsize.2": 4_500_000_000 * 0.249 / 20_000_000_000,
                "cachelib.pool.reslize.3": 4_500_000_000 * 0.625 / 20_000_000_000,
                # based on baseline results
                "operationcount.0": 37412757,
                "operationcount.1": 42304570,
                "operationcount.2": 8095184,
                "operationcount.3": 6756425,
            },
            controller_exec=os.path.join(
                sourceDir, "opt/ycsb/bin/cachelib_holpaca_controller"
            ),
            controller_args=f"ThroughputMaximization 1000:0.01",
        ),
    ]

    cases = [
        Case(
            "use-case-2",
            runs,
            load_setup,
            setups,
            "READ-PASSED READ-FAILED READ INSERT-PASSED INSERT-FAILED UPDATE-PASSED UPDATE-FAILED ALL",
        )
    ]

    # Run the benchmark
    RunYCSB(cases, outputDir, timeout="01:30:00", sif_path=sifPath, binds=[sourceDir])
