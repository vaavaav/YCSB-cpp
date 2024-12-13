import os
from typing import List, Dict
from typing_extensions import Self


class YCSBConfig:
    def __init__(self, executable: str, threads: int):
        self._executable = executable
        self._threads = threads
        self._config = {}
        self._config["threadcount"] = threads
        self._db = ""
        self._status_to_print = []
        self._workload = ""
        self._load = False
    
    def copy(self) -> Self:
        new = YCSBConfig(self._executable, self._threads)
        new._config = self._config.copy()
        new._load = self._load
        new._db = self._db
        new._status_to_print = self._status_to_print.copy()
        new._workload = self._workload
        return new 

    def setDelayAfterLoad(self, delay_after_load: List[int]) -> Self:
        assert (len (delay_after_load) == self._threads), 'Delay after load must be equal to the number of threads'
        assert all([d >= 0 for d in delay_after_load]), 'Delay after load must be greater than or equal to 0'
        for i,d in enumerate(delay_after_load):
            self._config[f"sleepafterload.{i}"] = d
        return self
    
    def setMaxExecutionTime(self, max_execution_time: List[int]) -> Self:
        assert (len (max_execution_time) == self._threads), 'Max execution time must be equal to the number of threads'
        assert all([m > 0 for m in max_execution_time]), 'Max execution time must be greater than 0'
        for i,m in enumerate(max_execution_time):
            self._config[f"maxexecutiontime.{i}"] = m
        return self
    
    def setNumberOfOperation(self, operation_count: int) -> Self:
        assert operation_count > 0, 'Operation count must be greater than 0'
        self._config["operationcount"] = operation_count
        return self
    
    def setRecordCount(self, record_count: int, key_distribution: List[float]) -> Self:
        assert record_count > 0, 'Record count must be greater than 0'
        assert (len (key_distribution) == self._threads), 'Key distribution must be equal to the number of threads'
        assert sum(key_distribution) == 1, 'Sum of key distribution must be equal to 1'
        self._config["recordcount"] = record_count
        accum = 0
        for i in range(self._threads):
            self._config[f"request_key_domain_start.{i}"] = accum
            self._config[f"insertstart.{i}"] = accum
            accum += int(key_distribution[i] * record_count)
            self._config[f"request_key_domain_end.{i}"] = accum - 1
        return self
    
    def setStatusinterval(self, status_interval: int) -> Self:
        assert status_interval > 0, 'Status interval must be greater than 0'
        self._config["status.interval"] = status_interval
        return self
        
    def setRequestDistribution(self, request_distribution: List[str]) -> Self: 
        assert (len (request_distribution) == self._threads), 'Request distribution must be equal to the number of threads'
        assert all([d in ["uniform", "zipfian"] for d in request_distribution]), 'Request distribution may be one of the following: uniform, zipfian'
        for i,d in enumerate(request_distribution):
            self._config[f"requestdistribution.{i}"] = d
        return self
    
    def setZipfianCoefficients(self, zipfian_coefficient: List[float]) -> Self:
        assert (len (zipfian_coefficient) == self._threads), 'Zipfian coefficient must be equal to the number of threads'
        assert all([0 <= c for c in zipfian_coefficient]), 'Zipfian coefficient must be greater than 0'
        for i,c in enumerate(zipfian_coefficient):
            self._config[f"zipfian_const.{i}"] = c
        return self
    
    def setWorkload(self, workload: str) -> Self:
        assert os.path.exists(f'{workload}'), f'Workload {workload} does not exist'
        self._workload = workload
        return self
    
    def setStatusToPrint(self, status: List[str]) -> Self:
        assert all([s in ["", "INSERT", "READ", "UPDATE", "SCAN", "READMODIFYWRITE", "DELETE", "INSERT-PASSED", "READ-PASSED", "UPDATE-PASSED", "SCAN-PASSED", "READMODIFYWRITE-PASSED", "DELETE-PASSED", "INSERT-FAILED", "READ-FAILED", "UPDATE-FAILED", "SCAN-FAILED", "READMODIFYWRITE-FAILED", "DELETE-FAILED", "ALL"] for s in status]), 'status to print may be none or some of the following: INSERT, READ, UPDATE, SCAN, READMODIFYWRITE, DELETE, INSERT-PASSED, READ-PASSED, UPDATE-PASSED, SCAN-PASSED, READMODIFYWRITE-PASSED, DELETE-PASSED, INSERT-FAILED, READ-FAILED, UPDATE-FAILED, SCAN-FAILED, READMODIFYWRITE-FAILED, DELETE-FAILED, ALL'
        self._status_to_print = status
        return self
    
    def setLoad(self, load: bool = False) -> Self:
        self._load = load
        return self
    
    def enableCacheLibHolpaca(self, hosts: List[str]) -> Self:
        assert len(hosts) == self._threads
        self._db = "cachelib-holpaca"
        for i,h in enumerate(hosts):
            self._config[f"holpaca.host.{i}"] = h
        return self

    def enableCacheLib(self, size: int, pools: Dict[str, float], poolResizer : bool, poolOptimizer : bool) -> Self:
        assert len(pools) == self._threads and sum(pools.values()) == 1
        self._db = "cachelib"
        self._config["cachelib.size"] = size
        self._config["cachelib.poolresizer"] = "on" if poolResizer else "off"
        self._config["cachelib.pooloptimizer"] = "on" if poolOptimizer else "off"
        for i,p in enumerate(pools.items()):
            self._config[f"cachelib.pool.name.{i}"] = p[0]
            self._config[f"cachelib.pool.relsize.{i}"] = p[1]
        return self
    
    def setObjectSize(self, object_size: int) -> Self:
        self._config["fieldlength"] = object_size
        self._config["readallfields"] = "false"
        self._config["fieldcount"] = 1
        return self
    
    def setRocksDB(self, db: str, destroy: bool) -> Self:
        self._config["rocksdb.dbname"] = db
        self._config["rocksdb.destroy"] = "true" if destroy else "false"
        self._config["rocksdb.write_buffer_size"] = 134217728
        self._config["rocksdb.max_write_buffer_number"] = 2
        self._config["rocksdb.level0_file_number_compaction_trigger"] = 4
        self._config["rocksdb.compression"] = "no"
        self._config["rocksdb.max_background_flushes"] = 1
        self._config["rocksdb.max_background_compactions"] = 3
        self._config["rocksdb.use_direct_reads"] = "true"
        self._config["rocksdb.use_direct_io_for_flush_compaction"] = "true"
        return self
    
    def toCommand(self) -> str:
        return f'{self._executable} {"-load" if self._load else "-run" } -db {self._db} -P {self._workload} -threads {self._threads} { "-s " + " ".join(self._status_to_print) if len(self._status_to_print) > 0 else ""} {" ".join([f"-p {k}={v}" for k,v in self._config.items()])}' 

class Setup:
    def __init__(self, name: str, output: str, setup_script: str, cleanup_script: str, workoads, ycsb: YCSBConfig, runs: int):
        self.name = name
        self.output = output
        self.setup_script = setup_script
        self.cleanup_script = cleanup_script
        self.ycsb = ycsb
        self.runs = runs
        self.workloads = workoads
