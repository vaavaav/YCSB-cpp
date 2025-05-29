import subprocess
import shutil
import os
from datetime import datetime
import json
import re
import time

def RunYCSB(name, runs, output_dir, load_setup, setups, status, sif_path=None, binds=[]):
    if sif_path:
        load_job_id = loadSIF(name, load_setup, sif_path, binds)
        if not load_job_id:
            print(f"[SIF] Failed to start load job for {name}.")
            return
        for setup in setups:
            for i in range(runs):
                out = os.path.join(output_dir, setup.name, str(i + 1))
                os.makedirs(out, exist_ok=True)
                runSIF(f"{name}-{setup.name}-{i+1}", setup, load_setup.config['rocksdb.dbname'], out, status, load_job_id, sif_path, binds)
    else:
        loadLOCAL(name, load_setup)
        for setup in setups:
            for i in range(runs):
                out = os.path.join(output_dir, setup.name, str(i + 1))
                os.makedirs(out, exist_ok=True)
                runLOCAL(f"{name}-{setup.name}-{i + 1}", setup, setup.config['rocksdb.dbname'], out, status)

class Load:
    def __init__(self, executable, config):
        self.executable = executable
        self.config = config
        self.threads = int(config.get('threadcount', 1))
        self.traces = [config.get(f'trace.file.{i}', config.get('trace.file')) for i in range(self.threads)]

    def build_cmd(self):
        return f"{self.executable} -load -db cachelib-holpaca {' '.join(f'-p {k}={v}' for k, v in self.config.items())}"


class Setup:
    def __init__(self, name, executable, config, controller_exec=None, controller_args=None):
        self.name = name
        self.config = config
        self.executable = executable
        self.total_cache_size = int(sum([
            int(config.get(f'cachelib.size.{i}', config.get('cachelib.size', 0))*config.get(f'cachelib.pool.relsize.{i}', config.get('cachelib.pool.relsize', 1)))
            for i in range(int(config.get('threadcount', 1)))
        ]) * 1.2) # 20% overhead
        self.controller_exec = controller_exec
        self.controller_ip = config.get('cachelib.controller.address', None)
        self.controller_args = controller_args or ""
        self.threads = int(config.get('threadcount', 1))
        self.traces = [config.get(f'trace.file.{i}', config.get('trace.file')) for i in range(int(config.get('threadcount', 1)))]

    def build_cmd(self, status=None):
        return f"{self.executable} -run -db cachelib-holpaca {f'-s {status}' if status else ''} {' '.join(f'-p {k}={v}' for k, v in self.config.items())}"


def build_sbatch_cmd(name, cmd, stdout, stderr, mem=None, jobid=None):
    cmd = [
        "sbatch",
        f"--job-name={name}",
        "--account=f202400014testdeucalionx",
        "--nodes=1",
        "--ntasks=1",
        "--cpus-per-task=1",
        "--partition=dev-x86",
        "--mail-type=END",
        "--mail-user=jose.p.peixoto@inesctec.pt",
        f"--output={stdout}",
        f"--error={stderr}",
        "--wrap", cmd
    ]
    if mem:
        cmd.insert(1, f"--mem={mem}M")
    if jobid:
        cmd.insert(1, f"--dependency=afterok:{jobid}")
    return cmd

# LOCAL 
def loadLOCAL(name, setup: Load): 
    print(f"[LOCAL] Loading for {name}: {setup.build_cmd()}")
    db = setup.config['rocksdb.dbname']
    shutil.rmtree(db, ignore_errors=True)
    os.makedirs(db, exist_ok=True)
    subprocess.run(setup.build_cmd(), shell=True)

def runLOCAL(name, setup: Setup, db_backup, outdir, status):
    db = setup.config['rocksdb.dbname']
    shutil.rmtree(db, ignore_errors=True)
    shutil.copytree(db_backup, db) # Restore the database from backup
    print(f"[LOCAL] Running {name}: {setup.build_cmd(status)}")
    controller = None
    with open(os.path.join(outdir, 'dstat.csv'), 'w') as dstat_output:
        dstat = subprocess.Popen(["dstat", "-cdlmnyt"], stdout=dstat_output)
        if setup.controller_exec:
            controller = subprocess.Popen([setup.controller_exec, setup.controller_ip, *setup.controller_args.split()], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(os.path.join(outdir, 'ycsb.txt'), 'w') as ycsb_output:
            subprocess.run(setup.build_cmd(status), shell=True, stdout=ycsb_output)
        if controller:
            controller.terminate()
        dstat.terminate()

# SLURM

# Returns the job ID of the load job
def loadSIF(name, setup: Load, sif_path=None, binds=[]):
    if not os.path.exists(sif_path):
        raise FileNotFoundError(f"SIF file not found: {sif_path}")
    # ---
    db = setup.config['rocksdb.dbname']
    fake_db = "/tmp/db"
    if os.path.exists(db):
        print(f"[SIF] Removing existing database at {db}")
        shutil.rmtree(db, ignore_errors=True)
    print(f"[SIF] Creating database directory at {db}")
    os.makedirs(db, exist_ok=True)
    setup.config['rocksdb.dbname'] = fake_db
    # ---
    copy_workloads_cmd = "" 
    for thread,tracefile in enumerate(setup.traces):
        if tracefile:
            setup.config[f"trace.file.{thread}"] = f"/tmp/{os.path.basename(tracefile)}"
            copy_workloads_cmd += f"cp '{tracefile}' /tmp; "
    # ---
    load_cmd = setup.build_cmd()
    load_job_id = None
    executable_dir = os.path.dirname(setup.executable)
    wrapped = f'''
        mkdir -p {fake_db}
        {copy_workloads_cmd}
        singularity run --bind "{executable_dir},/tmp,{",".join(binds)}" {sif_path} {load_cmd}
        cp {fake_db}/* {db}/
    '''
    print(f"[SIF] Submitting load job for {name}")
    result = subprocess.run(build_sbatch_cmd(
        name=f"load-{name}",
        cmd=wrapped,
        stdout=f"/tmp/slurm-load-{name}.out",
        stderr=f"/tmp/slurm-load-{name}.err"
    ),
     stdout=subprocess.PIPE, 
     stderr=subprocess.PIPE, 
     universal_newlines=True)

    if result.returncode == 0:
        for line in result.stdout.strip().splitlines():
            if line.startswith("Submitted batch job"):
                load_job_id = line.split()[-1]

    setup.config['rocksdb.dbname'] = db  # Restore the original database path in the setup config

    return load_job_id

def runSIF(name, setup: Setup, db_backup, outdir, status, load_job_id, sif_path=None, binds=[]):
    if not os.path.exists(sif_path):
        raise FileNotFoundError(f"SIF file not found: {sif_path}")
    # ---
    controller_job_id = None
    if setup.controller_exec:
        controller_dir = os.path.dirname(setup.controller_exec)
        inner = f"""dstat -cdlmnyt > {outdir}/controller_dstat.csv 2>&1 & \
        {setup.controller_exec} $(hostname -I | awk '{{print $1}}' | xargs):11110 {setup.controller_args}"""

        wrapped = f'singularity run --bind "{controller_dir},{outdir},{",".join(binds)}" {sif_path} bash -c "{inner}"'
        print(f"[SIF] Submitting controller job for {setup.name}")
        result = subprocess.run(build_sbatch_cmd(
            name=f"controller-{name}",
            cmd=wrapped,
            stdout=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-controller-{name}.out",
            stderr=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-controller-{name}.err",
            jobid=load_job_id
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True)

        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                if line.startswith("Submitted batch job"):
                    controller_job_id = line.split()[-1]

        if controller_job_id is None:
            print(f"[SIF] Failed to start controller job for {setup.name}.")
            return
    # ---
    copy_workloads_cmd = "" 
    for thread,tracefile in enumerate(setup.traces):
        if tracefile:
            setup.config[f"trace.file.{thread}"] = f"/tmp/{os.path.basename(tracefile)}"
            copy_workloads_cmd += f"cp '{tracefile}' /tmp; "
    # ---
    db = "/tmp/db"
    setup.config['rocksdb.dbname'] = db
    executable_dir = os.path.dirname(setup.executable)
    local_dstat_output = "/tmp/dstat.csv"
    local_ycsb_output = "/tmp/ycsb.txt"
    dstat_output = os.path.join(outdir, 'dstat.csv')
    ycsb_output = os.path.join(outdir, 'ycsb.txt')
    # remove holpaca.address if it exists
    override_ips = "IPS=''"
    if controller_job_id:
        override_ips = f"""\
CONTROLLER_COMPUTE_NODE=""
while [[ -z "$CONTROLLER_COMPUTE_NODE" ]]; do
    output=$(squeue -j "{controller_job_id}" -o "%N" --noheader 2>/dev/null | xargs)
    if [[ "$output" =~ cx([0-9]+) ]]; then
        CONTROLLER_COMPUTE_NODE="${{BASH_REMATCH[1]}}"
    fi
    sleep 1
done

IPS=" -p cachelib.controller.address=10.12.1.$CONTROLLER_COMPUTE_NODE:11110"

IP=$(hostname -I | awk '{{print $1}}' | xargs)
for i in $(seq 1 {setup.threads}); do
    PORT=$((11110 + i))
    IPS+=" -p cachelib.holpaca.address.$i=$IP:$PORT"
done

echo "$IPS"
"""
    wrapped = f'''
        {override_ips}
        mkdir -p {db} && cp -r {db_backup}/* {db}/
        {copy_workloads_cmd}
        singularity run --bind "{executable_dir},/tmp,{",".join(binds)}" {sif_path} bash -c "\
            dstat -cdlmnyt > {local_dstat_output} 2>&1 & \
            {setup.build_cmd(status)} $IPS > {local_ycsb_output} 2>&1; \
            kill $(pgrep dstat)"
        {f"scancel {controller_job_id}" if controller_job_id else ""}
        cp {local_ycsb_output} {ycsb_output}
        cp {local_dstat_output} {dstat_output}
    '''

    print(f"[SIF] Submitting run job for {name}, setup: {setup.name}")
    subprocess.run(build_sbatch_cmd(
        name=name,
        cmd=wrapped,
        stdout=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-{name}.out",
        stderr=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-{name}.err",
        mem=int(setup.total_cache_size / (1024 * 1024)),
        jobid=load_job_id
    ))

