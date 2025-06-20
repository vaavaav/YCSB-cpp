import subprocess
import shutil
import os
from datetime import datetime
import json
import re
import time

def RunYCSB(name, runs, output_dir, load_setup, setups, status, sif_path=None, binds=[]):
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, 'setups.json'), 'w') as f:
        json.dump({setup.name:setup.config for setup in setups}, f, indent=4)

    load_job_id = None
    if sif_path:
        load_job_id = loadSIF(name, load_setup, sif_path, binds)
        if not load_job_id:
            print(f"[SIF] Failed to start load job for {name}.")
            return
    else:
        loadLOCAL(name, load_setup)

    for setup in setups:
        for i in range(runs):
            out = os.path.join(output_dir, setup.name, str(i + 1))
            os.makedirs(out, exist_ok=True)

            if sif_path:
                if setup.controller_exec:
                    runSIFController(f"{name}-{setup.name}-{i + 1}", setup, load_setup.config['rocksdb.dbname'], out, status, load_job_id, sif_path, binds)
                else:
                    runSIF(f"{name}-{setup.name}-{i + 1}", setup, load_setup.config['rocksdb.dbname'], out, status, load_job_id, sif_path, binds)
            else:
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
        "--account=I20240005X",
        "--nodes=1",
        "--ntasks=1",
        "--cpus-per-task=1",
        "--partition=large-x86",
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
    wrapped = f'''
        mkdir -p {db} && cp -r {db_backup}/* {db}/
        {copy_workloads_cmd}
        singularity run --bind "{executable_dir},/tmp,{",".join(binds)}" {sif_path} bash -c "\
            dstat -cdlmnyt > {local_dstat_output} 2>&1 & \
            {setup.build_cmd(status)} > {local_ycsb_output} 2>&1; \
            kill $(pgrep dstat)"
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

def runSIFController(name, setup, db_backup, outdir, status, load_job_id, sif_path=None, binds=[]):
    if not os.path.exists(sif_path):
        raise FileNotFoundError(f"SIF file not found: {sif_path}")
    if not setup.controller_exec:
        raise ValueError("Controller executable not provided in setup.")

    controller_dir = os.path.dirname(setup.controller_exec)
    executable_dir = os.path.dirname(setup.executable)
    db = "/tmp/db"
    local_dstat_output = "/tmp/dstat.csv"
    local_ycsb_output = "/tmp/ycsb.txt"
    dstat_output = os.path.join(outdir, 'dstat.csv')
    ycsb_output = os.path.join(outdir, 'ycsb.txt')

    copy_workloads_cmd = ""
    for thread, tracefile in enumerate(setup.traces):
        if tracefile:
            setup.config[f"trace.file.{thread}"] = f"/tmp/{os.path.basename(tracefile)}"
            copy_workloads_cmd += f"cp '{tracefile}' /tmp; "

    build_cmd_str = setup.build_cmd(status)

    # === Controller job script ===
    controller_inner_script = f"""#!/bin/bash
set -e

CONTROLLER_IP=$(hostname -I | awk '{{print $1}}' | xargs)

# Start controller inside Singularity in background
singularity run --network host --bind "{controller_dir},{outdir},{','.join(binds)}" {sif_path} bash -c \\
"dstat -cdlmnyt > {outdir}/controller_dstat.csv 2>&1 & \\
{setup.controller_exec} $CONTROLLER_IP:11110 {setup.controller_args}" &

CONTROLLER_JOB_ID=$SLURM_JOB_ID

# Write client script
cat <<'EOF' > /tmp/client.sh
#!/bin/bash
set -e

CLIENT_IP=\$(hostname -I | awk '{{print $1}}' | xargs)
IPS=" -p cachelib.controller.address=$CONTROLLER_IP:11110"
for i in $(seq 0 {setup.threads - 1}); do
    PORT=$((11111 + i))
    IPS+=" -p cachelib.holpaca.address.$i=\$CLIENT_IP:$PORT"
done

mkdir -p {db} && cp -r {db_backup}/* {db}/
{copy_workloads_cmd}
singularity run --network host --bind "{executable_dir},/tmp,{','.join(binds)}" {sif_path} bash -c \\
"dstat -cdlmnyt > {local_dstat_output} 2>&1 & \\
{build_cmd_str} \$IPS > {local_ycsb_output} 2>&1; \\
kill \$(pgrep dstat)"
cp {local_ycsb_output} {ycsb_output}
cp {local_dstat_output} {dstat_output}

# Stop controller job
scancel {{"$CONTROLLER_JOB_ID"}}
EOF

chmod +x /tmp/client.sh

# Submit client job
{" ".join(build_sbatch_cmd(
        name=f"client-{name}",
        cmd="/tmp/client.sh",
        stdout=f"{outdir}/slurm-client-{name}.out",
        stderr=f"{outdir}/slurm-client-{name}.err",
        mem=int(setup.total_cache_size / (1024 * 1024))
))}

# Keep controller job alive until cancelled
sleep infinity
"""

    # Wrap script for sbatch
    wrapped_cmd = f"bash -c '{controller_inner_script.strip().replace(\"'\", \"'\\''\")}'"

    print(f"[SIF] Submitting combined controller+client job for {setup.name}")
    subprocess.run(build_sbatch_cmd(
        name=f"controller-{name}",
        cmd=wrapped_cmd,
        stdout=f"{outdir}/slurm-controller-{name}.out",
        stderr=f"{outdir}/slurm-controller-{name}.err",
        jobid=load_job_id
    ))
