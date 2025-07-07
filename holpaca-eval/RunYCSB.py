import subprocess
import shutil
import os
from datetime import datetime
import json
import re
import time

getMem = lambda cache_size: int(cache_size * 1.4 / (1024 * 1024))

sanitize = lambda name: re.sub(r'\W+', '_', name)

DRY_RUN = False
TIMEOUT = None

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
        self.used_mem = getMem(sum([
            int(config.get(f'cachelib.size.{i}', config.get('cachelib.size', 0))*config.get(f'cachelib.pool.relsize.{i}', config.get('cachelib.pool.relsize', 1)))
            for i in range(int(config.get('threadcount', 1)))
        ]))
        self.controller_exec = controller_exec
        self.controller_ip = config.get('cachelib.controller.address', None)
        self.controller_args = controller_args or ""
        self.threads = int(config.get('threadcount', 1))
        self.traces = [config.get(f'trace.file.{i}', config.get('trace.file')) for i in range(int(config.get('threadcount', 1)))]

    def build_cmd(self, status=None):
        return f"{self.executable} -run -db cachelib-holpaca {f'-s {status}' if status else ''} {' '.join(f'-p {k}={v}' for k, v in self.config.items())}"

class Case:
    def __init__(self, name, runs, load, setups, status):
        self.name = name
        self.runs = runs
        self.load = load
        self.setups = setups
        self.status = status

def build_sbatch_cmd(name, cmd, stdout, stderr, mem=None, jobid=None, export=None, ntasks=1):
    sbatch_cmd = [
        "sbatch",
        f"--job-name={name}",
        "--account=I20240005X",
        "--nodes=1",
        f"--ntasks={ntasks}",
        "--cpus-per-task=1",
        "--partition=large-x86",
        "--mail-type=END",
        "--mail-user=jose.p.peixoto@inesctec.pt",
        f"--output={stdout}",
        f"--error={stderr}",
        "--wrap", 
        cmd
    ]
    if mem:
        sbatch_cmd.insert(1, f"--mem={mem}M")
    if jobid:
        sbatch_cmd.insert(1, f"--dependency=afterok:{jobid}")
    if export:
        sbatch_cmd.insert(1, f"--export={export}")
    global TIMEOUT
    if TIMEOUT:
        sbatch_cmd.insert(1, f"--time={TIMEOUT}")

    return sbatch_cmd

def RunYCSB(cases, output_dir, sif_path=None, binds=[], colocated=False, dry_run=False, timeout=None):
    global DRY_RUN, TIMEOUT 
    DRY_RUN = dry_run
    TIMEOUT = timeout
    if not DRY_RUN:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    # If colocated, run all cases in a single node
    if colocated:
        colocate(cases, output_dir, sif_path=sif_path, binds=binds)
        return

    # Otherwise, run each case separately
    for case in cases:
        # Create a directory for each case
        case_dir = os.path.join(output_dir, sanitize(case.name))

        if not DRY_RUN:
            os.makedirs(case_dir, exist_ok=True)

            # Save the case configuration to a JSON file
            with open(os.path.join(case_dir, 'setups.json'), 'w') as f:
                json.dump({s.name: s.config for s in case.setups}, f, indent=2)

        load_job_id = None
        if case.load:
            if sif_path:
                load_job_id = loadSIF(case.name, case.load, sif_path, binds)
            else:
                loadLOCAL(case.name, case.load)

        db_backup = case.load.config['rocksdb.dbname']

        for setup in case.setups:
            for run_idx in range(case.runs):
                run_name = f"{case.name}-{setup.name}-{run_idx + 1}"
                out = os.path.join(case_dir, setup.name, str(run_idx + 1))
                if not DRY_RUN:
                    os.makedirs(out, exist_ok=True)

                if sif_path:
                    if setup.controller_exec:
                        runSIFController(run_name, setup, db_backup, out, case.status, load_job_id, sif_path, binds)
                    else:
                        runSIF(run_name, setup, db_backup, out, case.status, load_job_id, sif_path, binds)
                else:
                    runLOCAL(run_name, setup, db_backup, out, case.status)


# Local run function to execute a YCSB workload locally
def loadLOCAL(name, setup: Load): 
    if DRY_RUN:
        print(f"[DRY-RUN] [LOCAL] rm -r {setup.config['rocksdb.dbname']}")
        print(f"[DRY-RUN] [LOCAL] mkdir -p {setup.config['rocksdb.dbname']}")
        print(f"[DRY-RUN] [LOCAL] {setup.build_cmd()}")
        return

    db = setup.config['rocksdb.dbname']
    # Remove existing database if it exists
    shutil.rmtree(db, ignore_errors=True)
    # Create the database directory
    os.makedirs(db, exist_ok=True)
    # Run the load command
    subprocess.run(setup.build_cmd(), shell=True)

# Run function to execute a YCSB workload locally
def runLOCAL(name, setup: Setup, db_backup, outdir, status):
    if DRY_RUN:
        print(f"[DRY-RUN] [LOCAL] rm -r {setup.config['rocksdb.dbname']}")
        print(f"[DRY-RUN] [LOCAL] cp -r {db_backup} {setup.config['rocksdb.dbname']}")
        print(f"[DRY-RUN] [LOCAL] dstat -cdlmnyt > {os.path.join(outdir, 'dstat.csv')}")
        if setup.controller_exec:
            print(f"[DRY-RUN] [LOCAL] {setup.controller_exec} {setup.controller_ip} {setup.controller_args} &> {os.path.join(outdir, 'controller.log')}")
        print(f"[DRY-RUN] [LOCAL] {setup.build_cmd(status)} > {os.path.join(outdir, 'ycsb.txt')}")
        if setup.controller_exec:
            print(f"[DRY-RUN] [LOCAL] kill $(pgrep {setup.controller_exec})")
        print(f"[DRY-RUN] [LOCAL] kill $(pgrep dstat)")
    return

    db = setup.config['rocksdb.dbname']
    # Remove existing database if it exists
    shutil.rmtree(db, ignore_errors=True)
    # Restore the database from backup
    shutil.copytree(db_backup, db) 
    controller = None
    with open(os.path.join(outdir, 'dstat.csv'), 'w') as dstat_output:
        # Start dstat to collect system statistics
        dstat = subprocess.Popen(["dstat", "-cdlmnyt"], stdout=dstat_output)
        if setup.controller_exec:
            # Start the controller if specified
            controller = subprocess.Popen([setup.controller_exec, setup.controller_ip, *setup.controller_args.split()], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(os.path.join(outdir, 'ycsb.txt'), 'w') as ycsb_output:
            # Run the YCSB command
            subprocess.run(setup.build_cmd(status), shell=True, stdout=ycsb_output)
        if controller:
            controller.terminate()
        dstat.terminate()


# LoadSIF function to initialize the database for a YCSB workload using Singularity
def loadSIF(name, setup: Load, sif_path=None, binds=[]):
        if not os.path.exists(sif_path):
            raise FileNotFoundError(f"SIF file not found: {sif_path}")
        db = setup.config['rocksdb.dbname']
        # Use local storage for loading
        fake_db = "/tmp/db"
        # Redirect the database path to the fake location
        setup.config['rocksdb.dbname'] = fake_db
        # List commands to copy workload files to the local storage
        copy_workloads_cmd = []
        for thread,tracefile in enumerate(setup.traces):
            if tracefile:
                # Redirect the trace file to a local path
                setup.config[f"trace.file.{thread}"] = f"/tmp/{os.path.basename(tracefile)}"
                # Add command to copy the trace file to the local storage
                copy_workloads_cmd.append(f"cp '{tracefile}' /tmp")
        # Get the command to copy the database files to the local storage
        copy_workloads_cmd = '\n'.join(copy_workloads_cmd)
        # Store job ID for the load job
        load_job_id = None
        # Prepare the command to run the SIF container
        wrapped=f"""
    mkdir -p {fake_db}
    {copy_workloads_cmd}
    singularity run --bind '/tmp,{','.join(binds)}' {sif_path} {setup.build_cmd()}
    rm -rf {db} 
    cp -r {fake_db} {db}
    """
        sbatch_cmd = build_sbatch_cmd(
            name=f"load-{name}",
            cmd=wrapped,
            stdout=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-load-{name}.out",
            stderr=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-load-{name}.err",
        )

        # Restore the original database path in the setup config
        setup.config['rocksdb.dbname'] = db  

        if DRY_RUN:
            print(f"[DRY-RUN] [SIF] {' '.join(sbatch_cmd)}")
            return None

        # Run the sbatch command to submit the job
        result = subprocess.run(sbatch_cmd,
         stdout=subprocess.PIPE, 
         stderr=subprocess.PIPE, 
         universal_newlines=True)

        # Check if the job was submitted successfully
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                if line.startswith("Submitted batch job"):
                    # Extract the job ID from the output
                    load_job_id = line.split()[-1]

        return load_job_id


# RunSIF function to execute a YCSB workload using Singularity
def runSIF(name, setup: Setup, db_backup, outdir, status, load_job_id, sif_path=None, binds=[]):
    if not os.path.exists(sif_path):
        raise FileNotFoundError(f"SIF file not found: {sif_path}")
    
    # List commands to copy workload files to the local storage
    copy_workloads_cmd = []
    for thread, tracefile in enumerate(setup.traces):
        if tracefile:
            # Redirect the trace file to a local path
            setup.config[f"trace.file.{thread}"] = f"/tmp/{os.path.basename(tracefile)}"
            # Add command to copy the trace file to the local storage
            copy_workloads_cmd.append(f"cp '{tracefile}' /tmp")
    # Get the command to copy the database files to the local storage
    copy_workloads_cmd = '\n'.join(copy_workloads_cmd) 
    # Use local storage for the database
    db = "/tmp/db"
    # Redirect the database path to the local storage
    setup.config['rocksdb.dbname'] = db
    # Get the directory of the executable
    executable_dir = os.path.dirname(setup.executable)
    # Store dstat output in a local file
    local_dstat_output = "/tmp/dstat.csv"
    # Store YCSB output in a local file
    local_ycsb_output = "/tmp/ycsb.txt"
    # Prepare the output paths for dstat 
    dstat_output = os.path.join(outdir, 'dstat.csv')
    # Prepare the output paths for YCSB
    ycsb_output = os.path.join(outdir, 'ycsb.txt')
    # Prepare the command to run the SIF container
    wrapped = f"""
rm -rf {db}
cp -r {db_backup} {db}
{copy_workloads_cmd}
singularity run --bind '/tmp,{','.join(binds)}' {sif_path} bash -c "
    dstat -cdlmnyt > {local_dstat_output} 2>&1 &
    {setup.build_cmd(status)} > {local_ycsb_output}
    kill $(pgrep dstat)
"
cp {local_ycsb_output} {ycsb_output}
cp {local_dstat_output} {dstat_output}
"""
    print(f"[SIF] Submitting run job for {name}, setup: {setup.name}")

    sbatch_cmd = build_sbatch_cmd(
        name=name,
        cmd=wrapped,
        stdout=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-{name}.out",
        stderr=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-{name}.err",
        mem=setup.used_mem,
        jobid=load_job_id
    )

    if DRY_RUN:
        print(f"[DRY-RUN] [SIF] {' '.join(sbatch_cmd)}")
        return

    # Run the sbatch command to submit the job
    subprocess.run(sbatch_cmd)


# RunSIFController function to execute a YCSB workload with a controller using Singularity
def runSIFController(name, setup, db_backup, outdir, status, load_job_id, sif_path=None, binds=[]):
    if not os.path.exists(sif_path):
        raise FileNotFoundError(f"SIF file not found: {sif_path}")
    if not setup.controller_exec:
        raise ValueError("Controller executable not provided in setup.")

    # Use local storage for the database
    db = "/tmp/db"
    # Redirect the database path to the local storage
    setup.config['rocksdb.dbname'] = db
    # Store dstat output in a local file
    local_dstat_output = "/tmp/dstat.csv"
    # Store YCSB output in a local file
    local_ycsb_output = "/tmp/ycsb.txt"
    # Prepare the output paths for dstat
    dstat_output = os.path.join(outdir, 'dstat.csv')
    # Prepare the output paths for YCSB
    ycsb_output = os.path.join(outdir, 'ycsb.txt')
    # List commands to copy workload files to the local storage
    copy_workloads_cmd = []
    for thread, tracefile in enumerate(setup.traces):
        if tracefile:
            # Redirect the trace file to a local path
            setup.config[f"trace.file.{thread}"] = f"/tmp/{os.path.basename(tracefile)}"
            # Add command to copy the trace file to the local storage
            copy_workloads_cmd.append(f"cp '{tracefile}' /tmp")
    # Get the command to copy the database files to the local storage
    copy_workloads_cmd = '\n'.join(copy_workloads_cmd)

    # Prepare the command to run the SIF container of the client
    client_inner_script = f""" "
CLIENT_IP=\\$(hostname -I | awk '{{print \\$1}}' | xargs)
IPS=\\"-p cachelib.controller.address=\\$CONTROLLER_IP:11110\\"
for i in \\$(seq 0 {setup.threads - 1}); do
  PORT=\\$((11111+i))
  IPS+=\\" -p cachelib.holpaca.address.\\$i=\\$CLIENT_IP:\\$PORT\\"
done
mkdir -p {db} && cp -r {db_backup}/* {db}/
{copy_workloads_cmd}
singularity run --network host --bind '{executable_dir},/tmp,{','.join(binds)}' {sif_path} bash -c \\"
    dstat -cdlmnyt > {local_dstat_output} 2>&1 &
    {setup.build_cmd(status)} \\$IPS > {local_ycsb_output}
    kill \\$(pgrep dstat) 2>/dev/null || true
\\"
scancel \\"\\$CONTROLLER_JOB_ID\\" 2>/dev/null || true
cp {local_ycsb_output} {ycsb_output}
cp {local_dstat_output} {dstat_output}
" """

    # Get the sbatch command for scheduling the client job
    controller_sbatch = " ".join(build_sbatch_cmd(
        name=f"client-{name}",
        cmd=client_inner_script,
        stdout=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-client-{name}.out",
        stderr=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-client-{name}.err",
        mem=setup.used_mem,
        export="CONTROLLER_IP=$CONTROLLER_IP,CONTROLLER_JOB_ID=$CONTROLLER_JOB_ID"
        ))
    
    # Prepare the command to run the SIF container of the controller
    controller_inner_script = f"""
CONTROLLER_IP=$(hostname -I | awk '{{print $1}}' | xargs)
singularity run --network host --bind '{controller_dir},{executable_dir},{outdir},{','.join(binds)}' {sif_path} bash -c "
    dstat -cdlmnyt > {outdir}/controller_dstat.csv 2>&1 &
    {setup.controller_exec} $CONTROLLER_IP:11110 {setup.controller_args}
" &
CONTROLLER_JOB_ID=$SLURM_JOB_ID
{controller_sbatch}
sleep infinity
"""

    sbatch_cmd = build_sbatch_cmd(
        name=f"controller-{name}",
        cmd=controller_inner_script,
        stdout=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-controller-{name}.out",
        stderr=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-controller-{name}.err",
        jobid=load_job_id
    )    

    if DRY_RUN:
        print(f"[DRY-RUN] [SIF] {' '.join(sbatch_cmd)}")
        return

    # Run the sbatch command to submit the controller job
    subprocess.run()


def colocate(cases, output_dir, sif_path=None, binds=[]):
    os.makedirs(output_dir, exist_ok=True)

    case_ids = {case: f"case{i}" for i, case in enumerate(cases)}
    db_paths = {case: f"/tmp/db_{case_ids[case]}" for case in cases}

    load_cmds = []
    run_cmds = []
    task_id = 0
    total_mem = 0

    for i, case in enumerate(cases):
        case_id = case_ids[case]
        db_tmp = db_paths[case]
        setup = case.load
        executable_dir = os.path.dirname(setup.executable)

        copy_workloads_cmd = []
        for thread, tracefile in enumerate(setup.traces):
            if tracefile:
                setup.config[f"trace.file.{thread}"] = f"/tmp/{os.path.basename(tracefile)}"
                copy_workloads_cmd.append(f"cp '{tracefile}' /tmp")

        copy_cmds = "\n".join(copy_workloads_cmd)
        setup.config['rocksdb.dbname'] = db_tmp
        load_cmd = setup.build_cmd()

        load_block = f"""
srun --overlap bash -c "
mkdir -p {db_tmp}
{copy_cmds}
singularity run --bind '/tmp,{','.join(binds)}' {sif_path} {load_cmd}
" &
"""
        load_cmds.append(load_block)

        # Setup runs
        for setup in case.setups:
            for run_idx in range(case.runs):
                outdir = os.path.join(output_dir, sanitize(case.name), setup.name, str(run_idx + 1))
                os.makedirs(outdir, exist_ok=True)

                local_ycsb = f"/tmp/ycsb_{task_id}.txt"
                local_dstat = f"/tmp/dstat_{task_id}.csv"
                ycsb_out = os.path.join(outdir, "ycsb.txt")
                dstat_out = os.path.join(outdir, "dstat.csv")
                executable_dir = os.path.dirname(setup.executable)
                copy_workloads_cmd = []

                for thread, tracefile in enumerate(setup.traces):
                    if tracefile:
                        setup.config[f"trace.file.{thread}"] = f"/tmp/{os.path.basename(tracefile)}"
                        copy_workloads_cmd.append(f"cp '{tracefile}' /tmp")

                copy_cmds = "\n".join(copy_workloads_cmd)
                controller_prefix = ""
                controller_cleanup = ""

                if setup.controller_exec:
                    controller_port = 11110 + task_id * 1000
                    base_client_port = 11111 + task_id * 1000
                    controller_ip = "$HOSTNAME"
                    setup.controller_ip = f"{controller_ip}:{controller_port}"

                    ip_args = f"-p cachelib.controller.address={controller_ip}:{controller_port}"
                    for j in range(setup.threads):
                        ip_args += f" -p cachelib.holpaca.address.{j}={controller_ip}:{base_client_port + j}"

                    controller_prefix = f"""
{setup.controller_exec} {setup.controller_ip} {setup.controller_args} > /tmp/controller_{task_id}.log 2>&1 &
CONTROLLER_PID=$!
"""
                    controller_cleanup = "kill $CONTROLLER_PID 2>/dev/null || true"

                setup.config['rocksdb.dbname'] = db_paths[case]

                run_block = f"""
srun --exclusive -N1 -n1 bash -c "
{controller_prefix}
{copy_cmds}
dstat -cdlmnyt > {local_dstat} 2>&1 &
singularity run --bind '/tmp,{','.join(binds)}' {sif_path} {setup.build_cmd(case.status)} > {local_ycsb}
kill $(pgrep dstat) 2>/dev/null || true
{controller_cleanup}
cp {local_ycsb} {ycsb_out}
cp {local_dstat} {dstat_out}
" &
"""
                run_cmds.append(run_block)
                total_mem += setup.used_mem
                task_id += 1

    # Combine full job script
    script = "\n".join(load_cmds) + "\nwait\n\n" + "\n".join(run_cmds) + "\nwait\n"

    print(f"[SIF-Colocated] Submitting single-node full pipeline with {len(cases)} loads and {task_id} run tasks")

    sbatch_cmd = build_sbatch_cmd(
        name="colocated-all",
        cmd=script,
        stdout="/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-colocated-all.out",
        stderr="/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-colocated-all.err",
        mem=total_mem,
        ntasks=len(cases) + task_id
    )

    if DRY_RUN:
        print(f"[DRY-RUN] [SIF-Colocated] {' '.join(sbatch_cmd)}")
        return

    subprocess.run(sbatch_cmd)
