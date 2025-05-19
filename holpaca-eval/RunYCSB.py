import subprocess
import shutil
import os
from datetime import datetime
import json

def build_param_str(params):
    return " ".join(f"-p {k}={v}" for k, v in params.items())

def get_mem_mb(cachesize):
    return int(cachesize * 1.2 / 1024 / 1024)

def build_sbatch_cmd(name, mem, cmd, stdout, stderr, jobid=None):
    cmd = [
        "sbatch",
        f"--job-name={name}",
        "--account=f202400014testdeucalionx",
        "--nodes=1",
        "--ntasks=1",
        "--cpus-per-task=1",
        f"--mem={mem}M",
        "--partition=large-x86",
        "--mail-type=END",
        "--mail-user=jose.p.peixoto@inesctec.pt",
        f"--output={stdout}",
        f"--error={stderr}",
        "--wrap", cmd
    ]
    if jobid:
        cmd.insert(1, f"--dependency=afterok:{jobid}")
    return cmd

def build_result_dir(base_dir, setup_name, run):
    # sanitize everything
    setup_name = setup_name.replace(" ", "_").replace(":", "_")
    # create the directory
    result_dir = os.path.join(base_dir, setup_name, str(run))
    shutil.rmtree(result_dir, ignore_errors=True)
    os.makedirs(result_dir, exist_ok=True)
    return result_dir


def RunYCSB(name, sourceDir, outputDir, load_config, setups, runs, status='READ-FAILED READ-PASSED ALL', sif_path=None):
    exe = os.path.join(sourceDir, 'build-ycsb/ycsb')

    if 'cachelib.size' not in load_config:
        print("[ERROR] Missing cache size in load_config")
        return

    mem_mb = get_mem_mb(load_config['cachelib.size'])

    db_bkp = load_config['rocksdb.dbname']
    shutil.rmtree(db_bkp, ignore_errors=True)
    os.makedirs(db_bkp, exist_ok=True)

    if sif_path is not None:
        load_config['rocksdb.dbname'] = "/tmp/db"
        load_cmd = f"{exe} -load -db cachelib-holpaca {build_param_str(load_config)}"
        load_job_id = None
        wrapped = f"""
mkdir -p /tmp/db 
cd {sourceDir}
singularity run --bind '{sourceDir},/tmp' {sif_path} {load_cmd}
cp /tmp/db/* {db_bkp}/
"""
        result = subprocess.run(build_sbatch_cmd(
            name=f"load-{name}",
            mem=mem_mb,
            cmd=wrapped,
            stdout=f"/tmp/slurm-load-{name}.out",
            stderr=f"/tmp/slurm-load-{name}.err"
        ),
         stdout=subprocess.PIPE, 
         stderr=subprocess.PIPE, 
         universal_newlines=True)
        

        # Parse job ID
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                print(line)
                if line.startswith("Submitted batch job"):
                    load_job_id = line.split()[-1]

        if load_job_id is None:
            print("[ERROR] Failed to submit load job")
            return
        
        load_config['rocksdb.dbname'] = db_bkp

    else:
        load_cmd = f"{exe} -load -db cachelib-holpaca {build_param_str(load_config)}"
        print(f"[LOCAL] Running load: {load_cmd}")
        subprocess.run(load_cmd, shell=True)

    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, 'setups.json'), 'w') as f:
        json.dump(setups, f, indent=2)

    # Run phase
    for setup_name, setup_cfg in setups.items():

        for run in range(1, runs + 1):
            print(f"[RUN] Running {setup_name} for {name} (run {run})")
            rid = f"{setup_name}-{name}-run{run}"
            outdir = build_result_dir(outputDir, setup_name, run)

            if sif_path is not None:
                db = setup_cfg['rocksdb.dbname']
                setup_cfg['rocksdb.dbname'] = "/tmp/db"
                inner = f"""
mkdir -p /tmp/db && cp -r {db_bkp}/* /tmp/db/
cd {sourceDir}
dstat -cdlmnyt > /tmp/dstat.csv 2>&1 &
{exe} -run -db cachelib-holpaca -s {status} {build_param_str(setup_cfg)} > /tmp/ycsb.txt
kill $(pgrep dstat)
cp /tmp/ycsb.txt {outdir}/ycsb.txt
cp /tmp/dstat.csv {outdir}/dstat.csv
"""
                wrapped = f"singularity run --bind {sourceDir},/tmp {sif_path} bash -c '{inner}'"

                sbatch_cmd = build_sbatch_cmd(
                    name=rid,
                    mem=mem_mb,
                    cmd=wrapped,
                    stdout=f"/tmp/slurm-{rid}.out",
                    stderr=f"/tmp/slurm-{rid}.err",
                    jobid=load_job_id
                )
                print(sbatch_cmd)
                subprocess.run(sbatch_cmd)
                setup_cfg['rocksdb.dbname'] = db
            else:
                db = setup_cfg['rocksdb.dbname']
                shutil.rmtree(db, ignore_errors=True)
                shutil.copytree(db_bkp, db)
                dstat_out = os.path.join(outdir, 'dstat.csv')
                ycsb_out = os.path.join(outdir, 'ycsb.txt')
                print(f"[LOCAL] Running: {exe} -run -db cachelib-holpaca -s {status} {build_param_str(setup_cfg)}")
                dstat_output = open(dstat_out, 'w')
                dstat = subprocess.Popen(["dstat", "-cdlmnyt"], stdout=dstat_output)
                cmd = f"{exe} -run -db cachelib-holpaca -s {status} {build_param_str(setup_cfg)}"
                with open(ycsb_out, 'w') as outf:
                    subprocess.run(cmd, shell=True, stdout=outf)
                dstat.terminate()
                dstat_output.close()


