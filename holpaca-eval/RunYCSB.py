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
    setup_name = setup_name.replace(" ", "_").replace(":", "_")
    result_dir = os.path.join(base_dir, setup_name, str(run))
    shutil.rmtree(result_dir, ignore_errors=True)
    os.makedirs(result_dir, exist_ok=True)
    return result_dir

def build_workload_copy_snippet(config):
    snippet = ""
    original_parts = {}
    binds = []

    basefile = config.get("trace.file")
    if basefile:
        tmp_base = f"/tmp/{os.path.basename(basefile)}"
        snippet += f"cp {basefile} {tmp_base}; "
        config["trace.file"] = tmp_base
        binds.append(os.path.dirname(basefile))

    for i in range(int(config.get("threadcount", 1))):
        partfile = config.get(f"trace.file.{i}")
        if partfile:
            tmp_part = f"/tmp/{os.path.basename(partfile)}"
            snippet += f"cp {partfile} {tmp_part}; "
            original_parts[f"trace.file.{i}"] = partfile
            config[f"trace.file.{i}"] = tmp_part
            binds.append(os.path.dirname(partfile))
    return snippet, ','.join(binds), original_parts

def restore_workload_paths(config, original_parts):
    for key, value in original_parts.items():
        config[key] = value

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
        load_cfg_tmp = load_config.copy()
        workload_copy, tracesDirs, original_parts = build_workload_copy_snippet(load_cfg_tmp)
        load_cfg_tmp['rocksdb.dbname'] = "/tmp/db"
        load_cmd = f"{exe} -load -db cachelib-holpaca {build_param_str(load_cfg_tmp)}"
        load_job_id = None
        wrapped = f"""
mkdir -p /tmp/db
{workload_copy}
cd {sourceDir}
singularity run --bind '{sourceDir},/tmp,{tracesDirs}' {sif_path} {load_cmd}
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

        restore_workload_paths(load_cfg_tmp, original_parts)

        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                if line.startswith("Submitted batch job"):
                    load_job_id = line.split()[-1]

        if load_job_id is None:
            print("[ERROR] Failed to submit load job")
            return

    else:
        load_cmd = f"{exe} -load -db cachelib-holpaca {build_param_str(load_config)}"
        print(f"[LOCAL] Running load: {load_cmd}")
        subprocess.run(load_cmd, shell=True)

    os.makedirs(outputDir, exist_ok=True)
    with open(os.path.join(outputDir, 'setups.json'), 'w') as f:
        json.dump(setups, f, indent=2)

    for setup_name, setup_cfg in setups.items():
        for run in range(1, runs + 1):
            print(f"[RUN] Running {setup_name} for {name} (run {run})")
            rid = f"{setup_name}-{name}-run{run}"
            outdir = build_result_dir(outputDir, setup_name, run)

            if sif_path is not None:
                run_cfg_tmp = setup_cfg.copy()
                db = run_cfg_tmp['rocksdb.dbname']
                run_cfg_tmp['rocksdb.dbname'] = "/tmp/db"
                workload_copy, tracesDirs, original_parts = build_workload_copy_snippet(run_cfg_tmp)
                inner = f"""
mkdir -p /tmp/db && cp -r {db_bkp}/* /tmp/db/
{workload_copy}
cd {sourceDir}
dstat -cdlmnyt > /tmp/dstat.csv 2>&1 &
{exe} -run -db cachelib-holpaca -s {status} {build_param_str(run_cfg_tmp)} > /tmp/ycsb.txt
kill $(pgrep dstat)
cp /tmp/ycsb.txt {outdir}/ycsb.txt
cp /tmp/dstat.csv {outdir}/dstat.csv
"""
                wrapped = f"singularity run --bind '{sourceDir},/tmp,{tracesDirs}' {sif_path} bash -c '{inner}'"

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
                restore_workload_paths(run_cfg_tmp, original_parts)
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
