import subprocess
import shutil
import os
from datetime import datetime

def build_param_str(params):
    return " ".join(f"-p {k}={v}" for k, v in params.items())

def get_mem_mb(cachesize):
    return int(cachesize * 1.2 / 1024 / 1024)

def build_sbatch_cmd(name, mem, cmd, stdout, stderr):
    return [
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

def build_result_dir(base_dir, setup_name, workload, run):
    # sanitize everything
    setup_name = setup_name.replace(" ", "_").replace(":", "_")
    workload = workload.replace(" ", "_").replace(":", "_")
    run = str(run).replace(" ", "_").replace(":", "_")
    # create the directory
    result_dir = os.path.join(base_dir, setup_name, workload, run)
    os.makedirs(result_dir, exist_ok=True)
    return result_dir


def RunYCSB(sourceDir, workloads, outputDir, load_config, setups, runs, status='READ-FAILED READ-PASSED ALL', sif_path=None):
    exe = os.path.join(sourceDir, 'build-ycsb/ycsb')

    if 'cachelib.size' not in load_config:
        print("[ERROR] Missing cache size in load_config")
        return

    mem_mb = get_mem_mb(load_config['cachelib.size'])

    for wl_path in workloads:
        if not os.path.exists(wl_path):
            print(f"[ERROR] Missing workload: {wl_path}")
            continue

        db_bkp = os.path.join(sourceDir, 'db-backup', os.path.basename(wl_path))
        db = os.path.join(sourceDir, 'db', os.path.basename(wl_path))
        os.makedirs(db_bkp, exist_ok=True)
        load_config['rocksdb.dbname'] = db_bkp
        load_cmd = f"{exe} -load -db cachelib-holpaca -P {wl_path} {build_param_str(load_config)}"

        if sif_path:
            wrapped = f"singularity run --bind '{sourceDir},/tmp' {sif_path} {load_cmd}"
            subprocess.run(build_sbatch_cmd(
                name=f"load-{os.path.basename(wl_path)}",
                mem=mem_mb,
                cmd=wrapped,
                stdout=f"/tmp/slurm-load-{os.path.basename(wl_path)}.out",
                stderr=f"/tmp/slurm-load-{os.path.basename(wl_path)}.err"
            ))
        else:
            print(f"[LOCAL] Running load: {load_cmd}")
            subprocess.run(load_cmd, shell=True)

        # Run phase
        for setup_name, setup_cfg in setups.items():

            for run in range(1, runs + 1):
                print(f"[RUN] Running {setup_name} for {os.path.basename(wl_path)} (run {run})")
                rid = f"{setup_name}-{os.path.basename(wl_path)}-run{run}"
                outdir = build_result_dir(outputDir, setup_name, os.path.basename(wl_path), run)

                if sif_path:
                    setup_cfg['config']['rocksdb.dbname'] = "/tmp/db"
                    inner = f"""
mkdir -p /tmp/db && cp -r {db_bkp}/* /tmp/db/
cd {sourceDir}
dstat -cdlmnyt > /tmp/dstat.csv 2>&1 &
{exe} -run -db cachelib-holpaca-P {wl_path} -s {status} {build_param_str(setup_cfg['config'])} > /tmp/ycsb.txt
kill $(pgrep dstat)
cp /tmp/ycsb.txt {outdir}/ycsb.txt
"""
                    wrapped = f"singularity run --bind {sourceDir},/tmp {sif_path} bash -c '{inner}'"
                    sbatch_cmd = build_sbatch_cmd(
                        name=rid,
                        mem=mem_mb,
                        cmd=wrapped,
                        stdout=f"/tmp/slurm-{rid}.out",
                        stderr=f"/tmp/slurm-{rid}.err"
                    )
                    subprocess.run(sbatch_cmd)
                else:
                    setup_cfg['config']['rocksdb.dbname'] = db
                    shutil.rmtree(db, ignore_errors=True)
                    shutil.copytree(db_bkp, db)
                    dstat_out = os.path.join(outdir, 'dstat.csv')
                    ycsb_out = os.path.join(outdir, 'ycsb.txt')
                    print(f"[LOCAL] Running: {exe} -run -db cachelib-holpaca -P {wl_path} -s {status} {build_param_str(setup_cfg['config'])}")
                    dstat_output = open(dstat_out, 'w')
                    dstat = subprocess.Popen(["dstat", "-cdlmnyt"], stdout=dstat_output)
                    cmd = f"{exe} -run -db cachelib-holpaca -P {wl_path} -s {status} {build_param_str(setup_cfg['config'])}"
                    with open(ycsb_out, 'w') as outf:
                        subprocess.run(cmd, shell=True, stdout=outf)
                    dstat.terminate()
                    dstat_output.close()


