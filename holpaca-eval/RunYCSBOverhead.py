import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime

sanitize = lambda name: re.sub(r"\W+", "_", name)

DRY_RUN = False
TIMEOUT = None


def build_sbatch_cmd(
    name, cmd, stdout, stderr, mem=None, jobid=None, export=None, ntasks=1
):
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
        cmd,
    ]
    if jobid:
        sbatch_cmd.insert(1, f"--dependency=afterok:{jobid}")
    if export:
        sbatch_cmd.insert(1, f"--export={export}")
    if TIMEOUT:
        sbatch_cmd.insert(1, f"--time={TIMEOUT}")
    return sbatch_cmd


class Setup:
    def __init__(
        self,
        name,
        executable,
        config,
        controller_exec=None,
        controller_args=None,
        status=None,
    ):
        self.name = name
        self.config = config
        self.executable = executable
        self.controller_exec = controller_exec
        self.controller_ip = config.get("cachelib.controller.address", None)
        self.controller_args = controller_args or ""
        self.threads = int(config.get("threadcount", 1))
        self.out = {}
        self.status = status

    def build_cmd(self):
        return f"{self.executable} -load -run -db cachelib-holpaca-overhead {f'-s {self.status}' if self.status else ''} {' '.join(f'-p {k}={v}' for k, v in self.config.items())}"

    def run(self, sifPath, outputDir, binds=[], rehearse=False):
        if not os.path.exists(sifPath):
            raise FileNotFoundError(f"SIF file not found: {sifPath}")

        if not os.path.exists(outputDir) and not rehearse:
            os.makedirs(outputDir, exist_ok=True)

        # Get the directory of the executable
        executable_dir = os.path.dirname(self.executable)
        # Store dool output in a local file
        local_dool_output = "/tmp/dool.csv"
        # Store YCSB output in a local file
        local_ycsb_output = "/tmp/ycsb.txt"
        # Prepare the output paths for dool
        dool_output = os.path.join(self.out, "dool.csv")
        # Prepare the output paths for YCSB
        ycsb_output = os.path.join(self.out, "ycsb.txt")
        # Prepare the command to run the SIF container
        wrapped = f"""
    singularity run --bind '{','.join(binds)}' {sifPath} bash -c "
        dool -cdlmnyt --output {local_dool_output} &
        {self.build_cmd()} > {local_ycsb_output}
        kill $(pgrep dool)
    "
    cp {local_ycsb_output} {ycsb_output}
    cp {local_dool_output} {dool_output}
    """
        print(f"[OVERHEAD] Submitting run job for {self.name}")

        sbatch_cmd = build_sbatch_cmd(
            name=self.name,
            cmd=wrapped,
            stdout=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-{self.name}.out",
            stderr=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-{self.name}.err",
        )

        if rehearse:
            print(f"[REHEARSE] {' '.join(sbatch_cmd)}")
            return

        # Run the sbatch command to submit the job
        subprocess.run(sbatch_cmd)
