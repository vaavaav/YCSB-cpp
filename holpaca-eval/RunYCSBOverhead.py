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
        with_controller=False,
        controller_exec=None,
        controller_args=None,
        status=None,
    ):
        self.name = name
        self.config = config
        self.executable = executable
        self.with_controller = with_controller
        self.controller_exec = controller_exec
        self.controller_ip = config.get("cachelib.controller.address", None)
        self.controller_args = controller_args or ""
        self.threads = int(config.get("threadcount", 1))
        self.out = {}
        self.status = status

    def build_cmd(self):
        return f"{self.executable} -load -run -db cachelib-holpaca-overhead {f'-s {self.status}' if self.status else ''} {' '.join(f'-p {k}={v}' for k, v in self.config.items())}"

    def run_controller(self, sifPath, binds=[], rehearse=False):
        if not os.path.exists(sifPath):
            raise FileNotFoundError(f"SIF file not found: {sifPath}")

        if not os.path.exists(self.controller_exec):
            raise ValueError(f"Controller executable not found: {self.controller_exec}")

        # Store dool output in a local file
        local_dool_output = "/tmp/dool.csv"
        # Store YCSB output in a local file
        local_ycsb_output = "/tmp/ycsb.txt"
        # Prepare the output paths for dool
        dool_output = os.path.join(self.out, "dool.csv")
        # Prepare the output paths for YCSB
        ycsb_output = os.path.join(self.out, "ycsb.txt")
        # List commands to copy workload files to the local storage

        # Prepare the command to run the SIF container of the client
        client_inner_script = f""" "
    CLIENT_IP=\\$(hostname -I | awk '{{print \\$1}}' | xargs)
    IPS=\\"-p cachelib.controller.address=\\$CONTROLLER_IP:11110\\"
    for i in \\$(seq 0 {self.threads - 1}); do
      PORT=\\$((11111+i))
      IPS+=\\" -p cachelib.holpaca.address.\\$i=\\$CLIENT_IP:\\$PORT\\"
    done
    singularity run --network host --bind '{','.join(binds)}' {sifPath} bash -c \\"
        dool -cdlmnyt --output {local_dool_output} &
        {self.build_cmd()} \\$IPS > {local_ycsb_output}
        kill \\$(pgrep dool) 2>/dev/null || true
    \\"
    scancel \\"\\$CONTROLLER_JOB_ID\\" 2>/dev/null || true
    cp {local_ycsb_output} {ycsb_output}
    cp {local_dool_output} {dool_output}
    " """

        # Get the sbatch command for scheduling the client job
        controller_sbatch = " ".join(
            build_sbatch_cmd(
                name=f"client-{self.name}",
                cmd=client_inner_script,
                stdout=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-client-{self.name}.out",
                stderr=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-client-{self.name}.err",
                export="CONTROLLER_IP=$CONTROLLER_IP,CONTROLLER_JOB_ID=$CONTROLLER_JOB_ID",
            )
        )

        controller_dir = os.path.dirname(self.controller_exec)
        executable_dir = os.path.dirname(self.executable)
        # Prepare the command to run the SIF container of the controller
        controller_inner_script = f"""
    CONTROLLER_IP=$(hostname -I | awk '{{print $1}}' | xargs)
    singularity run --network host --bind '{controller_dir},{executable_dir},{self.out},{','.join(binds)}' {sifPath} bash -c "
        dool -cdlmnyt --output {self.out}/controller_dool.csv &
        {self.controller_exec} $CONTROLLER_IP:11110 {self.controller_args} > {self.out}/controller.log 2>&1
    " &
    CONTROLLER_JOB_ID=$SLURM_JOB_ID
    {controller_sbatch}
    sleep infinity
    """

        sbatch_cmd = build_sbatch_cmd(
            name=f"controller-{self.name}",
            cmd=controller_inner_script,
            stdout=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-controller-{self.name}.out",
            stderr=f"/projects/F202400014TESTDEUCALION/pedro/YCSB-cpp/slurm-controller-{self.name}.err",
        )

        if rehearse:
            print(f"[REHEARSE] {' '.join(sbatch_cmd)}")
            return

        # Run the sbatch command to submit the controller job
        subprocess.run(sbatch_cmd)

    def run(self, sifPath, binds=[], rehearse=False):
        if not os.path.exists(sifPath):
            raise FileNotFoundError(f"SIF file not found: {sifPath}")

        if self.with_controller:
            self.run_controller(sifPath, binds, rehearse)
            return

        if not os.path.exists(self.out) and not rehearse:
            os.makedirs(self.out, exist_ok=True)

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
