import glob
import os
import subprocess
import sys
from pathlib import Path


def _touch(fname):
    if os.path.exists(fname):
        os.utime(fname, None)
    else:
        open(fname, 'a').close()


def _exec(cmd):
    with subprocess.Popen(
            cmd,
            #stdout=subprocess.PIPE,
            #stderr=subprocess.PIPE,
            stdout=sys.stdout,
            stderr=sys.stderr,
            shell=True,
            text=True
    ) as p:
        p.wait()
        return p.returncode


def launch_task(control_dir, is_slurm, wait_for_completion=False):

    task_key = os.path.basename(control_dir)
    pipeline_instance_dir = os.path.dirname(os.path.dirname(control_dir))

    r_script, r_sbatch_script, r_out, r_err, r_out_sig_dir, r_state_file_glob, launch_state = map(
        lambda f: os.path.join(control_dir, f), [
            "script.sh",
            "sbatch-launcher.sh",
            "out.log",
            "err.log",
            "out_sigs",
            "state.*",
            "state.launched.0"
        ]
    )

    r_work_dir = os.path.join(pipeline_instance_dir, "publish", task_key)

    Path(r_work_dir).mkdir(exist_ok=True)
    Path(r_out_sig_dir).mkdir(exist_ok=True)

    for f in glob.glob(r_state_file_glob):
        os.remove(f)

    _touch(launch_state)

    if not is_slurm:

        ampersand_or_not = "&"

        if wait_for_completion:
            ampersand_or_not = ""

        cmd = f"nohup bash -c '{r_script}  >{r_out} 2>{r_err}' {ampersand_or_not}"
    else:

        Path(os.path.join(r_work_dir, "scratch")).mkdir(exist_ok=True)

        if wait_for_completion:
            cmd = f"bash -c 'export SBATCH_WAIT=True && {r_sbatch_script}'"
        else:
            cmd = r_sbatch_script

    print(f"res:{_exec(cmd)}")


if __name__ == '__main__':

    def exit_error(msg, code=1):
        print(msg, file=sys.stderr)
        os._exit(code)

    if len(sys.argv) < 1:
        exit_error("insufficient args")

    cmd_name = sys.argv[1]

    if cmd_name == "launch_task":
        launch_task(sys.argv[2], sys.argv[3] == "True")