import glob
import json
import os
import signal
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path


def python_shebang():
    return "#!/usr/bin/env python3"


def task_script_header():
    return f"{python_shebang()}\n" + textwrap.dedent(f"""            
        import os
        import sys 
        import importlib.machinery
        import importlib.util        
        
        __script_location = os.path.dirname(os.path.abspath(__file__))
        script_lib_path = os.path.join(os.path.dirname(__script_location), 'script_lib.py')        
        loader = importlib.machinery.SourceFileLoader('script_lib', script_lib_path)
        spec = importlib.util.spec_from_loader(loader.name, loader)
        script_lib = importlib.util.module_from_spec(spec)
        loader.exec_module(script_lib)                                
        env = script_lib.source_task_env(os.path.join(__script_location, 'task-env.sh'))
        script_lib.register_timeout_handler()        
    """)


def env_from_sourcing(env_file):

    p = os.path.abspath(sys.executable)

    dump_with_python_script = f'{p} -c "import os, json; print(json.dumps(dict(os.environ)))"'

    with subprocess.Popen(
            ['/bin/bash', '-c', f". {env_file} && {dump_with_python_script}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    ) as pipe:
        pipe.wait()
        err = pipe.stderr.read()
        out = pipe.stdout.read()
        if pipe.returncode != 0:
            raise Exception(f"Failed sourcing env file: {env_file}\n{err}")
        return json.loads(out)


def source_task_env(task_env_file):

    env = env_from_sourcing(task_env_file)

    for k, v in env.items():
        os.environ[k] = v

    return env


def read_task_state(control_dir=None):

    if control_dir is None:
        control_dir = os.environ["__control_dir"]

    glob_exp = os.path.join(control_dir, "state.*")
    state_file = list(glob.glob(glob_exp))

    if len(state_file) == 0:
        raise Exception(f"no state file in {control_dir}, {glob_exp}")
    if len(state_file) > 1:
        raise Exception(f"more than one state file found in {control_dir}, {glob_exp}")

    state_file = state_file[0]
    base_file_name = os.path.basename(state_file)
    name_parts = base_file_name.split(".")

    if len(name_parts) == 3:
        step_number = int(name_parts[-1])
    else:
        step_number = None

    return step_number, control_dir, state_file


def touch(fname):
    if os.path.exists(fname):
        os.utime(fname, None)
    else:
        open(fname, 'a').close()


def _append_to_history(control_dir, state_name, step_number=None):
    with open(os.path.join(control_dir, "history.tsv"), "a") as f:
        f.write(state_name)
        f.write("\t")
        f.write(datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f'))
        f.write("\t")
        if step_number is None:
            f.write("")
        else:
            f.write(str(step_number))
        f.write("\n")


def _transition_state_file(state_file, next_state_name, step_number, inc_step_number=False):

    control_dir = os.path.dirname(state_file)

    if step_number is None:
        next_step_number = None
        next_state_basename = f"state.{next_state_name}"
    else:
        next_step_number = step_number + 1 if inc_step_number else step_number
        next_state_basename = f"state.{next_state_name}.{next_step_number}"

    next_state_file = os.path.join(control_dir, next_state_basename)

    os.rename(
        state_file,
        next_state_file
    )

    _append_to_history(control_dir, next_state_name, step_number)

    return next_state_file, next_step_number


def transition_to_step_started(state_file, step_number):
    return _transition_state_file(state_file, "step-started", step_number)


def transition_to_step_completed(state_file, step_number):
    return _transition_state_file(state_file, "step-completed", step_number, inc_step_number=True)

def register_timeout_handler(sig=signal.SIGUSR1):

    def timeout_handler(s, frame):
        step_number, control_dir, state_file = read_task_state()
        _transition_state_file(state_file, "timed-out", step_number)
        exit(1)

    signal.signal(sig, timeout_handler)


def sign_files():
    file_list_to_sign = os.environ.get('__file_list_to_sign')

    if file_list_to_sign is None or file_list_to_sign == "":
        return

    sig_dir = os.path.join(os.environ['__control_dir'], 'out_sigs')

    Path(sig_dir).mkdir(exist_ok=True)

    def all_files():
        for f in file_list_to_sign.split(","):
            if os.path.exists(f):
                yield f

    def checksum_one_file(f):
        bf = os.path.basename(f)
        sig_file = os.path.join(sig_dir, f"{bf}.sig")
        cmd = f"sha1sum {f}"
        with subprocess.Popen(
            cmd.split(" "),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        ) as p:
            p.wait()
            if p.returncode != 0:
                raise Exception(f"failed: '{cmd}'")
            sha1sum = p.stdout.read().strip().decode("utf8")
            with open(sig_file, "w") as f:
                f.write(sha1sum)
            return True

    with ThreadPoolExecutor(max_workers=4) as executor:
        for b in executor.map(checksum_one_file, all_files()):
            assert b



def transition_to_completed():

    return


def run_script(cmd):

    class Tee(object):
        def __init__(self, name, mode, echo_file=None):
            self.name = name
            self.mode = mode
            self.echo_file = echo_file

        def __enter__(self):
            self.file = open(self.name, self.mode)

        def __exit__(self, *args):
            self.file.close()

        def write(self, data):
            self.file.write(data)
            if self.echo_file is not None:
                self.echo_file.write(data)

        def flush(self):
            self.file.flush()
            if self.echo_file is not None:
                self.echo_file.flush()

    #with Tee(os.environ['__out'], 'w', None if is_silent else sys.stdout) as out:
    #    with Tee(os.environ['__err'], 'w', None if is_silent else sys.stderr) as err:

    is_silent = "--is-silent" in sys.argv

    with open(os.environ['__out'], 'w') as out:
        with open(os.environ['__err'], 'w') as err:
            with subprocess.Popen(
                    cmd,
                    stdout=out,
                    stderr=err,
                    shell=True
            ) as p:
                p.wait()
                if p.returncode != 0:
                    step_number, control_dir, state_file = read_task_state()
                    _transition_state_file(state_file, "failed", step_number)
                    exit(1)


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

    touch(launch_state)

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

    print(f"res:{run_script(cmd)}")
