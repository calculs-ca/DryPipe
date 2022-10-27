import glob
import json
import os
import signal
import subprocess
import sys
import textwrap
import logging
import logging.config
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import groupby
from pathlib import Path
from threading import Thread


def create_task_logger(task_control_dir):
    _logger = logging.getLogger()
    h = logging.FileHandler(filename=os.path.join(task_control_dir, "drypipe.log"))
    if os.environ.get("DRYPIPE_TASK_DEBUG") != "True":
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG
    h.setLevel(logging_level)
    _logger.setLevel(logging_level)
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    _logger.addHandler(h)
    return _logger


__script_location = os.environ.get("__script_location")


if __script_location is None:
    logger = logging.getLogger(__name__)
else:
    logger = create_task_logger(__script_location)



def python_shebang():
    return "#!/usr/bin/env python3"


def write_pipeline_lib_script(file_handle):
    file_handle.write(
        f"{python_shebang()}\n" + textwrap.dedent(f"""            
            import os
            import sys 
            import importlib.machinery
            import importlib.util        

            __script_location = os.path.dirname(os.path.abspath(__file__))
            script_lib_path = os.path.join(__script_location, 'script_lib.py')        
            loader = importlib.machinery.SourceFileLoader('script_lib', script_lib_path)
            spec = importlib.util.spec_from_loader(loader.name, loader)
            script_lib = importlib.util.module_from_spec(spec)
            loader.exec_module(script_lib)
        """)
    )

    file_handle.write(textwrap.dedent(f"""        
    if __name__ == '__main__':
    
        if "gen-input-var-exports" in sys.argv:
            script_lib.gen_input_var_exports()
        elif "launch-task-remote" in sys.argv:
            task_key = sys.argv[2]
            is_slurm = "--is-slurm" in sys.argv
            wait_for_completion = "--wait-for-completion" in sys.argv
            script_lib.launch_task_remote(task_key, is_slurm, wait_for_completion)
        else:
            raise Exception('invalid args')
    """))


def task_script_header():

    return f"{python_shebang()}\n" + textwrap.dedent(f"""            
        import os
        import sys 
        import signal
        from threading import Thread
        import importlib.machinery
        import importlib.util        

        is_slurm = os.environ.get("__is_slurm") == "True"        
        if is_slurm:
            __script_location = os.environ['__script_location']
        else:
            __script_location = os.path.dirname(os.path.abspath(__file__))
            os.environ["__script_location"] = __script_location
        script_lib_path = os.path.join(os.path.dirname(__script_location), 'script_lib.py')        
        loader = importlib.machinery.SourceFileLoader('script_lib', script_lib_path)
        spec = importlib.util.spec_from_loader(loader.name, loader)
        script_lib = importlib.util.module_from_spec(spec)
        loader.exec_module(script_lib)
        env = script_lib.source_task_env(os.path.join(__script_location, 'task-env.sh'))        
        script_lib.ensure_upstream_tasks_completed(env)        
    """)


def _exit_process():
    logging.shutdown()
    os._exit(0)


def run_python(python_bin, mod_func, container=None):
    switches = "-u"
    cmd = [
        python_bin,
        switches,
        "-m",
        "dry_pipe.cli",
        "call",
        mod_func
    ]

    if container is not None:
        cmd = [
            "singularity",
            "exec",
            os.path.join(os.environ['__containers_dir'], container)
        ] + cmd

    env = {**os.environ}

    if os.environ.get("__is_slurm"):
        env['__scratch_dir'] = os.environ['SLURM_TMPDIR']

    has_failed = False

    logger.info("run_python: %s", ' '.join(cmd))

    with open(os.environ['__out_log'], 'a') as out:
        with open(os.environ['__err_log'], 'a') as err:
            with subprocess.Popen(cmd, stdout=out, stderr=err) as p:
                try:
                    p.wait()
                    has_failed = p.returncode != 0
                except Exception as ex:
                    has_failed = True
                    logger.exception(ex)
                finally:
                    if has_failed:
                        step_number, control_dir, state_file, state_name = read_task_state()
                        _transition_state_file(state_file, "failed", step_number)
    if has_failed:
        _exit_process()


def ensure_upstream_tasks_completed(env):
    non_completed_dependent_task = env.get("__non_completed_dependent_task")
    if non_completed_dependent_task is not None:
        print("upstream dependent task : " + non_completed_dependent_task + " not completed, cannot run.")


def _fail_safe_stderr(process):
    try:
        err = process.stderr.read().decode("utf-8")
    except Exception as _e:
        err = ""
    return err


def _terminate_descendants_and_exit(p1, p2):

    logger.info("signal SIGTERM received, will transition to failed and terminate descendants")
    step_number, control_dir, state_file, state_name = read_task_state()
    _transition_state_file(state_file, "failed", step_number)
    logger.info("will terminate descendants")
    try:
        this_pid = str(os.getpid())
        with subprocess.Popen(
            ['ps', '-opid', '--no-headers', '--ppid', this_pid],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ) as p:
            p.wait()
            pids = [
                int(line.decode("utf-8").strip())
                for line in p.stdout.readlines()
            ]
            pids = [pid for pid in pids if pid != p.pid]
            logger.debug("descendants of %s: %s", this_pid, pids)
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception as _:
                    pass
    finally:
        _exit_process()

def env_from_sourcing(env_file):

    p = os.path.abspath(sys.executable)

    dump_with_python_script = f'{p} -c "import os, json; print(json.dumps(dict(os.environ)))"'

    with subprocess.Popen(
            ['/bin/bash', '-c', f". {env_file} && {dump_with_python_script}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as p:
        p.wait()
        out = p.stdout.read().decode("utf-8")
        if p.returncode != 0:
            raise Exception(f"Failed sourcing env file: {env_file}\n{_fail_safe_stderr(p)}")
        return json.loads(out)


def iterate_out_vars_from(file):
    if os.path.exists(file):
        with open(file) as f:
            for line in f.readlines():
                var_name, value = line.split("=")
                yield var_name.strip(), value.strip()


def source_task_env(task_env_file):

    env = env_from_sourcing(task_env_file)

    for k, v in env.items():
        os.environ[k] = v

    return env


def parse_in_out_meta(name_to_meta_dict):

    # export __meta_<var-name>="(int|str|float):<producing-task-key>:<name_in_producing_task>"
    # export __meta_<file-name>="file:<producing-task-key>:<name_in_producing_task>"

    #Note: when producing-task-key == "", meta are output values and files

    def gen():
        for k, v in name_to_meta_dict.items():
            var_name = k[7:]
            typez, task_key, name_in_producing_task = v.split(":")

            yield task_key, name_in_producing_task, var_name, typez

    def k(t):
        return t[0]

    for producing_task_key, produced_files_or_vars_meta in groupby(sorted(gen(), key=k), key=k):

        def filter_and_map(condition_on_typez):
            return [
                (name_in_producing_task, var_name, typez)
                for task_key, name_in_producing_task, var_name, typez
                in produced_files_or_vars_meta
                if condition_on_typez(typez)
            ]

        yield producing_task_key, filter_and_map(lambda t: t != "file"), filter_and_map(lambda t: t == "file")


def read_task_state(control_dir=None, no_state_file_ok=False):

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
    state_name = name_parts[1]

    if len(name_parts) == 3:
        step_number = int(name_parts[-1])
    else:
        step_number = 0

    return step_number, control_dir, state_file, state_name


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


def _transition_state_file(state_file, next_state_name, step_number=None):

    control_dir = os.path.dirname(state_file)

    if step_number is None:
        next_step_number = None
        next_state_basename = f"state.{next_state_name}"
    else:
        next_step_number = step_number
        next_state_basename = f"state.{next_state_name}.{next_step_number}"

    next_state_file = os.path.join(control_dir, next_state_basename)

    logger.info("will transition to: %s", next_state_basename)

    os.rename(
        state_file,
        next_state_file
    )

    _append_to_history(control_dir, next_state_name, step_number)

    return next_state_file, next_step_number


def transition_to_step_started(state_file, step_number):
    return _transition_state_file(state_file, "step-started", step_number)


def transition_to_step_completed(state_file, step_number):
    state_file, step_number = _transition_state_file(state_file, "step-completed", step_number)
    return state_file, step_number + 1


def register_signal_handlers():

    def timeout_handler(s, frame):
        step_number, control_dir, state_file, state_name = read_task_state()
        _transition_state_file(state_file, "timed-out", step_number)
        _exit_process()

    logger.debug("will register signal handlers")

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    signal.signal(signal.SIGUSR1, timeout_handler)
    signal.signal(signal.SIGTERM, _terminate_descendants_and_exit)


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


def transition_to_completed(state_file):
    return _transition_state_file(state_file, "completed-unsigned")


def run_script(script, container=None):

    env = {**os.environ}

    if container is None:
        cmd = [script]
    else:
        cmd = [
            "singularity",
            "exec",
            os.path.join(os.environ['__containers_dir'], container),
            script
        ]

        singularity_bindings = []

        root_dir_of_script = _root_dir(script)

        if _fs_type(root_dir_of_script) in ["autofs", "nfs", "zfs"]:
            singularity_bindings.append(f"{root_dir_of_script}:{root_dir_of_script}")

        if os.environ.get("__is_slurm"):
            scratch_dir = os.environ['SLURM_TMPDIR']
            env['__scratch_dir'] = scratch_dir
            root_of_scratch_dir = _root_dir(scratch_dir)
            singularity_bindings.append(f"{root_of_scratch_dir}:{root_of_scratch_dir}")

        if len(singularity_bindings) > 0:

            prev_singularity_bindings = env.get("SINGULARITY_BIND")

            if prev_singularity_bindings is not None and prev_singularity_bindings != "":
                bindings_prefix = f"{prev_singularity_bindings},"
            else:
                bindings_prefix = ""

            env["SINGULARITY_BIND"] = f"{bindings_prefix}{','.join(singularity_bindings)}"

    logger.info("run_script: %s", ' '.join(cmd))
    has_failed = False
    with open(os.environ['__out_log'], 'a') as out:
        with open(os.environ['__err_log'], 'a') as err:
            with subprocess.Popen(
                cmd,
                stdout=out, stderr=err,
                env=env
            ) as p:
                try:
                    p.wait()
                    has_failed = p.returncode != 0
                except Exception as ex:
                    logger.exception(ex)
                    has_failed = True
                finally:
                    if has_failed:
                        step_number, control_dir, state_file, state_name = read_task_state()
                        _transition_state_file(state_file, "failed", step_number)
    if has_failed:
        _exit_process()


def launch_task_remote(task_key, is_slurm, wait_for_completion):

    pipeline_instance_dir = os.path.dirname(os.path.dirname(sys.argv[0]))

    control_dir = os.path.join(pipeline_instance_dir, '.drypipe', task_key)

    logger = create_task_logger(control_dir)

    out_sigs_dir = os.path.join(control_dir, 'out_sigs')
    touch(os.path.join(control_dir, 'output_vars'))

    work_dir = os.path.join(pipeline_instance_dir, 'publish', task_key)
    scratch_dir = os.path.join(work_dir, "scratch")

    for d in [work_dir, out_sigs_dir, scratch_dir]:
        Path(d).mkdir(exist_ok=True, parents=True)

    for state_file in glob.glob(os.path.join(control_dir, "state.*")):
        os.remove(state_file)

    touch(os.path.join(control_dir, 'state.launched.0'))

    env = os.environ

    if is_slurm:
        cmd = os.path.join(control_dir, 'sbatch-launcher.sh')
        if wait_for_completion:
            env = {**env, "SBATCH_EXTRA_ARGS": "--wait"}
            cmd = ["bash", "-c", cmd]
    else:
        if wait_for_completion:
            back_ground = ""
        else:
            back_ground = "&"
        scr = os.path.join(control_dir, 'task')
        cmd = ["nohup", "bash", "-c", f"python3 {scr} {back_ground}"]

    logger.info("launching from remote: %s", ' '.join(cmd))

    with open(os.path.join(control_dir, 'out.log'), 'w') as out:
        with open(os.path.join(control_dir, 'err.log'), 'w') as err:
            with subprocess.Popen(
                cmd,
                stdout=out,
                stderr=err,
                env=env
            ) as p:
                p.wait()
                print(str(p.returncode))


def launch_task(task_func, wait_for_completion):

    def task_func_wrapper():
        try:
            task_func()
            logger.info("task completed")
        except Exception as ex:
            logger.exception(ex)
        finally:
            _exit_process()

    if wait_for_completion:
        task_func_wrapper()
    else:
        if os.fork() != 0:
            exit(0)
        else:
            kill_script = os.path.join(os.environ['__script_location'], "kill")
            with open(kill_script, "w") as f:
                f.write("#!/bin/sh\n")
                f.write(f"kill -{signal.SIGTERM} {os.getpid()}\n")
            os.chmod(kill_script, 0o764)

            os.setpgrp()
            register_signal_handlers()
            Thread(target=task_func_wrapper).start()
            signal.pause()


def gen_input_var_exports(out=sys.stdout, env=os.environ):

    pipeline_instance_dir = env.get("__pipeline_instance_dir")

    task_key = env.get('__task_key')

    if pipeline_instance_dir is None:
        raise Exception("env variable __pipeline_instance_dir not set")

    if task_key is None:
        raise Exception("env variable __task_key not set")

    for producing_task_key, var_metas, file_metas in parse_in_out_meta({
        k: v
        for k, v in env.items()
        if k.startswith("__meta_")
    }):

        if producing_task_key == "":
            continue

        #control_dir = os.path.join(pipeline_instance_dir, ".drypipe", producing_task_key)

        #step_number, control_dir, state_file, state_name = read_task_state(control_dir, no_state_file_ok=True)

        #if not state_name == "completed":
        #    print(f"export __non_completed_dependent_task={producing_task_key}", file=out)
        #    return

        out_vars = dict(iterate_out_vars_from(
            os.path.join(pipeline_instance_dir, ".drypipe", producing_task_key, "output_vars")
        ))

        for name_in_producing_task, var_name, typez in var_metas:
            v = out_vars.get(name_in_producing_task)
            if v is not None:
                print(f"export {var_name}={v}", file=out)


def _root_dir(d):
    p = Path(d)
    return os.path.join(p.parts[0], p.parts[1])


def _fs_type(file):

    stat_cmd = f"stat -f -L -c %T {file}"
    with subprocess.Popen(
        stat_cmd.split(),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as p:
        p.wait()
        if p.returncode != 0:
            raise Exception(f"failed to stat for fs type: {stat_cmd}")
        return p.stdout.read().decode("utf-8").strip()


def set_singularity_bindings():

    def root_dir(d):
        p = Path(d)
        return os.path.join(p.parts[0], p.parts[1])

    pipeline_code_dir = os.environ['__pipeline_code_dir']
    root_of_pipeline_code_dir = root_dir(pipeline_code_dir)
    stat_cmd = f"stat -f -L -c %T {root_of_pipeline_code_dir}"
    with subprocess.Popen(
        stat_cmd.split(),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as p:
        p.wait()
        fs_type = p.stdout.read().decode("utf-8")
        if p.returncode != 0:
            raise Exception(f"failed to stat for fs type: {stat_cmd}")

        bind_list = []

        if fs_type in ["autofs", "nfs", "zfs"]:
            bind_list.append(f"{root_of_pipeline_code_dir}:{root_of_pipeline_code_dir}")

        if os.environ.get("__is_slurm"):
            scratch_dir = os.environ.get("__scratch_dir")
            root_of_scratch_dir = root_dir(scratch_dir)
            bind_list.append(f"{root_of_scratch_dir}:{root_of_scratch_dir}")

        prev_singularity_bind = os.environ.get("SINGULARITY_BIND")
        if prev_singularity_bind is not None:
            bind_list.append(prev_singularity_bind)

        os.environ['SINGULARITY_BIND'] = ",".join(bind_list)
