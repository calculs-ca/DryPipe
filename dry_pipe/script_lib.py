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
from functools import reduce
from itertools import groupby
from pathlib import Path
from threading import Thread


def create_task_logger(task_control_dir):
    if os.environ.get("DRYPIPE_TASK_DEBUG") != "True":
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG
    _logger = logging.Logger("task-logger", logging_level)
    file_handler = logging.FileHandler(filename=os.path.join(task_control_dir, "drypipe.log"))
    file_handler.setLevel(logging_level)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S%z')
    )
    _logger.addHandler(file_handler)
    return _logger


__script_location = os.environ.get("__script_location")


if __script_location is None:
    logger = logging.getLogger(__name__)
else:
    logger = create_task_logger(__script_location)


class PortablePopen:

    def __init__(self, process_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=None, shell=False, stdin=None):

        if stdout is None:
            raise Exception(f"stdout can't be None")

        if stderr is None:
            raise Exception(f"stderr can't be None")

        self.process_args = [str(p) for p in process_args]
        self.popen = subprocess.Popen(
            process_args,
            stdout=stdout,
            stderr=stderr,
            shell=shell,
            env=env,
            stdin=stdin
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.popen.__exit__(exc_type, exc_val, exc_tb)

    def wait(self, timeout=None):
        return self.popen.wait(timeout)

    def stderr_as_string(self):
        return self.popen.stderr.read().decode("utf8")

    def stdout_as_string(self):
        return self.popen.stdout.read().decode("utf8")

    def read_stdout_lines(self):
        for line in self.popen.stdout.readlines():
            yield line.decode("utf8")

    def safe_stderr_as_string(self):
        try:
            return self.stderr_as_string()
        except Exception as _:
            return ""

    def raise_if_non_zero(self):
        r = self.popen.returncode
        if r != 0:
            invocation = ' '.join(self.process_args)
            raise Exception(
                f"process invocation returned non zero {r}: {invocation}\nstderr: {self.safe_stderr_as_string()}"
            )

    def wait_and_raise_if_non_zero(self):
        self.popen.wait()
        self.raise_if_non_zero()


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
        script_lib.handle_script_lib_main()
    """))


def task_script_header():

    return f"{python_shebang()}\n" + textwrap.dedent(f"""            
        import os
        import sys 
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
    """)


def _exit_process():
    _delete_pid_and_slurm_job_id()
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
            with PortablePopen(cmd, stdout=out, stderr=err) as p:
                try:
                    p.wait()
                    has_failed = p.popen.returncode != 0
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

    try:
        try:
            logger.info("signal SIGTERM received, will transition to killed and terminate descendants")
            step_number, control_dir, state_file, state_name = read_task_state()
            _transition_state_file(state_file, "killed", step_number)
            logger.info("will terminate descendants")
        except Exception as _:
            pass

        this_pid = str(os.getpid())
        with PortablePopen(
            ['ps', '-opid', '--no-headers', '--ppid', this_pid]
        ) as p:
            p.wait_and_raise_if_non_zero()
            pids = [
                int(line.decode("utf-8").strip())
                for line in p.popen.stdout.readlines()
            ]
            pids = [pid for pid in pids if pid != p.popen.pid]
            logger.debug("descendants of %s: %s", this_pid, pids)
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception as _:
                    pass
    except Exception as ex:
        logger.exception(ex)
    finally:
        _exit_process()


def _delete_pid_and_slurm_job_id():
    try:
        sloc = os.environ["__script_location"]

        def delete_if_exists(f):
            f = os.path.join(sloc, f)
            if os.path.exists(f):
                os.remove(f)

        delete_if_exists("pid")
        delete_if_exists("slurm_job_id")
    except Exception as ex:
        logger.exception(ex)


def env_from_sourcing(env_file):

    p = os.path.abspath(sys.executable)

    dump_with_python_script = f'{p} -c "import os, json; print(json.dumps(dict(os.environ)))"'

    logger.debug("will source file: %s", env_file)

    with PortablePopen(['/bin/bash', '-c', f". {env_file} && {dump_with_python_script}"]) as p:
        p.wait_and_raise_if_non_zero()
        out = p.stdout_as_string()
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

    logger.debug("_transition_state_file: %s", state_file)

    control_dir = os.path.dirname(state_file)

    if step_number is None:
        next_step_number = None
        next_state_basename = f"state.{next_state_name}"
    else:
        next_step_number = step_number
        next_state_basename = f"state.{next_state_name}.{next_step_number}"

    next_state_file = os.path.join(control_dir, next_state_basename)

    logger.info("will transition to: %s", next_state_basename)
    logger.debug("next_state_file: %s", next_state_file)

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

    logger.debug("signal handlers registered")


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
        with PortablePopen(cmd.split(" ")) as p:
            p.wait_and_raise_if_non_zero()
            sha1sum = p.stdout_as_string()
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

    dump_env = f'python3 -c "import os, json; print(json.dumps(dict(os.environ)))"'
    out = os.environ['__out_log']
    err = os.environ['__err_log']

    cmd = ["bash", "-c", f". {script} 1> {out} 2> {err} && {dump_env}"]

    if container is not None:
        container_full_path = os.path.join(os.environ['__containers_dir'], container)
        cmd = [
            "singularity",
            "exec",
            container_full_path,
        ] + cmd

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

    logger.info("run_script: %s", cmd)

    has_failed = False
    try:

        with PortablePopen(cmd, env=env) as p:
            p.wait_and_raise_if_non_zero()
            out = p.stdout_as_string()
            output_vars = json.loads(out)

            with open(os.environ["__output_var_file"], "a") as out_vars:

                for producing_task_key, var_metas, file_metas in parse_in_out_meta({
                    k: v
                    for k, v in os.environ.items()
                    if k.startswith("__meta_")
                }):
                    if producing_task_key != "":
                        continue

                    for var_meta in var_metas:

                        name_in_producing_task, var_name, typez = var_meta
                        out_vars.write(f"{var_name}={output_vars.get(var_name)}\n")

    except Exception as ex:
        logger.exception(ex)
        has_failed = True
    finally:
        if has_failed:
            step_number, control_dir, state_file, state_name = read_task_state()
            _transition_state_file(state_file, "failed", step_number)
            _exit_process()


def launch_task_from_remote(task_key, is_slurm, wait_for_completion, drypipe_task_debug):

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
            env = {**env, "SBATCH_EXTRA_ARGS": "--wait", "DRYPIPE_TASK_DEBUG": str(drypipe_task_debug)}
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
            with PortablePopen(
                cmd,
                stdout=out,
                stderr=err,
                env=env
            ) as p:
                p.wait()
                if p.popen.returncode != 0:
                    logger.error(f"remote task launch failed: %s", sys.argv)
                print(str(p.popen.returncode))


def handle_main(task_func):
    is_kill_command = "kill" in sys.argv
    is_tail_command = "tail" in sys.argv
    wait_for_completion = "--wait" in sys.argv
    is_ps_command = "ps" in sys.argv

    is_run_command = not (is_tail_command or is_ps_command or is_kill_command)

    if is_run_command:
        _launch_task(task_func, wait_for_completion)
    else:

        def quit_command(p1, p2):
            logging.shutdown()
            exit(0)

        signal.signal(signal.SIGINT, quit_command)

        if is_tail_command:
            _tail_command()
        elif is_ps_command:
            _ps_command()
        elif is_kill_command:
            _kill_command()
        else:
            raise Exception(f"bad args: {sys.argv}")


def _load_pid_or_job_id(control_dir, pid_or_job_id):
    pid_file = os.path.join(control_dir, pid_or_job_id)
    if not os.path.exists(pid_file):
        return None
    else:
        with open(pid_file) as f:
            return int(f.read())


def load_pid(control_dir):
    return _load_pid_or_job_id(control_dir, "pid")


def load_slurm_job_id(control_dir):
    return _load_pid_or_job_id(control_dir, "slurm_job_id")


def print_pid_not_found(sloc):
    print(f"file {os.path.join(sloc, 'pid')} not found, task not running")


def _kill_command():
    sloc = os.environ["__script_location"]
    pid = load_pid(sloc)
    if pid is None:
        print_pid_not_found(sloc)
    else:
        os.kill(pid, signal.SIGTERM)


def _ps_command(pid=None):

    if pid is None:
        sloc = os.environ["__script_location"]
        pid = load_pid(sloc)
        if pid is None:
            print_pid_not_found(sloc)
            return

    res = list(ps_resources(pid))

    if len(res) == 0:
        return

    header, row = res

    sys.stdout.write("\t".join(header))
    sys.stdout.write("\n")
    sys.stdout.write("\t".join(str(v) for v in row))
    sys.stdout.write("\n")


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def ps_resources(pid):

    ps_format = "size,%mem,%cpu,rss,rsz,etimes"

    def parse_ps_line(r):
        size, pmem, pcpu, rss, rsz, etimes = r.split()
        return int(size), float(pmem), float(pcpu), int(rss), int(rsz), int(etimes)

    with PortablePopen(["ps", f"--pid={pid}", f"--ppid={pid}", "o", ps_format, "--no-header"]) as p:
        p.wait()
        if p.popen.returncode != 0:
            return iter([])

        def f(t1, t2):
            size1, pmem1, pcpu1, rss1, rsz1, etimes1 = t1
            size2, pmem2, pcpu2, rss2, rsz2, etimes2 = t2
            return (
               size1 + size2,
               pmem1 + pmem2,
               pcpu1 + pcpu2,
               rss1 + rss2,
               rsz1 + rsz2,
               max(etimes1, etimes2)
            )

        yield ps_format.split(",")

        out = p.stdout_as_string().strip()
        if out != "":
            yield list(reduce(
                f,
                [parse_ps_line(line) for line in out.split("\n")],
                (0, 0, 0, 0, 0, 0)
            ))


def _tail_command():

    script_location = os.environ["__script_location"]

    all_logs = [
        os.path.join(script_location, log)
        for log in ["drypipe.log", "out.log", "err.log"]
    ]

    tail_cmd = ["tail", "-f"] + all_logs

    with PortablePopen(tail_cmd, stdout=sys.stdout, stderr=sys.stderr, stdin=sys.stdin) as p:
        p.wait()


def _launch_task(task_func, wait_for_completion):

    def task_func_wrapper():
        try:
            logger.debug("task func started")
            task_func()
            logger.info("task completed")
        except Exception as ex:
            logger.exception(ex)
        finally:
            _exit_process()

    if wait_for_completion:
        task_func_wrapper()
    else:
        is_slurm = os.environ.get("__is_slurm") == "True"
        if (not is_slurm) and os.fork() != 0:
            exit(0)
        else:
            sloc = os.environ['__script_location']
            if is_slurm:
                slurm_job_id = os.environ.get("SLURM_JOB_ID")
                logger.info("slurm job started, slurm_job_id=%s", slurm_job_id)
                with open(os.path.join(sloc, "slurm_job_id"), "w") as f:
                    f.write(str(os.getpid()))
            else:
                with open(os.path.join(sloc, "pid"), "w") as f:
                    f.write(str(os.getpid()))

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

        # control_dir = os.path.join(pipeline_instance_dir, ".drypipe", producing_task_key)

        # step_number, control_dir, state_file, state_name = read_task_state(control_dir, no_state_file_ok=True)

        # if not state_name == "completed":
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
    with PortablePopen(stat_cmd.split()) as p:
        p.wait_and_raise_if_non_zero()
        return p.stdout_as_string().strip()


def set_singularity_bindings():

    def root_dir(d):
        p = Path(d)
        return os.path.join(p.parts[0], p.parts[1])

    pipeline_code_dir = os.environ['__pipeline_code_dir']
    root_of_pipeline_code_dir = root_dir(pipeline_code_dir)
    stat_cmd = f"stat -f -L -c %T {root_of_pipeline_code_dir}"
    with PortablePopen(stat_cmd.split()) as p:
        p.wait_and_raise_if_non_zero()
        fs_type = p.stdout_as_string()

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


def terminate_all():
    scancel_all()
    segterm_all()


def scancel_all():
    pass


def segterm_all():
    pass


def handle_script_lib_main():

    if "gen-input-var-exports" in sys.argv:
        gen_input_var_exports()
    elif "launch-task-from-remote" in sys.argv:
        task_key = sys.argv[2]
        is_slurm = "--is-slurm" in sys.argv
        wait_for_completion = "--wait-for-completion" in sys.argv
        drypipe_task_debug = "--drypipe-task-debug" in sys.argv
        launch_task_from_remote(task_key, is_slurm, wait_for_completion, drypipe_task_debug)
    elif "terminate-all" in sys.argv:
        terminate_all()
    elif "scancel-all" in sys.argv:
        terminate_all()
    elif "segterm-all" in sys.argv:
        segterm_all()
    else:
        raise Exception('invalid args')
