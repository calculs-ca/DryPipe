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

#APPTAINER_COMMAND="apptainer"
APPTAINER_COMMAND="singularity"

APPTAINER_BIND="APPTAINER_BIND"


def init_logger(_logger, filename, logging_level):
    file_handler = logging.FileHandler(filename=filename)
    file_handler.setLevel(logging_level)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S%z')
    )
    _logger.addHandler(file_handler)
    return _logger

def create_task_logger(task_control_dir):
    if os.environ.get("DRYPIPE_TASK_DEBUG") != "True":
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG
    _logger = logging.Logger("task-logger", logging_level)
    return init_logger(
        _logger,
        os.path.join(task_control_dir, "drypipe.log"),
        logging_level
    )


def create_remote_janitor_logger(pipeline_instance_dir, logger_name, is_debug):
    if is_debug:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO
    _logger = logging.Logger(logger_name, logging_level)
    return init_logger(
        _logger,
        os.path.join(pipeline_instance_dir, ".drypipe", f"{logger_name}.log"),
        logging_level
    )

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

        if isinstance(process_args, list):
            self.process_args = [str(p) for p in process_args]
        else:
            self.process_args = process_args

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
        i = 0
        try:
            s = self.popen.stderr.read()
            i = 1
            return s.decode("utf8")
        except Exception as e:
            return f"failed to capture process stderr ({i}): {e}"

    def raise_if_non_zero(self):
        r = self.popen.returncode
        if r != 0:
            raise Exception(
                f"process invocation returned non zero {r}: {self.process_args}\nstderr: {self.safe_stderr_as_string()}"
            )

    def wait_and_raise_if_non_zero(self, timeout=None):
        self.popen.wait(timeout)
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

    is_zombi_test_case = os.environ.get("IS_ZOMBI_TEST_CASE") == "True"
    bsheb = python_shebang() if not is_zombi_test_case else "#!/brokenshebang"

    return f"{bsheb}\n" + textwrap.dedent(f"""            
        import os 
        import importlib.machinery
        import importlib.util    
        import logging    

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

def load_task_conf_dict():
    script_location = os.environ["__script_location"]
    with open(os.path.join(script_location, "task-conf.json")) as _task_conf:
        task_conf_as_json = json.loads(_task_conf.read())

        set_generic_task_env(task_conf_as_json)

        command_before_task = task_conf_as_json.get("command_before_task")

        if command_before_task is not None:
            exec_cmd_before_launch(command_before_task)

        def override_deps(dep_file, bucket):
            dep_file = os.path.join(os.path.join(os.environ["__control_dir"], dep_file))
            if not os.path.exists(dep_file):
                return

            file_cache = os.path.join(task_conf_as_json["remote_base_dir"], "file-cache", bucket)

            logger.debug("will resolve external file deps: %s", file_cache)

            with open(dep_file) as f:
                deps = {l.strip() for l in f.readlines() if l != ""}
                for k, v in os.environ.items():
                    if v in deps:
                        if v.startswith("/"):
                            v = v[1:]
                        new_v = os.path.join(file_cache, v)
                        os.environ[k] = new_v
                        logger.debug("override var '%s' -> '%s'", k, new_v)

        override_deps("external-deps.txt", "shared")
        override_deps("external-deps-pipeline-instance.txt", os.environ["__pipeline_instance_name"])

        #for k, v in resolve_upstream_vars(task_conf_as_json):
        #    if v is not None:
        #        os.environ[k] = v

        return task_conf_as_json

def set_generic_task_env(task_conf_as_json):
    for k, v in iterate_task_env(task_conf_as_json):
        v = str(v)
        logger.debug("env var %s = %s", k, v)
        os.environ[k] = v

def iterate_task_env(task_conf_as_json=None, control_dir=None):

    if control_dir is None:
        control_dir = os.environ["__script_location"]

    if task_conf_as_json is None:
        with open(os.path.join(control_dir, "task-conf.json")) as _task_conf:
            task_conf_as_json = json.loads(_task_conf.read())

    pipeline_instance_dir = os.path.dirname(os.path.dirname(control_dir))
    task_key = os.path.basename(control_dir)

    yield "__pipeline_instance_dir", pipeline_instance_dir
    yield "__pipeline_instance_name", os.path.basename(pipeline_instance_dir)
    yield "__control_dir", control_dir
    yield "__task_key", task_key
    task_output_dir = os.path.join(pipeline_instance_dir, "output", task_key)
    yield "__task_output_dir", task_output_dir
    yield "__scratch_dir", os.path.join(task_output_dir, "scratch")
    yield "__output_var_file", os.path.join(control_dir, "output_vars")
    yield "__out_log", os.path.join(control_dir, "out.log")
    yield "__err_log", os.path.join(control_dir, "err.log")
    if task_conf_as_json.get("ssh_specs") is not None:
        yield "__is_remote", "True"
    else:
        yield "__is_remote", "False"

    container = task_conf_as_json.get("container")
    if container is not None and container != "":
        yield "__is_singularity", "True"
    else:
        yield "__is_singularity", "False"

    logger.debug("extra_env vars from task-conf.json")

    extra_env = task_conf_as_json["extra_env"]
    if extra_env is not None:
        for k, v in extra_env.items():
            yield k, os.path.expandvars(v)

    logger.debug("pipeline instance level vars")
    pipeline_overrides = os.path.join(pipeline_instance_dir, ".drypipe", "pipeline-env.sh")

    if os.path.exists(pipeline_overrides):
        env = _env_from_sourcing(pipeline_overrides)
        with open(pipeline_overrides) as f:
            for line in f:
                if line.startswith("export "):
                    line = line[7:].strip()
                    k, v = line.split("=")
                    yield k, env.get(k)

    logger.debug("resolved and constant input vars")
    for _, k, v in resolve_upstream_and_constant_vars(pipeline_instance_dir, task_conf_as_json):
        yield k, v

    logger.debug("file output vars")
    for _, k, f in iterate_file_task_outputs(task_conf_as_json, task_output_dir):
        yield k, f


def iterate_file_task_outputs(task_conf_as_json, task_output_dir):
    for o in task_conf_as_json["outputs"]:
        o = TaskOutput.from_json(o)
        if o.is_file():
            yield o, o.name, os.path.join(task_output_dir, o.produced_file_name)

class UpstreamTasksNotCompleted(Exception):
    def __init__(self, upstream_task_key, msg):
        self.upstream_task_key = upstream_task_key
        self.msg = msg

def resolve_upstream_and_constant_vars(
    pipeline_instance_dir, task_conf_as_json, log_error_if_upstream_task_not_completed=True
):

    for i in task_conf_as_json["inputs"]:
        i = TaskInput.from_json(i)
        if logger.level == logging.DEBUG:
            logger.debug("%s", i.as_string())
        if i.is_upstream_output():

            def ensure_upstream_task_is_completed():
                for state_file in glob.glob(os.path.join(pipeline_instance_dir, ".drypipe", i.upstream_task_key,"state.*")):
                    if not "completed" in state_file:
                        msg = f"upstream task {i.upstream_task_key} " + \
                            f"not completed (state={state_file}), this task " + \
                            f"dependency on {i.name_in_upstream_task} not satisfied"
                        if not log_error_if_upstream_task_not_completed:
                            logger.error(msg)
                        else:
                            raise UpstreamTasksNotCompleted(i.upstream_task_key, msg)

            ensure_upstream_task_is_completed()

            if i.is_file():
                yield i, i.name, os.path.join(pipeline_instance_dir, "output", i.upstream_task_key, i.file_name)
            else:
                out_vars = dict(iterate_out_vars_from(
                    os.path.join(pipeline_instance_dir, ".drypipe", i.upstream_task_key, "output_vars")
                ))
                v = out_vars.get(i.name_in_upstream_task)
                yield i, i.name, v
        elif i.is_constant():
            yield i, i.name, i.value
        elif i.is_file():
            # not is_upstream_output means they have either absolute path, or in pipeline_instance_dir
            if os.path.isabs(i.file_name):
                yield i, i.name, i.file_name
            else:
                yield i, i.name, os.path.join(pipeline_instance_dir, i.file_name)


def run_python(task_conf_dict, mod_func, container=None):

    switches = "-u"
    cmd = [
        task_conf_dict["python_bin"],
        switches,
        "-m",
        "dry_pipe.cli",
        "call",
        mod_func
    ]

    if container is not None:
        cmd = [
            APPTAINER_COMMAND,
            "exec",
            resolve_container_path(container)
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


def _delete_pid_and_slurm_job_id(sloc=None):
    try:
        if sloc is None:
            sloc = os.environ["__script_location"]

        def delete_if_exists(f):
            f = os.path.join(sloc, f)
            if os.path.exists(f):
                os.remove(f)

        delete_if_exists("pid")
        delete_if_exists("slurm_job_id")
    except Exception as ex:
        logger.exception(ex)


def _env_from_sourcing(env_file):

    p = os.path.abspath(sys.executable)

    dump_with_python_script = f'{p} -c "import os, json; print(json.dumps(dict(os.environ)))"'

    logger.debug("will source file: %s", env_file)

    with PortablePopen(['/bin/bash', '-c', f". {env_file} && {dump_with_python_script}"]) as p:
        p.wait_and_raise_if_non_zero()
        out = p.stdout_as_string()
        return json.loads(out)


def exec_cmd_before_launch(command_before_task):

    p = os.path.abspath(sys.executable)

    dump_with_python_script = f'{p} -c "import os, json; print(json.dumps(dict(os.environ)))"'

    logger.info("will execute 'command_before_task': %s", command_before_task)

    out = os.environ['__out_log']
    err = os.environ['__err_log']

    with PortablePopen([
        '/bin/bash', '-c', f"{command_before_task} 1>> {out} 2>> {err} && {dump_with_python_script}"
    ]) as p:
        p.wait_and_raise_if_non_zero()
        out = p.stdout_as_string()
        env = json.loads(out)
        for k, v in env.items():
            os.environ[k] = v


def iterate_out_vars_from(file):
    if os.path.exists(file):
        with open(file) as f:
            for line in f.readlines():
                var_name, value = line.split("=")
                yield var_name.strip(), value.strip()


def parse_in_out_meta(name_to_meta_dict):

    raise Exception(f"deprecated")

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

        produced_files_or_vars_meta = list(produced_files_or_vars_meta)

        def filter_and_map(condition_on_typez):
            return [
                (name_in_producing_task, var_name, typez)
                for task_key, name_in_producing_task, var_name, typez
                in produced_files_or_vars_meta
                if condition_on_typez(typez)
            ]

        yield producing_task_key, filter_and_map(lambda t: t != "file"), filter_and_map(lambda t: t == "file")


def read_task_state(control_dir=None, state_file=None):

    if control_dir is None:
        control_dir = os.environ["__control_dir"]

    if state_file is None:
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


def parse_ssh_specs(ssh_specs):
    ssh_specs_parts = ssh_specs.split(":")
    if len(ssh_specs_parts) == 2:
        ssh_username_ssh_host, key_filename = ssh_specs_parts
    elif len(ssh_specs_parts) == 1:
        ssh_username_ssh_host = ssh_specs_parts[0]
        key_filename = "~/.ssh/id_rsa"
    else:
        raise Exception(f"bad ssh_specs format: {ssh_specs}")

    ssh_username, ssh_host = ssh_username_ssh_host.split("@")

    return ssh_username, ssh_host, key_filename

def _rsync_with_args_and_remote_dir_ng(self, control_dir):
    with open(os.path.join(control_dir, "task-conf.json")) as tc:
        task_conf_as_json = json.loads(tc.read())
        ssh_username, ssh_host, key_filename = parse_ssh_specs(task_conf_as_json["ssh_specs"])
        remote_base_dir = task_conf_as_json["remote_base_dir"]
        ssh_args = f"-e 'ssh -q -i {key_filename} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'"
        timeout = f"--timeout={60 * 2}"
        return (
            f"rsync {ssh_args} {timeout}",
            f"{ssh_username}@{ssh_host}:{remote_base_dir}"
        )


def _transition_state_file(state_file, next_state_name, step_number=None, this_logger=logger):

    this_logger.debug("_transition_state_file: %s", state_file)

    control_dir = os.path.dirname(state_file)

    if step_number is None:
        next_step_number = None
        next_state_basename = f"state.{next_state_name}"
    else:
        next_step_number = step_number
        next_state_basename = f"state.{next_state_name}.{next_step_number}"

    next_state_file = os.path.join(control_dir, next_state_basename)

    this_logger.info("will transition to: %s", next_state_basename)
    this_logger.debug("next_state_file: %s", next_state_file)

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

    Path(sig_dir).mkdir(exist_ok=True, mode=FileCreationDefaultModes.pipeline_instance_directories)

    def all_files():
        for f in file_list_to_sign.split(","):
            if os.path.exists(f) and os.path.isfile(f):
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


def write_out_vars(out_vars):

    all_vars = [
        f"{k}={v}" for k, v in out_vars.items()
    ]

    with open(os.environ["__output_var_file"], "w") as f:
        f.write("\n".join(all_vars))

    logger.info("out vars written: %s", ",".join(all_vars))

def resolve_container_path(container):

    containers_dir = os.environ['__containers_dir']

    if os.path.isabs(container):
        if os.path.exists(container):
            resolved_path = container
        else:
            resolved_path = os.path.join(containers_dir, os.path.basename(container))
    else:
        resolved_path = os.path.join(containers_dir, container)

    if not os.path.exists(resolved_path):
        raise Exception(f"container file not found: {resolved_path}, __containers_dir={containers_dir}")

    logger.debug("container: %s resolves to: %s", container, resolved_path)

    return resolved_path

def run_script(script, container=None):

    env = {**os.environ}

    dump_env = f'python3 -c "import os, json; print(json.dumps(dict(os.environ)))"'
    out = os.environ['__out_log']
    err = os.environ['__err_log']

    cmd = ["bash", "-c", f". {script} 1>> {out} 2>> {err} ; {dump_env}"]

    if container is not None:
        cmd = [
            APPTAINER_COMMAND,
            "exec",
            resolve_container_path(container),
        ] + cmd

        apptainer_bindings = []

        root_dir_of_script = _root_dir(script)

        if _fs_type(root_dir_of_script) in ["autofs", "nfs", "zfs"]:
            apptainer_bindings.append(f"{root_dir_of_script}:{root_dir_of_script}")

        if os.environ.get("__is_slurm"):
            scratch_dir = os.environ['SLURM_TMPDIR']
            env['__scratch_dir'] = scratch_dir
            root_of_scratch_dir = _root_dir(scratch_dir)
            apptainer_bindings.append(f"{root_of_scratch_dir}:{root_of_scratch_dir}")

        if len(apptainer_bindings) > 0:

            prev_apptainer_bindings = env.get("APPTAINER_BIND")

            if prev_apptainer_bindings is not None and prev_apptainer_bindings != "":
                bindings_prefix = f"{prev_apptainer_bindings},"
            else:
                bindings_prefix = ""

            env["APPTAINER_BIND"] = f"{bindings_prefix}{','.join(apptainer_bindings)}"

        new_bind = env.get("APPTAINER_BIND")
        if new_bind is not None:
            logger.info("APPTAINER_BIND not set")
        else:
            logger.info("APPTAINER_BIND=%s", new_bind)


    logger.info("run_script: %s", " ".join(cmd))

    has_failed = False
    try:

        with PortablePopen(cmd, env=env) as p:
            p.wait_and_raise_if_non_zero()
            out = p.stdout_as_string()
            step_output_vars = json.loads(out)
            task_output_vars = dict(iterate_out_vars_from(os.environ["__output_var_file"]))

            with open(os.path.join(os.environ["__control_dir"], "task-conf.json")) as _task_conf:
                task_conf_as_json = json.loads(_task_conf.read())
                for o in task_conf_as_json["outputs"]:
                    o = TaskOutput.from_json(o)
                    if o.type != "file":
                        v = step_output_vars.get(o.name)
                        logger.debug("script exported output var %s = %s", o.name, v)
                        task_output_vars[o.name] = v
                        if v is not None:
                            os.environ[o.name] = v

            write_out_vars(task_output_vars)


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

    if drypipe_task_debug:
        os.environ["DRYPIPE_TASK_DEBUG"] = "True"

    logger = create_task_logger(control_dir)

    out_sigs_dir = os.path.join(control_dir, 'out_sigs')
    touch(os.path.join(control_dir, 'output_vars'))

    work_dir = os.path.join(pipeline_instance_dir, 'output', task_key)
    scratch_dir = os.path.join(work_dir, "scratch")

    for d in [work_dir, out_sigs_dir, scratch_dir]:
        Path(d).mkdir(exist_ok=True, parents=True, mode=FileCreationDefaultModes.pipeline_instance_directories)

    for state_file in glob.glob(os.path.join(control_dir, "state.*")):
        os.remove(state_file)

    touch(os.path.join(control_dir, 'state.launched.0'))

    _delete_pid_and_slurm_job_id(control_dir)

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

    logger.info("launching from remote: %s", ' '.join(cmd) if isinstance(cmd, list) else cmd)

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
    is_env = "env" in sys.argv
    is_with_exports = "--with-exports" in sys.argv

    is_run_command = not (is_tail_command or is_ps_command or is_kill_command or is_env)

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
        elif is_env:
            prefix = "export " if is_with_exports else ""
            for k, v in iterate_task_env():
                print(f"{prefix}{k}={v}")
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

def resolve_input_vars(pipeline_instance_dir, task_key):

    task_conf = os.path.join(pipeline_instance_dir, ".drypipe", task_key, "task-conf.json")
    with open(task_conf) as _task_conf:
        for _, k, v in resolve_upstream_and_constant_vars(pipeline_instance_dir, json.loads(_task_conf.read())):
            yield k, v


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

        prev_apptainer_bind = os.environ.get(APPTAINER_BIND)
        if prev_apptainer_bind is not None:
            bind_list.append(prev_apptainer_bind)

        os.environ['APPTAINER_BIND'] = ",".join(bind_list)

def _rsync_error_codes(code):
    """
    Error codes as documented here: https://download.samba.org/pub/rsync/rsync.1
    :param code rsync error code:
    :return (is_retryable, message):
    """
    if code == 1:
        return False, 'Syntax or usage error'
    elif code == 2:
        return False, 'Protocol incompatibility'
    elif code == 3:
        return True, 'Errors selecting input/output'
    elif code == 4:
        return False, 'Requested action not supported'
    elif code == 5:
        return True, 'Error starting client-serve'
    elif code == 6:
        return True, 'Daemon unable to append to log-file'
    elif code == 10:
        return True, 'Error in socket I/O'
    elif code == 11:
        return True, 'Error in file I/O'
    elif code == 12:
        return True, 'Error in rsync protocol data stream'
    elif code == 13:
        return True, 'Errors with program diagnostics'
    elif code == 14:
        return True, 'Error in IPC code'
    elif code == 20:
        return True, 'Received SIGUSR1 or SIGINT'
    elif code == 21:
        return True, 'Some error returned by waitpid()'
    elif code == 22:
        return True, 'Error allocating core memory'
    elif code == 23:
        return True, 'Partial transfer due to error'
    elif code == 24:
        return True, 'Partial transfer due to vanished source file'
    elif code == 25:
        return True, 'The --max-delete limit stopped deletions'
    elif code == 30:
        return True, 'Timeout in data send/received'
    elif code == 35:
        return True, 'Timeout waiting for daemon'
    else:
        return False, f'unknown error: {code}'

class RetryableRsyncException(Exception):
    def __init__(self, message):
        super().__init__(message)

def invoke_rsync(command):
    with PortablePopen(command, shell=True) as p:
        p.popen.wait()
        if p.popen.returncode != 0:
            is_retryable, rsync_message = _rsync_error_codes(p.popen.returncode)
            msg = f"rsync failed: {rsync_message}, \n'{command}' \n{p.safe_stderr_as_string()}"
            if not is_retryable:
                raise Exception(msg)
            else:
                raise RetryableRsyncException(msg)


def terminate_all():
    scancel_all()
    segterm_all()


def scancel_all():
    pass


def segterm_all():
    pass

def detect_slurm_crashes(user, is_debug):

    pipeline_instance_dir = os.path.dirname(os.path.dirname(sys.argv[0]))
    pid_base = os.path.basename(pipeline_instance_dir)

    janitor_logger = create_remote_janitor_logger(pipeline_instance_dir, "zombie-detector", is_debug)

    def iterate_all_expected_job_names_and_info():

        for state_file in glob.glob(os.path.join(pipeline_instance_dir, ".drypipe", "*", "state.*")):

            control_dir = os.path.dirname(state_file)
            state = os.path.basename(state_file).split(".")[1]

            if state not in ["launched", "scheduled", "step-started"]:
                continue

            if not os.path.exists(os.path.join(control_dir, "slurm_job_id")):
                continue

            task_key = os.path.basename(control_dir)

            yield f"{task_key}-{pid_base}", control_dir, state_file

    all_expected_job_names_and_info = list(iterate_all_expected_job_names_and_info())

    if len(all_expected_job_names_and_info) == 0:
        janitor_logger.debug("no expected slurm jobs at this time")
        return

    job_names_to_info = {
        job_name: (control_dir, state_file)
        for job_name, control_dir, state_file in all_expected_job_names_and_info
    }

    potential_slurm_zombies_names = job_names_to_info.keys()

    squeue_cmd = ['squeue', '--noheader', "--format=%j", f"--user={user}"]
    with PortablePopen(squeue_cmd) as p:
        p.wait_and_raise_if_non_zero()
        stdout = p.stdout_as_string()
        running_job_names = set([
            s
            for s in [s0.strip() for s0 in stdout.split("\n")]
            if s != ""
        ])
        janitor_logger.debug("%s returned jobs: %s", " ".join(squeue_cmd), running_job_names)

        for job_name in potential_slurm_zombies_names:
            if job_name not in running_job_names:

                control_dir, state_file = job_names_to_info[job_name]

                # prevent race condition
                if not os.path.exists(state_file):
                    janitor_logger.info("race condition detected for %s", state_file)
                    continue

                janitor_logger.debug("zombie job %s detected, in %s:", job_name, state_file)

                task_logger = create_task_logger(control_dir)

                try:
                    step_number, control_dir, state_file, state_name = read_task_state(control_dir)
                    _transition_state_file(state_file, "crashed", step_number, this_logger=task_logger)
                    print(f"WARNING: zombie slurm job_id={job_name} detected, in {control_dir}")
                except FileNotFoundError as ex:
                    janitor_logger.info(f"race condition encountered on %s", state_file)

def detect_process_crashes():

    def _scan_control_dirs(pipeline_instance_dir, filename):
        for f in glob.glob(os.path.join(pipeline_instance_dir, ".drypipe", "*", filename)):
            control_dir = os.path.dirname(f)
            state_file = list(glob.glob(os.path.join(control_dir, "state.*")))
            if len(state_file) == 0:
                continue
            with open(f) as _f:
                slurm_job_id_or_pid = _f.read().strip()
                if len(state_file) == 0:
                    yield slurm_job_id_or_pid, None, None, None
                else:
                    state = os.path.basename(state_file[0]).split(".")[1]
                    yield slurm_job_id_or_pid, state, control_dir, state_file[0]

    pipeline_instance_dir = os.path.dirname(os.path.dirname(sys.argv[0]))

    potential_process_zombies = {
        pid: (state, control_dir, state_file)
        for pid, state, control_dir, state_file in _scan_control_dirs(pipeline_instance_dir, "pid")
        if state in ["launched", "scheduled", "step-started"]
    }

    potential_zombies_pids = potential_process_zombies.keys()

    if len(potential_zombies_pids) > 0:
        pids = ','.join(potential_zombies_pids)
        with PortablePopen(["ps", f"--pid={pids}", "o", "pid", "--no-header"]) as p:
            p.wait_and_raise_if_non_zero()
            running_pids = set(p.stdout_as_string().split("\n"))
            crashed_pids = set(pids) - running_pids

            for pid in crashed_pids:
                state, control_dir, state_file = potential_process_zombies[pid]

                task_logger = create_task_logger(control_dir)

                step_number, control_dir, state_file, state_name = read_task_state()
                _transition_state_file(state_file, "crashed", step_number, this_logger=task_logger)

                print(f"WARNING: zombie tasks detected, pid: {pid}")


def handle_script_lib_main():

    if "gen-input-var-exports" in sys.argv:
        _deprecate_gen_input_var_exports()
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
    elif "detect-slurm-crashes" in sys.argv:
        user = sys.argv[2]
        detect_slurm_crashes(user, "--is-debug" in sys.argv)
        #detect_process_crashes()
    elif "detect-crashes" in sys.argv:
        user = sys.argv[2]
        detect_slurm_crashes(user, "--is-debug" in sys.argv)
        #detect_process_crashes()
    else:
        raise Exception('invalid args')

    logging.shutdown()


class FileCreationDefaultModes:
    pipeline_instance_directories = 0o774
    pipeline_instance_scripts = 0o774


class TaskInput:

    @staticmethod
    def from_json(json_string):
        j = json_string
        return TaskInput(
            j["name"],
            j["type"],
            upstream_task_key=j.get("upstream_task_key"),
            name_in_upstream_task=j.get("name_in_upstream_task"),
            file_name=j.get("file_name"),
            value=j.get("value")
        )

    def _dict(self):
        return {
            "upstream_task_key": self.upstream_task_key,
            "name_in_upstream_task": self.name_in_upstream_task,
            "file_name": self.file_name,
            "value": self.value
        }

    def as_string(self):

        def f(p):
            return f"TaskInput({self.name},{self.type},{p})"

        return f(
            ",".join([
                f"{k}={v}" for k, v in self._dict().items()
            ])
        )


    def __init__(self, name, type, upstream_task_key=None, name_in_upstream_task=None, file_name=None, value=None):

        if type not in ['file', 'str', 'int', 'float']:
            raise Exception(f"invalid type {type}")

        if type != 'file' and file_name is not None:
            raise Exception(f"inputs of type '{type}' can't have file_name: {file_name}")

        if value is not None:
            if name_in_upstream_task is not None or upstream_task_key is not None:
                raise Exception(f"invalid constant {name}")

        self.name = name
        self.type  = type
        self.name_in_upstream_task = name_in_upstream_task
        self.upstream_task_key = upstream_task_key
        self.value = value

        self.file_name = file_name

    def parse(self, v):
        if self.type == "int":
            return int(v)
        elif self.type == "float":
            return float(v)
        elif self.type == "glob_expression":
            class GlobExpression:
                def __call__(self, *args, **kwargs):
                    return glob.glob(os.path.expandvars(v))

                def __str__(self):
                    return os.path.expandvars(v)
            return GlobExpression()
        else:
            return v

    def is_file(self):
        return self.file_name is not None

    def is_upstream_output(self):
        return self.upstream_task_key is not None

    def is_constant(self):
        return self.value is not None

    def _ref(self):
        return f"{self.upstream_task_key}/{self.name_in_upstream_task}"

    def as_json(self):
        return {
            "name": self.name,
            "type":self.type,
            ** self._dict()
        }


class TaskOutput:

    @staticmethod
    def from_json(json_dict):
        j = json_dict
        return TaskOutput(j["name"], j["type"], j.get("produced_file_name"))


    def as_string(self):
        p = "" if self.produced_file_name is None else f",{self.produced_file_name}"
        return f"TaskOutput({self.name},{self.type}{p})"

    def __init__(self, name, type, produced_file_name=None):
        if type not in ['file', 'str', 'int', 'float']:
            raise Exception(f"invalid type {type}")

        if type != 'file' and produced_file_name is not None:
            raise Exception(f"non file output can't have not None produced_file_name: {produced_file_name}")

        self.name = name
        self.type = type
        self.produced_file_name = produced_file_name
        self._resolved_value = None

    def _set_resolved_value(self, v):
        self._resolved_value = self.parse(v)

    def is_file(self):
        return self.produced_file_name is not None

    def as_json(self):
        r = {
            "name": self.name,
            "type": self.type
        }

        if self.produced_file_name is not None:
            r["produced_file_name"] = self.produced_file_name

        return r

    def parse(self, v):
        if self.type == "int":
            return int(v)
        elif self.type == "float":
            return float(v)

        return v

    def __int__(self):

        if self.type == "float":
            return int(self._resolved_value)

        if self.type != "int":
            raise Exception(f"Task output {self.name} is not of type int, actual type is: {self.type} ")

        return self._resolved_value

    def __float__(self):

        if self.type == "int":
            return float(self._resolved_value)

        if self.type != "float":
            raise Exception(f"Task output {self.name} is not of type float, actual type is: {self.type} ")

        return self._resolved_value

    def __str__(self):
        return str(self._resolved_value)

    def __fspath__(self):

        if self.type != "file":
            raise Exception(f"Task output {self.name} is not of type file, actual type is: {self.type} ")

        return self._resolved_value
