import argparse
import fnmatch
import glob
import importlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import textwrap
import logging
import logging.config
import time
import traceback
from datetime import datetime
from functools import reduce
from hashlib import blake2b
from pathlib import Path
from tempfile import TemporaryFile, NamedTemporaryFile
from threading import Thread
from typing import List, Iterator, Tuple

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

def _fail_safe_stderr(process):
    try:
        err = process.stderr.read().decode("utf-8")
    except Exception as _e:
        err = ""
    return err


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

    def iterate_stdout_lines(self):
        for line in self.popen.stdout:
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


def assert_slurm_supported_version():

    with PortablePopen(
        ["sbatch", "--version"]
    ) as p:

        p.wait()

        if p.popen.returncode != 0:
            return

        out = p.stdout_as_string()
        version_s = out.split()[1]

        version = version_s.split(".")

        if len(version) != 3:
            raise Exception(f"unrecognized slurm version: {out}")
        else:
            v1, v2, v3 = [int(v0) for v0 in version]
            if v1 < 21:
                raise Exception(f"unsupported sbatch version {version_s}")

        return v1, v2, v3


def python_shebang():
    return "#!/usr/bin/env python3"


def is_inside_slurm_job():
    return "SLURM_JOB_ID" in os.environ


def write_pipeline_lib_script(file_handle):
    file_handle.write(
        f"{python_shebang()}\n" + textwrap.dedent(f"""            
            import os
            import sys 
            import importlib.machinery
            import importlib.util        

            if "SLURM_JOB_ID" in os.environ:
                __script_location = os.path.dirname(os.environ["DRYPIPE_CONTROL_DIR"])
            else:
                __script_location = os.path.dirname(os.path.abspath(__file__))

            core_lib_path = os.path.join(__script_location, 'core_lib.py')        
            loader = importlib.machinery.SourceFileLoader('core_lib', core_lib_path)
            spec = importlib.util.spec_from_loader(loader.name, loader)
            core_lib = importlib.util.module_from_spec(spec)
            loader.exec_module(core_lib)
        """)
    )

    file_handle.write(textwrap.dedent(f"""        
    if __name__ == '__main__':        
        core_lib.handle_script_lib_main()
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
        core_lib_path = os.path.join(os.path.dirname(__script_location), 'core_lib.py')        
        loader = importlib.machinery.SourceFileLoader('core_lib', core_lib_path)
        spec = importlib.util.spec_from_loader(loader.name, loader)
        core_lib = importlib.util.module_from_spec(spec)
        loader.exec_module(core_lib)        
    """)

class UpstreamTasksNotCompleted(Exception):
    def __init__(self, upstream_task_key, msg):
        self.upstream_task_key = upstream_task_key
        self.msg = msg

class StateFile:

    def __init__(self, task_key, current_hash_code, tracker, path=None, slurm_array_id=None):
        self.tracker = tracker
        self.task_key = task_key
        if path is not None:
            self.path = path
        else:
            self.path = os.path.join(tracker.pipeline_work_dir, task_key, "state.waiting")
        self.hash_code = current_hash_code
        self.inputs = None
        self.outputs = None
        self.is_slurm_array_child = False
        if slurm_array_id is not None:
            self.slurm_array_id = slurm_array_id
        self.is_parent_task = False

    def __repr__(self):
        return f"/{self.task_key}/{os.path.basename(self.path)}"

    def refresh(self, new_path):
        assert self.path != new_path
        self.path = new_path
        if self.path.endswith("state.completed"):
            # load from file:
            self.outputs = None
            self.inputs = None

    def transition_to_pre_launch(self):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state._step-started")

    def transition_to_crashed(self):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state.crashed")

    def transition_to_ready(self):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state.crashed")

    def state_as_string(self):
        return os.path.basename(self.path)

    def is_completed(self):
        return self.path.endswith("state.completed")

    def is_failed(self):
        return fnmatch.fnmatch(self.path, "*/state.failed.*")

    def is_crashed(self):
        return fnmatch.fnmatch(self.path, "*/state.crashed.*")

    def is_timed_out(self):
        return fnmatch.fnmatch(self.path, "*/state.timed-out.*")

    def is_waiting(self):
        return self.path.endswith("state.waiting")

    def is_ready(self):
        return self.path.endswith("state.ready")

    def is_ready_or_passed(self):
        return not self.is_waiting()

    def is_in_pre_launch(self):
        return fnmatch.fnmatch(self.path, "*/state._step-started")

    def control_dir(self):
        return os.path.join(self.tracker.pipeline_work_dir, self.task_key)

    def output_dir(self):
        return os.path.join(self.tracker.pipeline_output_dir, self.task_key)

    def task_conf_file(self):
        return os.path.join(self.control_dir(), "task-conf.json")

    def load_task_conf_json(self):
        with open(self.task_conf_file()) as tc:
            return json.loads(tc.read())

    def touch_initial_state_file(self, is_ready):
        if is_ready:
            self.transition_to_ready()
        Path(self.path).touch(exist_ok=False)


class TaskProcess:

    @staticmethod
    def run(
        control_dir, as_subprocess=True, wait_for_completion=False,
        run_python_calls_in_process=False,
        array_limit=None,
        by_pipeline_runner=False
    ):

        if not as_subprocess:
            TaskProcess(
                control_dir,
                run_python_calls_in_process,
                as_subprocess=as_subprocess
            ).launch_task(wait_for_completion, exit_process_when_done=False, array_limit=array_limit)
        else:
            pipeline_work_dir = os.path.dirname(control_dir)
            pipeline_cli = os.path.join(pipeline_work_dir, "cli")
            if wait_for_completion:
                cmd = [pipeline_cli, "start", control_dir, "--wait"]
            else:
                cmd = [pipeline_cli, "start", control_dir]

            if by_pipeline_runner:
                cmd.append("--by-runner")

            logger.debug("will launch task %s", ' '.join(cmd))

            with PortablePopen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            ) as p:
                p.wait()
                if p.popen.returncode != 0:
                    logger.warning("task ended with non zero code: %s", p.popen.returncode)
                else:
                    logger.debug("task ended with returncode: %s", p.popen.returncode)

    def __init__(
        self, control_dir, run_python_calls_in_process=False, as_subprocess=True, ensure_all_upstream_deps_complete=True
    ):
        self.control_dir = control_dir
        self.task_key = os.path.basename(control_dir)
        self.pipeline_work_dir = os.path.dirname(control_dir)
        self.pipeline_instance_dir = os.path.dirname(self.pipeline_work_dir)
        self.pipeline_output_dir = os.path.join(self.pipeline_instance_dir, "output")
        self.task_output_dir = os.path.join(self.pipeline_output_dir, self.task_key)
        # For test cases:
        self.run_python_calls_in_process = run_python_calls_in_process
        self.as_subprocess = as_subprocess
        script_location = os.path.abspath(self.control_dir)
        with open(os.path.join(script_location, "task-conf.json")) as _task_conf:

            self.task_conf = json.loads(_task_conf.read())
            self.env = {}

            task_inputs, task_outputs = self._unserialize_and_resolve_inputs_outputs(ensure_all_upstream_deps_complete)

            self.inputs = TaskInputs(self.task_key, task_inputs)
            self.outputs = TaskOutputs(self.task_key, task_outputs)

            for k, v in self.iterate_task_env(False):
                v = str(v)
                logger.debug("env var %s = %s", k, v)
                self.env[k] = v

            command_before_task = self.task_conf.get("command_before_task")

            if command_before_task is not None:
                self.exec_cmd_before_launch(command_before_task)

    def __repr__(self):
        return f"{self.task_key}"

    def _exit_process(self):
        self._delete_pid_and_slurm_job_id()
        logging.shutdown()
        os._exit(0)

    def call_python(self, mod_func, python_task):

        task_logger = create_task_logger(self.control_dir)

        pythonpath_in_env = os.environ.get("PYTHONPATH")

        if pythonpath_in_env is not None:
            for p in pythonpath_in_env.split(":"):
                if not os.path.exists(p):
                    msg = f"WARNING: path {p} in PYTHONPATH does not exist, if running in apptainer, ensure proper mount"
                    print(msg, file=sys.stderr)
                    task_logger.warning(msg)

        inputs_by_name = {}
        file_outputs_by_name = {}
        # python_calls can access outputs of previous ones with input args
        var_outputs_by_name = {}

        for k, v in self.inputs._task_inputs.items():
            inputs_by_name[k] = v.resolved_value

        for o, k, f in self.outputs.iterate_file_task_outputs(self.task_output_dir):
            file_outputs_by_name[k] = f

        for o in self.outputs.iterate_non_file_outputs():
            v = os.environ.get(o.name)
            if v is not None:
                var_outputs_by_name[o.name] = o.parse(v)

        all_function_input_candidates = {
            ** os.environ,
            ** inputs_by_name,
            ** file_outputs_by_name,
            ** self._local_copy_adjusted_file_env_vars(),
            ** var_outputs_by_name
        }

        def get_arg(k):
            v = all_function_input_candidates.get(k)
            if v is None and k != "test":
                tk = self.task_key
                raise Exception(
                    f"Task {tk} called {mod_func} with None assigned to arg {k}\n" +
                    f"make sure task has var {k} declared in it's consumes clause. Ex:\n" +
                    f"  dsl.task(key={tk}).consumes({k}=...)"
                )
            return v

        args_tuples = [
            (k, get_arg(k))
            for k, v in python_task.signature.parameters.items()
            if not k == "kwargs"
        ]

        args = [v for _, v in args_tuples]
        args_names = [k for k, _ in args_tuples]
        task_logger.debug("args list: %s", args_names)

        if "kwargs" not in python_task.signature.parameters:
            kwargs = {}
        else:
            kwargs = {
                k : v
                for k, v in all_function_input_candidates.items()
                if k not in args_names
            }

        task_logger.info("will invoke PythonCall: %s(%s,%s)", mod_func, args, kwargs)

        try:
            out_vars = python_task.func(* args, ** kwargs)
        except Exception as ex:
            task_logger.exception(ex)
            logging.shutdown()
            exit(1)

        if out_vars is not None and not isinstance(out_vars, dict):
            raise Exception(
                f"function {python_task.mod_func()} called by task {self.task_key} {type(out_vars)}" +
                f"@DryPipe.python_call() can only return a python dict, or None"
            )

        try:
            if out_vars is not None:

                next_out_vars = dict(self.iterate_out_vars_from())

                wrong_type_errors = []

                for o in self.outputs.iterate_non_file_outputs():
                    v = out_vars.get(o.name)
                    if v is not None:
                        error_msg = o.ensure_valid_and_prescribed_type(v)
                        if error_msg is None:
                            next_out_vars[o.name] = v
                        else:
                            wrong_type_errors.append(f"{o.name}: {error_msg}")

                if len(wrong_type_errors) > 0:
                    raise Exception(f"function {mod_func} returned invalid types {wrong_type_errors}")

                self.write_out_vars(next_out_vars)

        except Exception as ex:
            traceback.print_exc()
            logging.shutdown()
            exit(1)

        logging.shutdown()

    def _unserialize_and_resolve_inputs_outputs(self, ensure_all_upstream_deps_complete):

        def resolve_upstream_and_constant_vars():

            for i in self.task_conf["inputs"]:
                i = TaskInput.from_json(i)
                if logger.level == logging.DEBUG:
                    logger.debug("%s", i.as_string())
                if i.is_upstream_output():

                    def ensure_upstream_task_is_completed():
                        for state_file in glob.glob(
                                os.path.join(self.pipeline_work_dir, i.upstream_task_key, "state.*")):
                            if not "completed" in state_file:
                                msg = f"upstream task {i.upstream_task_key} " + \
                                      f"not completed (state={state_file}), this task " + \
                                      f"dependency on {i.name_in_upstream_task} not satisfied"
                                raise UpstreamTasksNotCompleted(i.upstream_task_key, msg)

                    if ensure_all_upstream_deps_complete:
                        ensure_upstream_task_is_completed()

                    if i.is_file():
                        yield i, i.name, os.path.join(self.pipeline_output_dir, i.upstream_task_key, i.name_in_upstream_task)
                    else:
                        out_vars = dict(self.iterate_out_vars_from(
                            os.path.join(self.pipeline_work_dir, i.upstream_task_key, "output_vars")
                        ))
                        v = out_vars.get(i.name_in_upstream_task)
                        if v is not None:
                            v = i.parse(v)
                        yield i, i.name, v
                elif i.is_constant():
                    yield i, i.name, i.value
                elif i.is_file():
                    # not is_upstream_output means they have either absolute path, or in pipeline_instance_dir
                    if os.path.isabs(i.file_name):
                        yield i, i.name, i.file_name
                    else:
                        yield i, i.name, os.path.join(self.pipeline_instance_dir, i.file_name)

        var_file = os.path.join(self.control_dir, "output_vars")

        task_inputs = {}

        for task_input, k, v in resolve_upstream_and_constant_vars():
            task_inputs[k] = task_input
            if v is not None:
                task_input.resolved_value = v

        task_outputs = {}
        to = self.task_conf.get("outputs")
        if to is not None:
            unparsed_out_vars = dict(self.iterate_out_vars_from(var_file))
            for o in to:
                o = TaskOutput.from_json(o)
                if o.type != 'file':
                    v = unparsed_out_vars.get(o.name)
                    if v is not None:
                        o.set_resolved_value(v)
                    task_outputs[o.name] = o
                else:
                    o.set_resolved_value(os.path.join(self.pipeline_output_dir, self.task_key, o.produced_file_name))
                    task_outputs[o.name] = o

        return task_inputs, task_outputs

    def resolve_task(self, state_file):

        class ResolvedTask:
            def __init__(self, tp):
                self.key = state_file.task_key
                self.inputs = tp.inputs
                self.outputs = tp.outputs
                self.state_file = state_file

            def __str__(self):
                return f"Task(key={self.key})"

            def is_completed(self):
                return state_file.is_completed()

            def is_waiting(self):
                return state_file.is_waiting()

            def is_failed(self):
                return state_file.is_failed()

            def is_ready(self):
                return state_file.is_ready()

            def state_name(self):
                return state_file.state_as_string()

        return ResolvedTask(self)


    def iterate_out_vars_from(self, file=None):
        if file is None:
            file = self.env["__output_var_file"]
        if os.path.exists(file):
            with open(file) as f:
                for line in f.readlines():
                    var_name, value = line.split("=")
                    yield var_name.strip(), value.strip()

    def iterate_task_env(self, ensure_all_upstream_deps_complete):
        pipeline_conf = Path(self.pipeline_work_dir, "conf.json")
        if os.path.exists(pipeline_conf):
            with open(pipeline_conf) as pc:
                pipeline_conf_json = json.loads(pc.read())

                def en_vars_in_pipeline(*var_names):
                    for name in var_names:
                        value = pipeline_conf_json.get(name)
                        if value is not None:
                            value = os.path.expandvars(value)
                            yield name, value


                yield from en_vars_in_pipeline(
                    "__containers_dir",
                    "__pipeline_code_dir"
                )

        yield "__pipeline_instance_dir", self.pipeline_instance_dir
        yield "__pipeline_instance_name", os.path.basename(self.pipeline_instance_dir)
        yield "__control_dir", self.control_dir
        yield "__task_key", self.task_key
        yield "__task_output_dir", self.task_output_dir

        yield "__scratch_dir", self.resolve_scratch_dir()

        yield "__output_var_file", os.path.join(self.control_dir, "output_vars")
        yield "__out_log", os.path.join(self.control_dir, "out.log")

        # stderr defaults to stdout, by default
        yield "__err_log", os.path.join(self.control_dir, "out.log")

        if self.task_conf.get("ssh_specs") is not None:
            yield "__is_remote", "True"
        else:
            yield "__is_remote", "False"

        container = self.task_conf.get("container")
        if container is not None and container != "":
            yield "__is_singularity", "True"
        else:
            yield "__is_singularity", "False"

        logger.debug("extra_env vars from task-conf.json")

        extra_env = self.task_conf.get("extra_env")
        if extra_env is not None:
            for k, v in extra_env.items():
                yield k, os.path.expandvars(v)

        logger.debug("resolved and constant input vars")
        for k, v in self.inputs._task_inputs.items():
            yield k, v.resolved_value

        logger.debug("file output vars")
        for _, k, f in self.outputs.iterate_file_task_outputs(self.task_output_dir):
            yield k, f

    def resolve_scratch_dir(self):
        scratch_dir = os.environ.get('SLURM_TMPDIR')
        if scratch_dir is None:
            return os.path.join(self.task_output_dir, "scratch")
        else:
            return scratch_dir

    def run_python(self, mod_func, container=None):

        switches = "-u"
        cmd = [
            self.task_conf["python_bin"],
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
                self.resolve_container_path(container)
            ] + cmd

        env = self.env

        has_failed = False

        logger.info("run_python: %s", ' '.join(cmd))

        with open(env['__out_log'], 'a') as out:
            with open(env['__err_log'], 'a') as err:
                with PortablePopen(cmd, stdout=out, stderr=err, env={** os.environ, ** env}) as p:
                    try:
                        p.wait()
                        has_failed = p.popen.returncode != 0
                    except Exception as ex:
                        has_failed = True
                        logger.exception(ex)
                    finally:
                        if has_failed:
                            step_number, control_dir, state_file, state_name = self.read_task_state()
                            self._transition_state_file(state_file, "failed", step_number)
        if has_failed:
            self._exit_process()

    def _terminate_descendants_and_exit(self, p1, p2):

        try:
            try:
                logger.info("signal SIGTERM received, will transition to killed and terminate descendants")
                step_number, control_dir, state_file, state_name = self.read_task_state()
                self._transition_state_file(state_file, "killed", step_number)
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
            self._exit_process()


    def _delete_pid_and_slurm_job_id(self, sloc=None):
        try:
            if sloc is None:
                sloc = self.control_dir

            def delete_if_exists(f):
                f = os.path.join(sloc, f)
                if os.path.exists(f):
                    os.remove(f)

            delete_if_exists("pid")
            delete_if_exists("slurm_job_id")
        except Exception as ex:
            logger.exception(ex)


    def exec_cmd_before_launch(self, command_before_task):

        p = os.path.abspath(sys.executable)

        dump_with_python_script = f'{p} -c "import os, json; print(json.dumps(dict(os.environ)))"'

        logger.info("will execute 'command_before_task': %s", command_before_task)

        out = self.env['__out_log']
        err = self.env['__err_log']

        with PortablePopen([
            '/bin/bash', '-c', f"{command_before_task} 1>> {out} 2>> {err} && {dump_with_python_script}"
        ]) as p:
            p.wait_and_raise_if_non_zero()
            out = p.stdout_as_string()
            env = json.loads(out)
            for k, v in env.items():
                self.env[k] = v

    def read_task_state(self, control_dir=None, state_file=None):

        if control_dir is None:
            control_dir = self.env["__control_dir"]

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

    def _append_to_history(self, control_dir, state_name, step_number=None):
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


    def _transition_state_file(self, state_file, next_state_name, step_number=None, this_logger=logger):

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

        self._append_to_history(control_dir, next_state_name, step_number)

        return next_state_file, next_step_number


    def transition_to_step_started(self, state_file, step_number):
        return self._transition_state_file(state_file, "step-started", step_number)


    def transition_to_step_completed(self, state_file, step_number):
        state_file, step_number = self._transition_state_file(state_file, "step-completed", step_number)
        return state_file, step_number + 1


    def register_signal_handlers(self):

        def timeout_handler(s, frame):
            step_number, control_dir, state_file, state_name = self.read_task_state()
            self._transition_state_file(state_file, "timed-out", step_number)
            self._exit_process()

        logger.debug("will register signal handlers")

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)

        signal.signal(signal.SIGUSR1, timeout_handler)

        def f(p1, p2):
            self._terminate_descendants_and_exit(p1, p2)
        signal.signal(signal.SIGTERM, f)

        logger.debug("signal handlers registered")


    def transition_to_completed(self, state_file):
        return self._transition_state_file(state_file, "completed")


    def write_out_vars(self, out_vars):

        def serialize(v):
            if v is None:
                return 'null'
            else:
                return v

        all_vars = [
            f"{k}={serialize(v)}" for k, v in out_vars.items()
        ]

        output_vars = os.environ.get("__output_var_file")
        if output_vars is None:
            output_vars = self.env["__output_var_file"]

        with open(output_vars, "w") as f:
            f.write("\n".join(all_vars))

        logger.info("out vars written: %s", ",".join(all_vars))

    def resolve_container_path(self, container):

        containers_dir = self.env.get('__containers_dir')

        if containers_dir is None:
            containers_dir = os.path.join(self.env["__pipeline_code_dir"], "containers")

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

    def run_script(self, script, container=None):

        env = {
            ** self.env,
            ** self._local_copy_adjusted_file_env_vars()
        }

        script = os.path.join(
            env["__control_dir"],
            os.path.basename(script)
        )

        dump_env = f'python3 -c "import os, json; print(json.dumps(dict(os.environ)))"'
        out = env['__out_log']
        err = env['__err_log']

        cmd = ["bash", "-c", f". {script} 1>> {out} 2>> {err} ; {dump_env}"]

        if container is not None:
            cmd = [
                APPTAINER_COMMAND,
                "exec",
                self.resolve_container_path(container),
            ] + cmd

            apptainer_bindings = []

            root_dir_of_script = _root_dir(script)

            if _fs_type(root_dir_of_script) in ["autofs", "nfs", "zfs"]:
                apptainer_bindings.append(f"{root_dir_of_script}:{root_dir_of_script}")

            slurm_tmpdir = os.environ.get("SLURM_TMPDIR")
            if slurm_tmpdir is not None:
                root_of_scratch_dir = _root_dir(slurm_tmpdir)
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

            with PortablePopen(cmd, env={** os.environ, ** env}) as p:
                p.wait_and_raise_if_non_zero()
                out = p.stdout_as_string()
                step_output_vars = json.loads(out)
                task_output_vars = dict(self.iterate_out_vars_from())

                with open(os.path.join(env["__control_dir"], "task-conf.json")) as _task_conf:
                    task_conf_as_json = json.loads(_task_conf.read())
                    for o in task_conf_as_json["outputs"]:
                        o = TaskOutput.from_json(o)
                        if o.type != "file":
                            v = step_output_vars.get(o.name)
                            logger.debug("script exported output var %s = %s", o.name, v)
                            task_output_vars[o.name] = v
                            if v is not None:
                                self.env[o.name] = v

                self.write_out_vars(task_output_vars)


        except Exception as ex:
            logger.exception(ex)
            has_failed = True
        finally:
            if has_failed:
                step_number, control_dir, state_file, state_name = self.read_task_state()
                self._transition_state_file(state_file, "failed", step_number)
                self._exit_process()


    def _is_work_on_local_copy(self):
        work_on_local_copy = self.task_conf.get("work_on_local_file_copies")
        return work_on_local_copy is not None and work_on_local_copy

    def _local_inputs_root(self):
        return os.path.join(self.resolve_scratch_dir(), "local-input-files")

    def _local_outputs_root(self):
        return os.path.join(self.resolve_scratch_dir(), "local-output-files")

    def _local_copy_adjusted_file_env_vars(self):

        if not self._is_work_on_local_copy():
            return {}

        def gen():
            local_inputs = self._local_inputs_root()
            for var_name, file in self.inputs.rsync_external_file_list():
                yield var_name, os.path.join(local_inputs, file)

            for var_name, file in self.inputs.rsync_file_list_produced_upstream():
                yield var_name, os.path.join(local_inputs, file)

            local_outputs = self._local_outputs_root()
            for _, var_name, file in self.outputs.iterate_file_task_outputs(local_outputs):
                yield var_name, file

        return dict(gen())

    def _create_local_scratch_and_rsync_inputs(self):

        Path(self._local_inputs_root()).mkdir(exist_ok=True)
        Path(self._local_outputs_root()).mkdir(exist_ok=True)

        def files_included():
            for var_name, file in self.inputs.rsync_external_file_list():
                yield var_name, file

            for var_name, file in self.inputs.rsync_file_list_produced_upstream():
                yield var_name, file

        with NamedTemporaryFile("w", prefix="zzz") as tf:
            for _, fi in files_included():
                tf.write(fi)
                tf.write("\n")
            tf.flush()

            pid = self.pipeline_instance_dir

            invoke_rsync(f"rsync --files-from={tf.name} {pid}/ {self._local_inputs_root()}")

    def _rsync_outputs_from_scratch(self):
        invoke_rsync(
            f"rsync -a --dirs {self._local_outputs_root()}/ {self.pipeline_output_dir}/{self.task_key}"
        )

    def _run_steps(self):

        step_number, control_dir, state_file, state_name = self.read_task_state()
        step_invocations = self.task_conf["step_invocations"]

        if self._is_work_on_local_copy():
            self._create_local_scratch_and_rsync_inputs()

        for i in range(step_number, len(step_invocations)):

            step_invocation = step_invocations[i]
            state_file, step_number = self.transition_to_step_started(state_file, step_number)

            call = step_invocation["call"]

            if call == "python":
                if not self.run_python_calls_in_process:
                    self.run_python(step_invocation["module_function"], step_invocation.get("container"))
                else:
                    self._run_python_calls_in_process(step_invocation["module_function"])
            elif call == "bash":
                self.run_script(os.path.expandvars(step_invocation["script"]),
                           step_invocation.get("container"))
            else:
                raise Exception(f"unknown step invocation type: {call}")

            state_file, step_number = self.transition_to_step_completed(state_file, step_number)

        if self._is_work_on_local_copy():
            self._rsync_outputs_from_scratch()

        self.transition_to_completed(state_file)

    def _run_python_calls_in_process(self, module_function):
        python_task = func_from_mod_func(module_function)
        self.call_python(module_function, python_task)

    def run_array(self, limit):

        step_number, _, state_file, _ = self.read_task_state()
        ended_ok = False
        try:
            SlurmArrayParentTask(
                self.task_key,
                StateFileTracker(self.pipeline_instance_dir),
                self.task_conf,
                logger,
                mockup_run_launch_local_processes=not self.as_subprocess
            ).run_array(False, False, limit)
            ended_ok = True
        except Exception as ex:
            self._transition_state_file(state_file, "failed", step_number)
        finally:
            if ended_ok:
                self.transition_to_completed(state_file)

    def _sbatch_cmd_lines(self, wait_for_completion=False):

        if self.task_conf["executer_type"] != "slurm":
            raise Exception(f"not a slurm task")

        yield "sbatch"

        if wait_for_completion:
            yield "--wait"

        sacc = self.task_conf.get('slurm_account')
        if sacc is not None:
            yield f"--account={sacc}"

        yield f"--output={self.control_dir}/out.log"

        control_dir = os.environ["__script_location"]

        yield "--export={0}".format(",".join([f"DRYPIPE_CONTROL_DIR={control_dir}"]))
        yield "--signal=B:USR1@50"
        yield "--parsable"
        yield f"{self.pipeline_instance_dir}/.drypipe/cli"

    def _submit_sbatch_task(self, wait_for_completion):

        p = PortablePopen(
            list(self._sbatch_cmd_lines(wait_for_completion)),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )

        p.popen.wait()

        for l in p.read_stdout_lines():
            print(l)


    def launch_task(self, wait_for_completion, exit_process_when_done=True, array_limit=None):

        def task_func_wrapper():
            try:
                logger.debug("task func started")
                is_slurm_parent = self.task_conf.get("is_slurm_parent")
                if is_slurm_parent is not None and is_slurm_parent:
                    self.run_array(array_limit)
                else:
                    self._run_steps()
                logger.info("task completed")
            except Exception as ex:
                if not exit_process_when_done:
                    raise ex
                logger.exception(ex)
            finally:
                if exit_process_when_done:
                    self._exit_process()

        if wait_for_completion or not self.as_subprocess:
            task_func_wrapper()
        else:
            slurm_job_id = os.environ.get("SLURM_JOB_ID")
            is_slurm = slurm_job_id is not None
            if (not is_slurm) and os.fork() != 0:
                # launching process, die to let the child run in the background
                exit(0)
            else:
                # forked child, or slurm job
                sloc = os.environ['__script_location']
                if is_slurm:
                    sloc = _script_location_of_array_task_id_if_applies(sloc)

                    if sloc is None:
                        sloc = os.environ["DRYPIPE_CONTROL_DIR"]

                    logger.info("slurm job started, slurm_job_id=%s", slurm_job_id)
                    with open(os.path.join(sloc, "slurm_job_id"), "w") as f:
                        f.write(str(os.getpid()))
                else:
                    with open(os.path.join(sloc, "pid"), "w") as f:
                        f.write(str(os.getpid()))

                os.setpgrp()
                self.register_signal_handlers()
                Thread(target=task_func_wrapper).start()
                signal.pause()

    def archive_produced_files(self, task_output_dir, exclusion_glob_patterns):

        archive_tar_name = "drypipe-archive.tar.gz"
        archive_tar = os.path.join(task_output_dir, archive_tar_name)

        if os.path.exists(archive_tar):
            return False

        def matches_one_pattern(f_name):
            for p in exclusion_glob_patterns:
                if fnmatch.fnmatch(f_name, p):
                    return True

        def gen_to_archive():
            with os.scandir(task_output_dir) as files_in_output_dir:
                for f in files_in_output_dir:
                    if not (f.name == archive_tar_name or matches_one_pattern(f.name)):
                        yield f.name

        files_to_archive = list(gen_to_archive())

        to_delete = []

        with tarfile.open(archive_tar, "w:gz") as tar:
            for f in files_to_archive:
                f0 = os.path.join(task_output_dir, f)
                tar.add(f0, arcname=f)
                to_delete.append(f0)

        for f in to_delete:
            if os.path.isdir(f):
                shutil.rmtree(f)
            else:
                os.remove(f)

        return True


def handle_main():
    is_kill_command = "kill" in sys.argv
    is_tail_command = "tail" in sys.argv
    wait_for_completion = "--wait" in sys.argv
    is_ps_command = "ps" in sys.argv
    is_env = "env" in sys.argv
    is_with_exports = "--with-exports" in sys.argv

    is_run_command = not (is_tail_command or is_ps_command or is_kill_command or is_env)

    task_runner = TaskProcess(os.environ["__script_location"])

    if is_run_command:
        task_runner.launch_task(wait_for_completion)
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
            for k, v in task_runner.iterate_task_env():
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

def _script_location_of_array_task_id_if_applies(sloc):
    slurm_array_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if slurm_array_task_id is None:
        return slurm_array_task_id
    else:
        slurm_array_task_id = int(slurm_array_task_id)
        with open(os.path.join(sloc, "array_children_task_ids.tsv")) as _array_children_task_ids:
            c = 0
            for line in _array_children_task_ids:
                if c == slurm_array_task_id:
                    p = os.path.join(os.path.dirname(sloc), line.strip())
                    os.environ['__script_location'] = p
                    return p
                else:
                    c += 1

        msg = f"Error: no task_key for SLURM_ARRAY_TASK_ID={slurm_array_task_id}"
        raise Exception(msg)



def _root_dir(d):
    p = Path(d)
    return os.path.join(p.parts[0], p.parts[1])


def _fs_type(file):

    stat_cmd = f"stat -f -L -c %T {file}"
    with PortablePopen(stat_cmd.split()) as p:
        p.wait_and_raise_if_non_zero()
        return p.stdout_as_string().strip()


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


def handle_script_lib_main():

    cli_file = __file__

    def get__script_location_and_ensure_set():

        sloc = os.environ.get("__script_location")

        if sloc is not None:
            return sloc

        if is_inside_slurm_job():
            sloc = os.environ["DRYPIPE_CONTROL_DIR"]
        else:
            sloc = sys.argv[2]

        if not os.path.exists(sloc):
            sloc = os.path.join(os.path.pardir(cli_file), sloc)

            if os.path.exists(sloc):
                raise Exception(f"file not found {sloc}")

        sloc = os.path.abspath(sloc)

        os.environ["__script_location"] = sloc

        return sloc

    wait_for_completion = "--wait" in sys.argv

    if is_inside_slurm_job():
        cmd = "start"
    else:
        cmd = sys.argv[1]

    if cmd == "start" and "--by-runner" in sys.argv:
        cmd = "sbatch"

    if cmd == "start":
        task_runner = TaskProcess(get__script_location_and_ensure_set())
        task_runner.launch_task(wait_for_completion)
    elif cmd == "sbatch":
        task_runner = TaskProcess(get__script_location_and_ensure_set())
        task_runner._submit_sbatch_task(wait_for_completion)
    elif cmd == "sbatch-gen":
        task_runner = TaskProcess(get__script_location_and_ensure_set())
        print("#!/usr/bin/env bash\n")

        print(" \\\n".join(task_runner._sbatch_cmd_lines()))
    else:
        raise Exception('invalid args')

    logging.shutdown()


class FileCreationDefaultModes:
    pipeline_instance_directories = 0o774
    pipeline_instance_scripts = 0o774

def func_from_mod_func(mod_func):

    mod, func_name = mod_func.split(":")

    if not mod.startswith("."):
        module = importlib.import_module(mod)
    else:
        module = importlib.import_module(mod[1:])

    python_task = getattr(module, func_name, None)
    if python_task is None:
        raise Exception(f"function {func_name} not found in module {mod}")

    return python_task



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

        if type not in ['file', 'str', 'int', 'float', 'task-list']:
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
        self._cached_task_list_hash_codes = None
        self.resolved_value = None

    def _digest_if_task_list(self):
        if self._cached_task_list_hash_codes is None:
            d = blake2b(digest_size=20)
            for t in self.value:
                d.update(t.key.encode("utf8"))

            self._cached_task_list_hash_codes = d.hexdigest()

        return self._cached_task_list_hash_codes

    def hash_values(self):
        yield self.name
        yield self.type
        if self.type == 'task-list':
            yield self._digest_if_task_list()
        else:
            if self.name_in_upstream_task is not None:
                yield self.name_in_upstream_task
            if self.upstream_task_key is not None:
                yield str(self.upstream_task_key)
            if self.value is not None:
                yield str(self.value)
            if self.file_name is not None:
                yield str(self.file_name)


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

        #if self.file_name is None and self.type == 'file':
        #    raise Exception("!!")

        return self.type == 'file'
        #return self.file_name is not None

    def is_upstream_output(self):
        return self.upstream_task_key is not None

    def is_constant(self):
        return self.value is not None

    def _ref(self):
        return f"{self.upstream_task_key}/{self.name_in_upstream_task}"

    def as_json(self):

        d = self._dict().copy()
        d["name"] = self.name
        d["type"] = self.type

        if self.type == 'task-list':
            d["value"] = self._digest_if_task_list()

        return d

    def __int__(self):

        if self.type == "float":
            return int(self.resolved_value)

        if self.type != "int":
            raise Exception(f"Task input {self.name} is not of type int, actual type is: {self.type} ")

        return self.resolved_value

    def __float__(self):

        if self.type == "int":
            return float(self.resolved_value)

        if self.type != "float":
            raise Exception(f"Task input {self.name} is not of type float, actual type is: {self.type} ")

        return self.resolved_value

    def __str__(self):
        return str(self.resolved_value)



class TaskOutput:

    @staticmethod
    def from_json(json_dict):
        j = json_dict
        return TaskOutput(j["name"], j["type"], j.get("produced_file_name"))


    def as_string(self):
        p = "" if self.produced_file_name is None else f",{self.produced_file_name}"
        return f"TaskOutput({self.name},{self.type}{p})"

    def __init__(self, name, type, produced_file_name=None, task_key=None):
        if type not in ['file', 'str', 'int', 'float']:
            raise Exception(f"invalid type {type}")

        if type != 'file' and produced_file_name is not None:
            raise Exception(f"non file output can't have not None produced_file_name: {produced_file_name}")

        self.name = name
        self.type = type
        self.produced_file_name = produced_file_name
        self._resolved_value = None
        self.task_key = task_key

    def hash_values(self):
        yield self.name
        yield self.type
        if self.produced_file_name is not None:
            yield self.produced_file_name
        if self._resolved_value is not None:
            yield str(self._resolved_value)

    def set_resolved_value(self, v):
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
        if v == "null":
            return None
        elif self.type == "int":
            return int(v)
        elif self.type == "float":
            return float(v)

        return v

    def ensure_valid_and_prescribed_type(self, v):

        def expected_x_got_v(x, v):
            return f"expected {x} got {v}"

        if self.type == 'str':
            if not isinstance(v, str):
                return expected_x_got_v('str', v)
        elif self.type == 'int':
            if not isinstance(v, int):
                return expected_x_got_v('int', v)
        elif self.type == 'float':
            if not isinstance(v, float):
                return expected_x_got_v('float', v)

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

class TaskInputs:

    def __init__(self, task_key, task_inputs):
        self.task_key = task_key
        self._task_inputs = task_inputs

    def __iter__(self):
        yield from self._task_inputs.values()

    def as_json(self):
        return [
            o.as_json()
            for o in  self._task_inputs.values()
        ]

    def hash_values(self):
        for i in self._task_inputs.values():
            yield from i.hash_values()

    def __getattr__(self, name):

        p = self._task_inputs.get(name)

        if p is None:
            raise Exception(
                f"task {self.task_key} does not declare input '{name}' in it's consumes() clause.\n" +
                f"Use task({self.task_key}).consumes({name}=...) to specify input"
            )

        return p

    def rsync_file_list_produced_upstream(self):
        for i in self._task_inputs.values():
            if i.is_upstream_output() and i.type == 'file':
                yield i.name, f"output/{i.upstream_task_key}/{i.name_in_upstream_task}"

    def rsync_external_file_list(self):
        for i in self._task_inputs.values():
            if not i.is_upstream_output() and i.is_file():
                yield i.name, i.file_name

class TaskOutputs:

    def __init__(self, task_key, task_outputs):
        self.task_key = task_key
        self._task_outputs = task_outputs

    def hash_values(self):
        for o in self._task_outputs.values():
            yield from o.hash_values()

    def as_json(self):
        return [
            o.as_json()
            for o in  self._task_outputs.values()
        ]

    def __getattr__(self, name):

        p = self._task_outputs.get(name)

        if p is None:
            raise Exception(
                f"task {self.task_key} does not declare output '{name}' in it's outputs() clause.\n" +
                f"Use task({self.task_key}).outputs({name}=...) to specify outputs"
            )

        return p

    def iterate_non_file_outputs(self):
        for o in self._task_outputs.values():
            if not o.is_file():
                yield o

    def rsync_file_list(self):
        for o in self._task_outputs:
            if o.is_file():
                yield o.name, f"output/{self.task_key}/{o.produced_file_name}"

    def iterate_file_task_outputs(self, task_output_dir):
        for o in self._task_outputs.values():
            if o.is_file():
                yield o, o.name, os.path.join(task_output_dir, o.produced_file_name)

class SlurmArrayParentTask:

    def __init__(self, task_key, tracker, task_conf=None, logger=None, mockup_run_launch_local_processes=False):
        self.task_key = task_key
        self.debug = True
        self.task_conf = task_conf
        self.tracker = tracker
        self.mockup_run_launch_local_processes = mockup_run_launch_local_processes
        if logger is None:
            self.logger = logging.getLogger('log nothing')
            self.logger.addHandler(logging.NullHandler())
        else:
            self.logger = logger

    def control_dir(self):
        return os.path.join(self.tracker.pipeline_work_dir,  self.task_key)

    def task_keys_file(self):
        return os.path.join(self.control_dir(),  "task-keys.tsv")

    def children_task_keys(self):
        with open(self.task_keys_file()) as f:
            for line in f:
                yield line.strip()

    def prepare_sbatch_command(self, task_key_file, array_size):

        if array_size == 0:
            raise Exception(f"should not start an empty array")
        elif array_size == 1:
            array_arg = "0"
        else:
            array_arg = f"0-{array_size - 1}"

        return [
            "sbatch",
            f"--array={array_arg}",
            f"--account={self.task_conf.get('slurm_account')}",
            f"--output={self.control_dir}/out.log",
            f"--export={0}".format(",".join([
                f"DRYPIPE_CONTROL_DIR={self.control_dir()}",
                f"DRYPIPE_TASK_KEY_FILE_BASENAME={os.path.basename(task_key_file)}",
                f"DRYPIPE_TASK_DEBUG={self.debug}"
            ])),
            "--signal=B:USR1@50",
            "--parsable",
            f"{self.control_dir}/cli"
        ]

    def call_sbatch(self, command_args):
        cmd = " ".join(command_args)
        logging.info("will launch array: %s", cmd)
        with PortablePopen(cmd, shell=True) as p:
            p.wait_and_raise_if_non_zero()
            return p.stdout_as_string().strip()

    def call_squeue(self, submitted_job_ids):
        job_ids_as_str = ",".join(list(submitted_job_ids))
        # see JOB STATE CODES: https://slurm.schedmd.com/squeue.html
        with PortablePopen(f'squeue -r --format="%i %t" --states=all --jobs={job_ids_as_str}', shell=True) as p:
            p.wait_and_raise_if_non_zero()
            for line in p.iterate_stdout_lines():
                yield line

    def compare_and_reconcile_squeue_with_state_files(self, mockup_squeue_call=None):

        submitted_arrays_files = list(self.submitted_arrays_files_with_job_is_running_status())

        assumed_active_job_ids = {
            job_id
            for array_n, job_id, file, is_terminated in submitted_arrays_files
            if not is_terminated
        }

        if len(assumed_active_job_ids) == 0:
            return None

        job_ids_to_array_idx_to_squeue_state = self.call_squeue_and_index_response(
            assumed_active_job_ids, mockup_squeue_call
        )

        actual_active_job_ids = job_ids_to_array_idx_to_squeue_state.keys()

        ended_job_ids_since_last_call = assumed_active_job_ids - actual_active_job_ids

        """
        j1: t0, t1, t3
        j2:     t1
        """
        task_key_to_job_id_array_idx = {}

        for array_n, job_id, job_submission_file, is_assumed_terminated in submitted_arrays_files:
            if not is_assumed_terminated:
                for task_key, array_idx in self.task_keys_in_i_th_array_file(array_n):
                    task_key_to_job_id_array_idx[task_key] = (job_id, array_idx)

        dict_unexpected_states = {}

        for task_key, (job_id, array_idx) in task_key_to_job_id_array_idx.items():
            array_idx_to_squeue_state = job_ids_to_array_idx_to_squeue_state.get(job_id)
            squeue_state = None
            if array_idx_to_squeue_state is not None:
                squeue_state = array_idx_to_squeue_state.get(array_idx)
            unexpected_states = self.compare_and_reconcile_task_state_file_with_squeue_state(task_key, squeue_state)
            if unexpected_states is not None:
                dict_unexpected_states[task_key] = unexpected_states
                drypipe_state_as_string, expected_squeue_state, actual_queue_state = unexpected_states
                self.logger.warning(
                    "unexpected squeue state %s, expected %s for task %s",
                    actual_queue_state, expected_squeue_state, task_key
                )

        # flag new ended job_ids
        for array_n, job_id, job_submission_file, is_assumed_terminated in submitted_arrays_files:
            if not is_assumed_terminated:
                if job_id in ended_job_ids_since_last_call:
                    with open(job_submission_file, "a") as f:
                        f.write("COMPLETED\n")

        return dict_unexpected_states

    def call_squeue_and_index_response(self, submitted_job_ids, mockup_squeue_call=None) -> dict[str,dict[int,str]]:

        res: dict[str,dict[int,str]] = {}

        if self.mockup_run_launch_local_processes:
            squeue_func = lambda: []
        elif mockup_squeue_call is None:
            squeue_func = lambda: self.call_squeue(submitted_job_ids)
        else:
            squeue_func = lambda: mockup_squeue_call(submitted_job_ids)

        for line in squeue_func():
            line = line.strip()
            job_id_array_idx, squeue_state = line.split()
            job_id, array_idx = job_id_array_idx.split("_")

            array_idx_to_state_dict = res.get(job_id)
            if array_idx_to_state_dict is None:
                array_idx_to_state_dict = {}
                res[job_id] = array_idx_to_state_dict

            array_idx = int(array_idx)
            array_idx_to_state_dict[array_idx] = squeue_state
        return res

    def mock_compare_and_reconcile_squeue_with_state_files(self, mock_squeue_lines: List[str]):
        def mockup_squeue_call(submitted_job_ids):
            yield from mock_squeue_lines

        return self.compare_and_reconcile_squeue_with_state_files(mockup_squeue_call)

    def fetch_state_file(self, task_key):
        _, state_file = self.tracker.fetch_true_state_and_update_memory_if_changed(task_key)
        return state_file

    def compare_and_reconcile_task_state_file_with_squeue_state(self, task_key, squeue_state):
        state_file = self.fetch_state_file(task_key)
        drypipe_state_as_string = state_file.state_as_string()
        # strip "state."
        drypipe_state_as_string = drypipe_state_as_string[6:]
        if "." in drypipe_state_as_string:
            drypipe_state_as_string = drypipe_state_as_string.split(".")[0]

        # see JOB STATE CODES at https://slurm.schedmd.com/squeue.html

        if drypipe_state_as_string in ["completed", "failed", "killed", "timed-out"]:
            if squeue_state is not None:
                return drypipe_state_as_string, None, squeue_state
        elif drypipe_state_as_string in ["ready", "waiting"]:
            if squeue_state is not None:
                return drypipe_state_as_string, None, squeue_state
        elif drypipe_state_as_string.endswith("_step-started"):
            if squeue_state is None:
                self.tracker.transition_to_crashed(state_file)
                return drypipe_state_as_string,  "R,PD", None
            elif squeue_state not in ["R", "PD"]:
                return drypipe_state_as_string, "R,PD", squeue_state
        elif drypipe_state_as_string.endswith("step-started"):
            if squeue_state is None:
                self.tracker.transition_to_crashed(state_file)
                return drypipe_state_as_string,  "R,PD", None
            elif squeue_state not in ["R", "PD"]:
                return drypipe_state_as_string, "R,PD", squeue_state


    def arrays_files(self) -> Iterator[Tuple[int, str]]:

        def gen():
            for f in glob.glob(os.path.join(self.control_dir(), "array.*.tsv")):
                b = os.path.basename(f)
                idx = b.split(".")[1]
                idx = int(idx)
                yield idx, f

        yield from sorted(gen(), key= lambda t: t[0])

    def i_th_array_file(self, array_number):
        return os.path.join(self.control_dir(), f"array.{array_number}.tsv")

    def task_keys_in_i_th_array_file(self, array_number):
        with open(self.i_th_array_file(array_number)) as f:
            array_idx = 0
            for line in f:
                yield line.strip(), array_idx
                array_idx += 1

    def submitted_arrays_files(self) -> Iterator[Tuple[int, str, str]]:
        def gen():
            for f in glob.glob(os.path.join(self.control_dir(), "array.*.job.*")):
                b = os.path.basename(f)
                _, array_n, _, job_id = b.split(".")
                array_n = int(array_n.strip())
                yield array_n, job_id, f

        yield from sorted(gen(), key=lambda t: t[0])

    def submitted_arrays_files_with_job_is_running_status(self) -> Iterator[Tuple[int, str, str, bool]]:
        def s(file):
            with open(file) as _file:
                for line in _file:
                    if line.strip() == "COMPLETED":
                        return True
            return False

        for array_n, job_id, f in self.submitted_arrays_files():
            status = s(f)
            yield array_n, job_id, f, status

    def i_th_submitted_array_file(self, array_number, job_id):
        return os.path.join(self.control_dir(), f"array.{array_number}.job.{job_id}")

    def _iterate_next_task_state_files(self, start_next_n, restart_failed):
        i = 0
        for k in self.children_task_keys():
            state_file = self.tracker.load_state_file(k)
            if state_file.is_in_pre_launch():
                pass
            elif state_file.is_ready():
                yield state_file
                i += 1
            elif restart_failed and (state_file.is_failed() or state_file.is_timed_out()):
                self.tracker.register_pre_launch(state_file)
                yield state_file
                i += 1
            if start_next_n is not None and i >= start_next_n:
                break

    def prepare_and_launch_next_array(self, limit=None, restart_failed=False, call_sbatch_mockup=None):

        arrays_files = list(self.arrays_files())

        if len(arrays_files) == 0:
            next_array_number = 0
        else:
            last_array_file_idx = arrays_files[-1][0]
            next_array_number = last_array_file_idx + 1

        next_task_key_file = os.path.join(self.control_dir(), f"array.{next_array_number}.tsv")

        next_task_state_files = list(self._iterate_next_task_state_files(limit, restart_failed))

        if len(next_task_state_files) == 0:
            return 0
        else:
            with open(next_task_key_file, "w") as _next_task_key_file:
                for state_file in next_task_state_files:
                    _next_task_key_file.write(f"{state_file.task_key}\n")
                    self.tracker.register_pre_launch(state_file)

            command_args = self.prepare_sbatch_command(next_task_key_file, len(next_task_state_files))

            if call_sbatch_mockup is not None:
                call_sbatch_func = call_sbatch_mockup
            elif self.mockup_run_launch_local_processes:
                call_sbatch_func = lambda: self._sbatch_mockup_launch_as_local_proceses()
            else:
                call_sbatch_func = lambda: self.call_sbatch(command_args)

            job_id = call_sbatch_func()
            if job_id is None:
                raise Exception(f"sbatch returned None:\n {command_args}")
            with open(self.i_th_submitted_array_file(next_array_number, job_id), "w") as f:
                f.write(" ".join(command_args))

            return len(next_task_state_files)

    def _sbatch_mockup_launch_as_local_proceses(self):

        launch_idx, next_array_file = list(self.arrays_files())[-1]

        def gen_task_keys_to_launch():
            with open(next_array_file) as af:
                for line in af:
                    line = line.strip()
                    if line != "":
                        yield line

        for task_key in gen_task_keys_to_launch():
            TaskProcess.run(
                os.path.join(self.tracker.pipeline_work_dir, task_key),
                as_subprocess=True,
                wait_for_completion=True
            )

        return f"123400{launch_idx}"

    def run_array(self, restart_failed, reset_failed, limit):

        self.prepare_and_launch_next_array(limit)

        if self.mockup_run_launch_local_processes:
            return

        pause_in_seconds = [5, 5, 60, 600]
        pause_idx = 0
        max_idx = len(pause_in_seconds) - 1
        while self.compare_and_reconcile_squeue_with_state_files() is not None:
            time.sleep(pause_in_seconds[pause_idx])
            if pause_idx < max_idx:
                pause_idx += 1

        for task_key in self.children_task_keys():
            state_file = self.fetch_state_file(task_key)
            if not state_file.is_completed():
                raise Exception(f"at least one child task has not completed: {state_file.path}")


class StateFileTracker:

    def __init__(self, pipeline_instance_dir):
        self.pipeline_instance_dir = pipeline_instance_dir
        self.pipeline_work_dir = os.path.join(pipeline_instance_dir, ".drypipe")
        self.pipeline_output_dir = os.path.join(self.pipeline_instance_dir, "output")
        self.state_files_in_memory: dict[str, StateFile] = {}
        self.load_from_disk_count = 0
        self.resave_count = 0
        self.new_save_count = 0

    def instance_exists(self):
        return os.path.exists(self.pipeline_work_dir)

    def prepare_instance_dir(self, conf_dict):
        Path(self.pipeline_instance_dir).mkdir(
            exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)
        Path(self.pipeline_work_dir).mkdir(
            exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)
        Path(self.pipeline_instance_dir, "output").mkdir(
            exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)

        with open(Path(self.pipeline_work_dir, "conf.json"), "w") as conf_file:
            conf_file.write(json.dumps(conf_dict, indent=4))

        shutil.copy(__file__, self.pipeline_work_dir)

        script_lib_file = os.path.join(self.pipeline_work_dir, "cli")
        with open(script_lib_file, "w") as script_lib_file_handle:
            write_pipeline_lib_script(script_lib_file_handle)
        os.chmod(script_lib_file, FileCreationDefaultModes.pipeline_instance_scripts)


    def set_completed_on_disk(self, task_key):
        os.rename(
            self.state_files_in_memory[task_key].path,
            os.path.join(self.pipeline_work_dir, task_key, "state.completed")
        )

    def set_ready_on_disk_and_in_memory(self, task_key):
        p = os.path.join(self.pipeline_work_dir, task_key, "state.ready")
        os.rename(self.state_files_in_memory[task_key].path, p)
        self.state_files_in_memory[task_key].path = p

    def set_step_state_on_disk_and_in_memory(self, task_key, state_base_name):
        p = os.path.join(self.pipeline_work_dir, task_key, state_base_name)
        b4 = self.state_files_in_memory[task_key].path
        os.rename(b4, p)
        self.state_files_in_memory[task_key].path = p

    def transition_to_crashed(self, state_file):
        previous_path = state_file.path
        state_file.transition_to_crashed()
        os.rename(previous_path, state_file.path)

    def register_pre_launch(self, state_file):
        previous_path = state_file.path
        state_file.transition_to_pre_launch()
        os.rename(previous_path, state_file.path)

    def completed_task_keys(self):
        for k, state_file in self.state_files_in_memory.items():
            if state_file.is_completed():
                yield k

    def all_state_files(self):
        return self.state_files_in_memory.values()

    def lookup_state_file_from_memory(self, task_key):
        return self.state_files_in_memory[task_key]

    def _find_state_file_path_in_task_control_dir(self, task_key) -> str :
        try:
            with os.scandir(os.path.join(self.pipeline_work_dir, task_key)) as i:
                for f in i:
                    if f.name.startswith("state."):
                        return f.path
        except FileNotFoundError:
            pass
        return None

    def load_state_file(self, task_key, slurm_array_id=None):
        state_file_path = self._find_state_file_path_in_task_control_dir(task_key)
        if state_file_path is None:
            raise Exception(f"no state file exists in {os.path.join(self.pipeline_work_dir, task_key)}")
        state_file = StateFile(task_key, None, self, path=state_file_path, slurm_array_id=slurm_array_id)
        self.state_files_in_memory[task_key] = state_file
        return state_file

    def fetch_true_state_and_update_memory_if_changed(self, task_key):
        state_file = self.lookup_state_file_from_memory(task_key)
        if os.path.exists(state_file.path):
            return None, state_file
        else:
            state_file_path = self._find_state_file_path_in_task_control_dir(task_key)
            if state_file_path is None:
                raise Exception(f"no state file exists in {os.path.join(self.pipeline_work_dir, task_key)}")
            state_file.refresh(state_file_path)
            return state_file, state_file

    def _load_task_conf(self, task_control_dir):
        with open(os.path.join(task_control_dir, "task-conf.json")) as tc:
            return json.loads(tc.read())

    def load_from_existing_file_on_disc_and_resave_if_required(self, task, state_file_path):

        task_control_dir = os.path.dirname(state_file_path)
        task_key = os.path.basename(task_control_dir)
        pipeline_work_dir = os.path.dirname(task_control_dir)
        assert task.key == task_key
        task_conf = self._load_task_conf(task_control_dir)
        current_hash_code = task.compute_hash_code()
        state_file = StateFile(task_key, current_hash_code, self, path=state_file_path)
        if state_file.is_completed():
            pass
            #load inputs and outputs
        else:
            if task_conf["hash_code"] != state_file.hash_code:
                task.save(state_file, current_hash_code)
                state_file.hash_code = current_hash_code
                self.resave_count += 1
        self.load_from_disk_count += 1
        return state_file

    def _iterate_all_tasks_from_disk(self, glob_filter):
        with os.scandir(self.pipeline_work_dir) as pwd_i:
            for task_control_dir_entry in pwd_i:
                if not task_control_dir_entry.is_dir():
                    continue
                if task_control_dir_entry.name == "__pycache__":
                    continue
                task_control_dir = task_control_dir_entry.path
                task_key = os.path.basename(task_control_dir)

                if glob_filter is not None:
                    if not fnmatch.fnmatch(task_key, glob_filter):
                        continue

                state_file_dir_entry = self._find_state_file_path_in_task_control_dir(task_key)
                if state_file_dir_entry is None:
                    raise Exception(f"no state file exists in {task_control_dir_entry.path}")

                yield task_key, task_control_dir, state_file_dir_entry

    def _load_task_from_state_file(self, task_key, task_control_dir, state_file_path, include_non_completed):
        if state_file_path.endswith("state.completed") or include_non_completed:
            yield TaskProcess(
                task_control_dir, ensure_all_upstream_deps_complete= not include_non_completed
            ).resolve_task(
                StateFile(task_key, None, self, path=state_file_path)
            )

    def load_tasks_for_query(self, glob_filter=None, include_non_completed=False):
        for task_key, task_control_dir, state_file_dir_entry in self._iterate_all_tasks_from_disk(glob_filter):
            yield from self._load_task_from_state_file(
                task_key, task_control_dir, state_file_dir_entry, include_non_completed
            )

    def load_single_task_or_none(self, task_key, include_non_completed=False):
        task_control_dir = os.path.join(self.pipeline_work_dir, task_key)
        state_file_path = self._find_state_file_path_in_task_control_dir(task_key)
        if state_file_path is None:
            return None
        else:
            # update mem
            self.fetch_true_state_and_update_memory_if_changed(task_key)
            l = list(self._load_task_from_state_file(
                task_key, task_control_dir, state_file_path, include_non_completed
            ))
            c = len(l)
            if c == 0:
                return None
            elif c == 1:
                return l[0]
            else:
                raise Exception(f"expected zero or one task with key '{task_key}', got {c}")

    def load_state_files_for_run(self, glob_filter=None):
        for task_key, task_control_dir, state_file_path in self._iterate_all_tasks_from_disk(glob_filter):
            state_file = StateFile(
                task_key, None, self, path=state_file_path
            )
            self.state_files_in_memory[task_key] = state_file
            task_conf = self._load_task_conf(task_control_dir)
            if state_file.is_completed():
                yield True, state_file, None
            else:
                upstream_task_keys = set()
                for i in task_conf["inputs"]:
                    k = i.get("upstream_task_key")
                    if k is not None:
                        upstream_task_keys.add(k)
                yield False, state_file, upstream_task_keys

    def create_true_state_if_new_else_fetch_from_memory(self, task):
        """
        :return: (task_is_new, state_file)
        """
        state_file_in_memory = self.state_files_in_memory.get(task.key)
        if state_file_in_memory is not None:
            # state_file_in_memory is assumed up to date, since task declarations don't change between runs
            # by design, DAG generators that violate this assumption are considered at fault
            return False, state_file_in_memory
        else:
            # we have a new task OR process was restarted
            hash_code = task.compute_hash_code()
            state_file_path = self._find_state_file_path_in_task_control_dir(task.key)
            if state_file_path is not None:
                # process was restarted, task is NOT new
                state_file_in_memory = self.load_from_existing_file_on_disc_and_resave_if_required(task, state_file_path)
                self.state_files_in_memory[task.key] = state_file_in_memory
                task.save_if_hash_has_changed(state_file_in_memory, hash_code)
                return False, state_file_in_memory
            else:
                # task is new
                state_file_in_memory = StateFile(task.key, hash_code, self)
                state_file_in_memory.is_slurm_array_child = task.is_slurm_array_child
                if task.is_slurm_parent:
                    state_file_in_memory.is_parent_task = True
                task.save(state_file_in_memory, hash_code)
                state_file_in_memory.touch_initial_state_file(False)
                self.state_files_in_memory[task.key] = state_file_in_memory
                self.new_save_count += 1
                return True, state_file_in_memory



class Cli:

    def __init__(self, args, invocation_script=None, env={}):
        self.env = env
        self.parser = argparse.ArgumentParser(
            description="DryPipe CLI"
        )

        self.parser.add_argument(
            '--pipeline-instance-dir',
            help='pipeline instance directory, can also be set with environment var DRYPIPE_PIPELINE_INSTANCE_DIR',
            default=self.env.get("DRYPIPE_PIPELINE_INSTANCE_DIR")
        )
        self.parser.add_argument(
            '--verbose'
        )
        self.parser.add_argument(
            '--dry-run',
            help="don't actualy run, but print what will run (implicit --verbose)",
        )

        self._sub_parsers()

        self.parsed_args = self.parser.parse_args(args)


    def invoke(self, test_mode=False):

        if self.parsed_args.command == 'submit-array':
            TaskProcess.run(
                os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
                as_subprocess=not test_mode,
                array_limit=self.parsed_args.limit
            )
        elif self.parsed_args.command == 'run':
            pipeline = func_from_mod_func(self.parsed_args.generator)()
            pipeline_instance = pipeline.create_pipeline_instance(self.parsed_args.pipeline_instance_dir)
            pipeline_instance.run_sync(until_patterns=self.parsed_args.until)

    def _sub_parsers(self):


        self.subparsers = self.parser.add_subparsers(required=True, dest='command')
        self.add_run_args(self.subparsers.add_parser('run'))
        #self.add_run_args(self.subparsers.add_parser('status', parents=[self.parser]))
        self.add_array_args(self.subparsers.add_parser('submit-array'))

    def add_status_args(self):
        pass

    def add_run_args(self, run_parser):

        run_parser.add_argument(
            '--generator',
            help='<module>:<function> task generator function, can also be set with environment var DRYPIPE_PIPELINE_GENERATOR',
            metavar="GENERATOR",
            default=self.env.get("DRYPIPE_PIPELINE_GENERATOR")
        )

        run_parser.add_argument(
            '--until', help='tasks matching PATTERN will not be started',
            action='append',
            metavar='PATTERN'
        )


    def add_array_args(self, run_parser):
        run_parser.add_argument(
            '--filter',
            help=textwrap.dedent(
            """
            reduce the set of task that will run, with task-key match pattern: TASK_KEY(:STEP_NUMBER)?
            ex:
                --filter=my_taskABC
                --filter=my_taskABC:3            
            """)
        )

        run_parser.add_argument(
            '--limit', type=int, help='limit submitted array size to N tasks', metavar='N'
        )

        run_parser.add_argument(
            '--slurm-account'
        )

        run_parser.add_argument(
            '--task-key'
        )

        run_parser.add_argument(
            '--slurm-args',
            help="string that will be passed as argument to the sbatch invocation"
        )

        run_parser.add_argument(
            '--restart-at-step',
            help='task key',
        )

        run_parser.add_argument(
            '--restart-failed',
            action='store_true', default=False,
            help='re submit failed tasks in array, restart from last failed step, keep previous output'
        )

        run_parser.add_argument(
            '--reset-failed',
            action='store_true', default=False,
            help='delete and re submit failed tasks in array'
        )

