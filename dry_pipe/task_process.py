import fnmatch
import glob
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import traceback
import time
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Thread
from typing import List, Iterator, Tuple

from dry_pipe import TaskConf
from dry_pipe.core_lib import UpstreamTasksNotCompleted, PortablePopen, func_from_mod_func, StateFileTracker

from dry_pipe.task import TaskOutput, TaskInputs, TaskOutputs, TaskInput

#APPTAINER_COMMAND="apptainer"
APPTAINER_COMMAND="singularity"

APPTAINER_BIND="APPTAINER_BIND"

class RetryableRsyncException(Exception):
    def __init__(self, message):
        super().__init__(message)

class TaskProcess:

    def __init__(
        self,
            control_dir,
            run_python_calls_in_process=False,
            as_subprocess=True,
            ensure_all_upstream_deps_complete=True,
            no_logger=False,
            test_mode=False,
            wait_for_completion=False,
            tail=False,
            tail_all=False
    ):

        self.wait_for_completion = wait_for_completion
        self.record_history = False
        self.tail = tail
        self.tail_all = tail_all

        array_task_control_dir = self._script_location_of_array_task_id_if_applies(control_dir)
        if array_task_control_dir is not None:
            control_dir = array_task_control_dir
            os.environ["DRYPIPE_TASK_CONTROL_DIR"] = control_dir

        self.control_dir = control_dir

        if no_logger:
            self.task_logger = logging.getLogger('dummy')
        else:
            self.task_logger = self._create_task_logger(test_mode)

        try:

            self.task_key = os.path.basename(control_dir)
            self.pipeline_work_dir = os.path.dirname(control_dir)
            self.pipeline_instance_dir = os.path.dirname(self.pipeline_work_dir)

            if self.pipeline_instance_dir == "":
                raise Exception(f"pipeline_instance_dir can't be empty string")

            self.pipeline_output_dir = os.path.join(self.pipeline_instance_dir, "output")
            self.task_output_dir = os.path.join(self.pipeline_output_dir, self.task_key)
            # For test cases:
            self.test_mode = test_mode
            self.run_python_calls_in_process = run_python_calls_in_process
            self.as_subprocess = as_subprocess
            self.task_conf = TaskConf.from_json_file(self.control_dir)
            self.env = {}
            task_inputs, task_outputs = self._unserialize_and_resolve_inputs_outputs(ensure_all_upstream_deps_complete)

            self.inputs = TaskInputs(self.task_key, task_inputs)
            self.outputs = TaskOutputs(self.task_key, task_outputs)

            for k, v in self.iterate_task_env(False):
                v = str(v)
                self.task_logger.debug("env var %s = %s", k, v)
                self.env[k] = v

            command_before_task = self.task_conf.command_before_task

            if command_before_task is not None:
                self.exec_cmd_before_launch(command_before_task)
        except Exception as ex:
            if not no_logger:
                self.task_logger.exception(ex)
            raise ex

    def _create_task_logger(self, test_mode=False):

        #logging.getLogger().handlers.clear()

        if test_mode or os.environ.get("DRYPIPE_TASK_DEBUG") == "True":
            logging_level = logging.DEBUG
        else:
            logging_level = logging.INFO

        logger = logging.getLogger(f"task-logger-{os.path.basename(self.control_dir)}")


        logger.propagate = False

        logger.setLevel(logging_level)

        if len(logger.handlers) == 0:
            file_handler = logging.FileHandler(filename=os.path.join(self.control_dir, "drypipe.log"))
            file_handler.setLevel(logging_level)
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S%z')
            )
            logger.addHandler(file_handler)

        logger.info("log level: %s", logging.getLevelName(logging_level))
        return logger

    def __repr__(self):
        return f"{self.task_key}"

    def run(self, array_limit=None, by_pipeline_runner=False):

        if not self.as_subprocess:
            self.launch_task(array_limit=array_limit)
        else:
            pipeline_cli = os.path.join(self.pipeline_work_dir, "cli")
            if self.wait_for_completion:
                cmd = [pipeline_cli, "task", self.control_dir, "--wait"]
            else:
                cmd = [pipeline_cli, "task", self.control_dir]

            if by_pipeline_runner:
                cmd.append("--by-runner")

            with PortablePopen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            ) as p:
                p.wait()
                if p.popen.returncode != 0:
                    self.task_logger.warning(
                        "task ended with non zero code: %s, %s",
                        p.popen.returncode,
                        p.safe_stderr_as_string()
                    )
    def _exit_process(self):
        self._delete_pid_and_slurm_job_id()
        logging.shutdown()
        os._exit(0)

    def call_python(self, mod_func, python_task):

        pythonpath_in_env = os.environ.get("PYTHONPATH")

        if pythonpath_in_env is not None:
            for p in pythonpath_in_env.split(":"):
                if not os.path.exists(p):
                    msg = f"WARNING: path {p} in PYTHONPATH does not exist, if running in apptainer, ensure proper mount"
                    print(msg, file=sys.stderr)
                    self.task_logger.warning(msg)

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
                    f"make sure task has var {k} declared in it's inputs clause. Ex:\n" +
                    f"  dsl.task(key={tk}).inputs({k}=...)"
                )
            return v

        args_tuples = [
            (k, get_arg(k))
            for k, v in python_task.signature.parameters.items()
            if not k == "kwargs"
        ]

        args = [v for _, v in args_tuples]
        args_names = [k for k, _ in args_tuples]
        self.task_logger.debug("args list: %s", args_names)

        if "kwargs" not in python_task.signature.parameters:
            kwargs = {}
        else:
            kwargs = {
                k : v
                for k, v in all_function_input_candidates.items()
                if k not in args_names
            }

        func_log = f"{mod_func}({','.join(map(str, args))},{kwargs})"
        log_msg = f"will invoke PythonCall: {func_log}"
        self.task_logger.info(log_msg)

        with open(os.path.join(self.control_dir, "out.log"), mode="a") as out:
            out.write(f"================ {func_log} ====================")
            out.write(log_msg)
            out.write("=================================================\n")

        try:
            out_vars = python_task.func(* args, ** kwargs)
        except Exception as ex:
            self.task_logger.exception(ex)
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

            for i in self.task_conf.inputs:
                i = TaskInput.from_json(i)
                if self.task_logger.level == logging.DEBUG:
                    self.task_logger.debug("%s", i.as_string())
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
        to = self.task_conf.outputs
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

            def control_dir(self):
                return state_file.control_dir()

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

        if self.task_conf.ssh_remote_dest is not None:
            yield "__is_remote", "True"
        else:
            yield "__is_remote", "False"

        container = self.task_conf.container
        if container is not None and container != "":
            yield "__is_singularity", "True"
        else:
            yield "__is_singularity", "False"

        self.task_logger.debug("extra_env vars from task-conf.json")

        extra_env = self.task_conf.extra_env
        if extra_env is not None:
            for k, v in extra_env.items():
                yield k, os.path.expandvars(v)

        self.task_logger.debug("resolved and constant input vars")
        for k, v in self.inputs._task_inputs.items():
            yield k, v.resolved_value

        self.task_logger.debug("file output vars")
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
            self.task_conf.python_bin,
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

        self.task_logger.info("run_python: %s", ' '.join(cmd))

        with open(env['__out_log'], 'a') as out:
            with open(env['__err_log'], 'a') as err:
                with PortablePopen(cmd, stdout=out, stderr=err, env={** os.environ, ** env}) as p:
                    try:
                        p.wait()
                        has_failed = p.popen.returncode != 0
                    except Exception as ex:
                        has_failed = True
                        self.task_logger.exception(ex)
                    finally:
                        if has_failed:
                            step_number, control_dir, state_file, state_name = self.read_task_state()
                            self._transition_state_file(state_file, "failed", step_number)
        if has_failed:
            self._exit_process()

    def _terminate_descendants_and_exit(self, p1, p2):

        try:
            try:
                self.task_logger.info("signal SIGTERM received, will transition to killed and terminate descendants")
                step_number, control_dir, state_file, state_name = self.read_task_state()
                self._transition_state_file(state_file, "killed", step_number)
                self.task_logger.info("will terminate descendants")
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
                self.task_logger.debug("descendants of %s: %s", this_pid, pids)
                for pid in pids:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except Exception as _:
                        pass
        except Exception as ex:
            self.task_logger.exception(ex)
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
            self.task_logger.exception(ex)


    def exec_cmd_before_launch(self, command_before_task):

        p = os.path.abspath(sys.executable)

        dump_with_python_script = f'{p} -c "import os, json; print(json.dumps(dict(os.environ)))"'

        self.task_logger.info("will execute 'command_before_task': %s", command_before_task)

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


    def _transition_state_file(self, state_file, next_state_name, step_number=None):

        self.task_logger.debug("_transition_state_file: %s", state_file)

        control_dir = os.path.dirname(state_file)

        if step_number is None:
            next_step_number = None
            next_state_basename = f"state.{next_state_name}"
        else:
            next_step_number = step_number
            next_state_basename = f"state.{next_state_name}.{next_step_number}"

        next_state_file = os.path.join(control_dir, next_state_basename)

        self.task_logger.info("will transition to: %s", next_state_basename)
        self.task_logger.debug("next_state_file: %s", next_state_file)

        os.rename(
            state_file,
            next_state_file
        )

        if self.record_history:
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

        self.task_logger.debug("will register signal handlers")

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)

        signal.signal(signal.SIGUSR1, timeout_handler)

        def f(p1, p2):
            self._terminate_descendants_and_exit(p1, p2)
        signal.signal(signal.SIGTERM, f)

        self.task_logger.debug("signal handlers registered")


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

        self.task_logger.info("out vars written: %s", ",".join(all_vars))

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

        self.task_logger.debug("container: %s resolves to: %s", container, resolved_path)

        return resolved_path

    def run_script(self, script, container=None):

        def _root_dir(d):
            p = Path(d)
            return os.path.join(p.parts[0], p.parts[1])

        def _fs_type(file):

            stat_cmd = f"stat -f -L -c %T {file}"
            with PortablePopen(stat_cmd.split()) as p:
                p.wait_and_raise_if_non_zero()
                return p.stdout_as_string().strip()

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
                self.task_logger.info("APPTAINER_BIND not set")
            else:
                self.task_logger.info("APPTAINER_BIND=%s", new_bind)


        self.task_logger.info("run_script: %s", " ".join(cmd))

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
                            self.task_logger.debug("script exported output var %s = %s", o.name, v)
                            task_output_vars[o.name] = v
                            if v is not None:
                                self.env[o.name] = v

                self.write_out_vars(task_output_vars)


        except Exception as ex:
            self.task_logger.exception(ex)
            has_failed = True
        finally:
            if has_failed:
                step_number, control_dir, state_file, state_name = self.read_task_state()
                self._transition_state_file(state_file, "failed", step_number)
                self._exit_process()


    def _is_work_on_local_copy(self):
        work_on_local_copy = self.task_conf.work_on_local_file_copies
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

    def dependent_file_list(self):
        for var_name, file in self.inputs.rsync_external_file_list():
            yield file

        for var_name, file in self.inputs.rsync_file_list_produced_upstream():
            yield file

        for file in self.inputs.rsync_output_var_file_list_produced_upstream():
            yield file

    def invoke_rsync(self, command):
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

        with PortablePopen(command, shell=True) as p:
            p.popen.wait()
            if p.popen.returncode != 0:
                is_retryable, rsync_message = _rsync_error_codes(p.popen.returncode)
                msg = f"rsync failed: {rsync_message}, \n'{command}' \n{p.safe_stderr_as_string()}"
                if not is_retryable:
                    raise Exception(msg)
                else:
                    raise RetryableRsyncException(msg)

    def _create_local_scratch_and_rsync_inputs(self):

        Path(self._local_inputs_root()).mkdir(exist_ok=True)
        Path(self._local_outputs_root()).mkdir(exist_ok=True)

        with NamedTemporaryFile("w", prefix="zzz") as tf:
            for fi in self.dependent_file_list():
                tf.write(fi)
                tf.write("\n")
            tf.flush()

            pid = self.pipeline_instance_dir

            self.invoke_rsync(f"rsync --files-from={tf.name} {pid}/ {self._local_inputs_root()}")

    def _rsync_outputs_from_scratch(self):
        self.invoke_rsync(
            f"rsync -a --dirs {self._local_outputs_root()}/ {self.pipeline_output_dir}/{self.task_key}"
        )

    def _run_steps(self):

        step_number, control_dir, state_file, state_name = self.read_task_state()
        step_invocations = self.task_conf.step_invocations

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
                    module_function = step_invocation["module_function"]
                    python_task = func_from_mod_func(module_function)
                    self.call_python(module_function, python_task)
            elif call == "bash":
                self.run_script(os.path.expandvars(step_invocation["script"]),
                           step_invocation.get("container"))
            else:
                raise Exception(f"unknown step invocation type: {call}")

            state_file, step_number = self.transition_to_step_completed(state_file, step_number)

        if self._is_work_on_local_copy():
            self._rsync_outputs_from_scratch()

        self.transition_to_completed(state_file)

    def run_array(self, limit):

        step_number, _, state_file, _ = self.read_task_state()
        try:
            sapt = SlurmArrayParentTask(self)
            all_children_completed = sapt.run_array(False, False, limit)

            if all_children_completed:
                self.transition_to_completed(state_file)
        except Exception as ex:
            self.task_logger.exception(ex)
            self._transition_state_file(state_file, "failed", step_number)
            raise ex

    def _sbatch_cmd_lines(self):

        if self.task_conf.executer_type != "slurm":
            raise Exception(f"not a slurm task")

        yield "sbatch"

        if self.wait_for_completion:
            yield "--wait"

        sacc = self.task_conf.slurm_account
        if sacc is not None:
            yield f"--account={sacc}"

        yield f"--output={self.control_dir}/out.log"


        yield "--export={0}".format(",".join([f"DRYPIPE_TASK_CONTROL_DIR={self.control_dir}"]))
        yield "--signal=B:USR1@50"
        yield "--parsable"
        yield f"{self.pipeline_instance_dir}/.drypipe/cli"

    def submit_sbatch_task(self):

        p = PortablePopen(
            list(self._sbatch_cmd_lines()),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )

        p.popen.wait()

        for l in p.read_stdout_lines():
            print(l)


    def _script_location_of_array_task_id_if_applies(self, sloc):

        slurm_array_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
        if slurm_array_task_id is None:
            return None
        else:

            # TODO: consider geting info from SLURM_ARRAY_JOB_ID instead of DRYPIPE_TASK_KEY_FILE_BASENAME
            #slurm_array_job_id = os.environ.get("SLURM_ARRAY_JOB_ID")

            array_index_2_task_key = os.environ.get("DRYPIPE_TASK_KEY_FILE_BASENAME")

            def children_task_keys():
                with open(os.path.join(sloc, array_index_2_task_key)) as f:
                    for line in f:
                        yield line.strip()

            slurm_array_task_id = int(slurm_array_task_id)
            c = 0
            _drypipe_dir = os.path.dirname(sloc)

            for task_key in children_task_keys():
                if c == slurm_array_task_id:
                    p = os.path.join(_drypipe_dir, task_key)
                    os.environ['DRYPIPE_TASK_CONTROL_DIR'] = p
                    return p
                else:
                    c += 1

            msg = f"Error: no task_key for SLURM_ARRAY_TASK_ID={slurm_array_task_id}"
            raise Exception(msg)

    def exec_remote(self, user_at_host, cmd):
        with PortablePopen(["ssh", user_at_host, " ".join(cmd)]) as p:
            p.wait_and_raise_if_non_zero()
            return p.stdout_as_string()

    def launch_remote_task(self):

        user_at_host, remote_base_dire, ssh_key_file = self.task_conf.parse_ssh_remote_dest()

        remote_instance_work_dir = os.path.join(
            remote_base_dire,
            self.pipeline_instance_base_dir(),
            ".drypipe"
        )

        remote_task_control_dir = os.path.join(remote_instance_work_dir, self.task_key)

        remote_cli = os.path.join(remote_instance_work_dir, "cli")

        self.exec_remote(user_at_host, [
            remote_cli, "task", remote_task_control_dir
        ])

    def _launch_and_tail(self, launch_func):

        def _all_logs():
            if self.tail_all:
                yield "drypipe.log"
            yield "out.log"

        all_logs = [os.path.join(self.control_dir, log) for log in _all_logs()]

        tail_cmd = ["tail", "-f"] + all_logs

        def tail_func():
            def count_files_ready():
                c = 0
                for f in all_logs:
                    if os.path.exists(f):
                        c += 1
                return c

            while True:
                if count_files_ready() == len(all_logs):
                    break
                else:
                    time.sleep(1)

            with PortablePopen(tail_cmd, stdout=sys.stdout, stderr=sys.stderr, stdin=sys.stdin) as p:
                p.wait()

        launcher = Thread(target=launch_func)
        launcher.start()

        tail_thread = Thread(target=tail_func)
        tail_thread.start()

        launcher.join()


    def launch_task(self, array_limit=None):

        exit_process_when_done = self.as_subprocess
        def task_func_wrapper():
            try:
                self.task_logger.debug("task func started")
                if self.task_conf.ssh_remote_dest is not None:
                    self.launch_remote_task()
                else:
                    is_slurm_parent = self.task_conf.is_slurm_parent
                    if is_slurm_parent is not None and is_slurm_parent:
                        self.run_array(array_limit)
                    else:
                        self._run_steps()

                self.task_logger.info("task completed")
            except Exception as ex:
                if not exit_process_when_done:
                    raise ex
                self.task_logger.exception(ex)
            finally:
                if exit_process_when_done:
                    self._exit_process()

        if self.wait_for_completion or not self.as_subprocess:
            if self.tail or self.tail_all:
                self._launch_and_tail(task_func_wrapper)
            else:
                task_func_wrapper()
        else:
            slurm_job_id = os.environ.get("SLURM_JOB_ID")
            is_slurm = slurm_job_id is not None
            if (not is_slurm) and os.fork() != 0:
                # launching process, die to let the child run in the background
                exit(0)
            else:
                # forked child, or slurm job
                #sloc = os.environ['DRYPIPE_TASK_CONTROL_DIR']
                if is_slurm:
                    self.task_logger.info("slurm job started, slurm_job_id=%s", slurm_job_id)
                #else:
                #    with open(os.path.join(sloc, "pid"), "w") as f:
                #        f.write(str(os.getpid()))

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

    def pipeline_instance_base_dir(self):
        return os.path.basename(self.pipeline_instance_dir)


class SlurmArrayParentTask:

    def __init__(self, task_process, mockup_run_launch_local_processes=False):
        self.task_process = task_process
        self.tracker = StateFileTracker(pipeline_instance_dir=task_process.pipeline_instance_dir)
        self.pipeline_instance_dir = os.path.dirname(self.tracker.pipeline_work_dir)
        self.mockup_run_launch_local_processes = mockup_run_launch_local_processes
        self.debug = task_process.task_logger.isEnabledFor(logging.DEBUG)

    def control_dir(self):
        return os.path.join(self.tracker.pipeline_work_dir,  self.task_process.task_key)

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

        def sbatch_lines():
            yield "sbatch"
            yield f"--array={array_arg}"
            a = self.task_process.task_conf.slurm_account
            if a is not None:
                yield f"--account={a}"

            yield "--output=/dev/null"

            if self.debug:
                yield f"--error={self.control_dir()}/err-%A_%a.log"

            yield "--export={0}".format(",".join([
                f"DRYPIPE_TASK_CONTROL_DIR={self.control_dir()}",
                f"DRYPIPE_TASK_KEY_FILE_BASENAME={os.path.basename(task_key_file)}",
                f"DRYPIPE_TASK_DEBUG={self.debug}"
            ]))

            yield "--signal=B:USR1@50"
            yield "--parsable"
            yield f"{self.pipeline_instance_dir}/.drypipe/cli"

        return list(sbatch_lines())

    def call_sbatch(self, command_args):
        cmd = " ".join(command_args)
        with PortablePopen(cmd, shell=True) as p:
            p.wait_and_raise_if_non_zero()
            return p.stdout_as_string().strip()

    def call_squeue(self, submitted_job_ids):
        job_ids_as_str = ",".join(list(submitted_job_ids))
        # see JOB STATE CODES: https://slurm.schedmd.com/squeue.html
        squeue_cmd = f'squeue -r --noheader --format="%i %t" --states=all --jobs={job_ids_as_str}'
        self.task_process.task_logger.debug(squeue_cmd)
        with PortablePopen(squeue_cmd, shell=True) as p:
            #p.wait()
            # "stderr: slurm_load_jobs error: Invalid job id specified"
            #if self.popen.returncode != 0:
            #    p.safe_stderr_as_string()
            p.wait_and_raise_if_non_zero()
            for line in p.iterate_stdout_lines():
                yield line

    def _task_key_to_job_id_and_array_idx_map(self):

        submitted_arrays_files = list(self.submitted_arrays_files_with_job_is_running_status())

        job_id_array_idx_to_task_key = {}

        for array_n, job_id, job_submission_file, is_assumed_terminated in submitted_arrays_files:
            if not is_assumed_terminated:
                for task_key, array_idx in self.task_keys_in_i_th_array_file(array_n):
                    job_id_array_idx_to_task_key[(job_id, array_idx)] = task_key

        return job_id_array_idx_to_task_key

    def compare_and_reconcile_squeue_with_state_files(self, mockup_squeue_call=None):
        """
        Returns None when job has ended (completed or crashed, timed out, etc)
        or a dictionary:
            task_key -> (drypipe_state_as_string expected_squeue_state, actual_queue_state), ...}
        when the tasks state file has is not expected according to the task status returned by squeue.

        For example, the following response {'t1': ('_step-started', 'R,PD', None)}, means:

        task t1 is '_step-started' according to the state_file, BUT is 'R' (running) according to squeue,
        the expected squeue state should be 'PD' (pending).
        """

        submitted_arrays_files = list(self.submitted_arrays_files_with_job_is_running_status())

        assumed_active_job_ids = {
            job_id: array_n
            for array_n, job_id, file, is_terminated in submitted_arrays_files
            if not is_terminated
        }
        # 363:

        if len(assumed_active_job_ids) == 0:
            return None

        job_ids_to_array_idx_to_squeue_state = self.call_squeue_and_index_response(assumed_active_job_ids, mockup_squeue_call)
        # {'363': {3: 'CD', 2: 'CD', 1: 'CD', 0: 'CD'}}

        task_key_to_job_id_and_array_idx = self._task_key_to_job_id_and_array_idx_map()

        for job_id, array_idx_to_state_codes in job_ids_to_array_idx_to_squeue_state.items():
            def is_running_or_will_run(slurm_code):
                if slurm_code in {"PD", "R", "CG", "CF"}:
                    return True
                elif slurm_code in {"F", "CD", "TO", "ST", "PR", "RV", "SE", "BF", "CA", "DL", "OOM", "NF"}:
                    return False
                self.task_process.task_logger.warning("rare code: %s", slurm_code)
                return False

            is_running_or_will_run_count = 0

            for array_idx, state_code in array_idx_to_state_codes.items():
                if is_running_or_will_run(state_code):
                    is_running_or_will_run_count += 1
                else:
                    task_key = task_key_to_job_id_and_array_idx[(job_id, array_idx)]
                    latest_state = self.fetch_state_file(task_key)

                    if not latest_state.has_ended():
                        is_running_or_will_run_count += 1
                    # validate state_file

            if is_running_or_will_run_count == 0:
                for _, job_id_, job_submission_file, is_assumed_terminated in submitted_arrays_files:
                    if job_id_ == job_id:
                        if not is_assumed_terminated:
                            with open(job_submission_file, "a") as f:
                                f.write("\nBATCH_ENDED\n")
                            break
                        else:
                            self.task_process.task_logger.warning("submission %s already ended")

        u_states = self._validate_squeue_states_and_state_files(
            submitted_arrays_files, job_ids_to_array_idx_to_squeue_state
        )

        return u_states


    def _validate_squeue_states_and_state_files(self, submitted_arrays_files, job_ids_to_array_idx_to_squeue_state):

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
                self.task_process.task_logger.warning(
                    "unexpected squeue state '%s', expected '%s' for task '%s'",
                    actual_queue_state, expected_squeue_state, task_key
                )
        return dict_unexpected_states


    def call_squeue_and_index_response(self, submitted_job_ids, mockup_squeue_call=None):
        """
          job_id -> (array_idx, job_state_code)
        """
        res: dict[str,dict[int,str]] = {}

        if self.mockup_run_launch_local_processes:
            squeue_func = lambda: []
        elif mockup_squeue_call is None:
            squeue_func = lambda: self.call_squeue(submitted_job_ids)
        else:
            squeue_func = lambda: mockup_squeue_call(submitted_job_ids)

        for line in squeue_func():
            try:

                line = line.strip()
                job_id_array_idx, squeue_state = line.split()
                job_id, array_idx = job_id_array_idx.split("_")

                array_idx_to_state_dict = res.get(job_id)
                if array_idx_to_state_dict is None:
                    array_idx_to_state_dict = {}
                    res[job_id] = array_idx_to_state_dict

                array_idx = int(array_idx)
                array_idx_to_state_dict[array_idx] = squeue_state
            except Exception as ex:
                self.task_process.task_logger.error(f"failed while parsing squeue line: '%s'", line)
                raise ex
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

        self.task_process.task_logger.debug(
            "task_key=%s, drypipe_state_as_string=%s, squeue_state=%s ",
            task_key, drypipe_state_as_string, squeue_state
        )

        if drypipe_state_as_string in ["completed", "failed", "killed", "timed-out"]:
            if squeue_state != "CD" and squeue_state is not None:
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
                    if line.strip() == "BATCH_ENDED":
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
                self.task_process.task_logger.info("Will use SBATCH MOCKUP")
            elif self.mockup_run_launch_local_processes:
                self.task_process.task_logger.info("Will fake SBATCH as local process")
                call_sbatch_func = lambda: self._sbatch_mockup_launch_as_local_proceses()
            else:
                self.task_process.task_logger.info("will submit array: %s", " ".join(command_args))
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
            tp = TaskProcess(
                os.path.join(self.tracker.pipeline_work_dir, task_key),
                as_subprocess=True,
                wait_for_completion=True
            )
            tp.run()

        return f"123400{launch_idx}"

    def run_array(self, restart_failed, reset_failed, limit):

        self.prepare_and_launch_next_array(limit)

        if self.mockup_run_launch_local_processes:
            return

        self.task_process.task_logger.info("will run array %s", self.task_process.task_key)

        if self.debug:
            pause_in_seconds = [0, 0, 0, 0, 0, 1]
        else:
            pause_in_seconds = [2, 2, 3, 3, 4, 10, 30, 60, 120, 120, 180, 240, 300]

        pause_idx = 0
        max_idx = len(pause_in_seconds) - 1
        while True:
            res = self.compare_and_reconcile_squeue_with_state_files()
            if res is None:
                break
            next_sleep = pause_in_seconds[pause_idx]
            self.task_process.task_logger.debug("will sleep %s seconds", next_sleep)
            time.sleep(next_sleep)
            if pause_idx < max_idx:
                pause_idx += 1

        self.submitted_arrays_files_with_job_is_running_status()

        total_children_tasks = 0
        ended_tasks = 0
        completed_tasks = 0
        for task_key in self.children_task_keys():
            total_children_tasks += 1
            state_file = self.tracker.load_state_file(task_key)
            if state_file.has_ended():
                ended_tasks += 1
            if state_file.is_completed():
                completed_tasks += 1

        if completed_tasks == total_children_tasks:
            self.task_process.task_logger.info("array %s completed", self.task_process.task_key)
            return True

        return False

    def upload_array(self):

        task_key = self.task_process.task_key

        if self.task_process.task_conf.ssh_remote_dest is None:
            raise Exception(
                f"upload_array not possible for task {task_key}, " +
                f"requires ssh_remote_dest in TaskConf OR --ssh-remote-dest argument to be set"
            )

        def gen_dep_files_0():
            for child_task_key in self.children_task_keys():

                p = TaskProcess(
                    os.path.join(self.tracker.pipeline_work_dir, child_task_key),
                    ensure_all_upstream_deps_complete=True
                )

                yield from p.dependent_file_list()
                yield f".drypipe/{child_task_key}/state.ready"
                yield f".drypipe/{child_task_key}/task-conf.json"

                for step in p.task_conf.step_invocations:
                    if step["call"] == "bash":
                        _, script = step["script"].rsplit("/", 1)
                        yield f".drypipe/{child_task_key}/{script}"

            yield ".drypipe/cli"
            yield ".drypipe/dry_pipe/core_lib.py"
            yield ".drypipe/dry_pipe/__init__.py"
            yield ".drypipe/dry_pipe/cli.py"
            yield ".drypipe/dry_pipe/task_process.py"
            yield ".drypipe/dry_pipe/task.py"

            yield f".drypipe/{task_key}/task-conf.json"
            yield f".drypipe/{task_key}/task-keys.tsv"
            yield f".drypipe/{task_key}/state.ready"

        def gen_dep_files():
            pipeline_instance_name = os.path.basename(self.pipeline_instance_dir)
            for f in gen_dep_files_0():
                yield f"{pipeline_instance_name}/{f}"

        self.task_process.task_logger.info("will generate file list for upload")
        dep_file_txt = os.path.join(self.control_dir(), "deps.txt")
        uniq_files = set()
        with open(dep_file_txt, "w") as tf:
            for dep_file in gen_dep_files():
                if dep_file not in uniq_files:
                    tf.write(dep_file)
                    tf.write("\n")
                    uniq_files.add(dep_file)

        self.task_process.task_logger.info("done")

        user_at_host, remote_base_dire, ssh_key_file = self.task_process.task_conf.parse_ssh_remote_dest()

        ssh_remote_dest = f"{user_at_host}:{remote_base_dire}/"

        dn = os.path.abspath(os.path.dirname(self.task_process.pipeline_instance_dir))

        rsync_cmd = f"rsync --mkpath -a --dirs --files-from={dep_file_txt} {dn}/ {ssh_remote_dest}"
        self.task_process.task_logger.debug("%s", rsync_cmd)
        self.task_process.invoke_rsync(rsync_cmd)

    def download_array(self):
        def gen_result_files():
            for child_task_key in self.children_task_keys():
                p = TaskProcess(
                    os.path.join(self.tracker.pipeline_work_dir, child_task_key),
                    ensure_all_upstream_deps_complete=False
                )
                for file in p.outputs.rsync_file_list():
                    yield file

        result_file_txt = os.path.join(self.control_dir(), "result-files.txt")
        uniq_files = set()
        with open(result_file_txt, "w") as tf:
            for result_file in gen_result_files():
                if result_file not in uniq_files:
                    tf.write(result_file)
                    tf.write("\n")
                    uniq_files.add(result_file)

        rps = self.task_process.task_conf.remote_pipeline_specs(self.tracker.pipeline_instance_dir)

        pid = self.task_process.pipeline_instance_dir

        pipeline_base_name = os.path.basename(pid)

        ssh_remote_dest = \
            f"{rps.user_at_host}:{rps.remote_base_dir}/{pipeline_base_name}/"

        rsync_cmd = f"rsync -a --dirs --files-from={result_file_txt} {ssh_remote_dest} {pid}/"

        self.task_process.task_logger.debug("%s", rsync_cmd)
        self.task_process.invoke_rsync(rsync_cmd)

        remote_exec_result = self.task_process.exec_remote(rps.user_at_host, [
            rps.remote_cli,
            "list-array-states",
            f"--task-key={self.task_process.task_key}"
        ])

        for task_key_task_state in remote_exec_result.split("\n"):

            task_key_task_state = task_key_task_state.strip()

            if task_key_task_state == "":
                continue

            task_key, task_state = task_key_task_state.split("/")

            task_control_dir = os.path.join(self.task_process.pipeline_work_dir, task_key)

            state_file_path = StateFileTracker.find_state_file_if_exists(task_control_dir)

            if state_file_path is not None:
                actual_state = os.path.join(task_control_dir, task_state)
                os.rename(state_file_path.path, actual_state)

    @staticmethod
    def create_array_parent(pipeline_instance_dir, new_task_key, matcher, slurm_account, split_into):

        state_file_tracker = StateFileTracker(pipeline_instance_dir)

        control_dir = Path(state_file_tracker.pipeline_work_dir, new_task_key)

        control_dir.mkdir(exist_ok=True)

        not_ready_task_keys = []

        tc = TaskConf(
            executer_type="slurm",
            slurm_account=slurm_account
        )
        tc.is_slurm_parent = True
        tc.inputs.append({
            "upstream_task_key": None,
            "name_in_upstream_task": None,
            "file_name": None,
            "value": None,
            "name": "children_tasks",
            "type": "task-list"
        })
        tc.save_as_json(control_dir, "")

        with open(os.path.join(control_dir, "task-keys.tsv"), "w") as tc:
            for resolved_task in state_file_tracker.load_tasks_for_query(matcher, include_non_completed=True):

                if resolved_task.is_completed():
                    continue

                # ensure upstream dependencies are met
                task_process = TaskProcess(resolved_task.control_dir(), no_logger=True)
                task_process._unserialize_and_resolve_inputs_outputs(ensure_all_upstream_deps_complete=True)

                if not resolved_task.is_ready():
                    not_ready_task_keys.append(resolved_task.key)

                tc.write(resolved_task.key)
                tc.write("\n")

        if len(not_ready_task_keys) > 0:
            print(f"Warning: {len(not_ready_task_keys)} are not in 'ready' state:")

        Path(os.path.join(control_dir, "state.ready")).touch(exist_ok=True)

    def list_array_states(self):
        for child_task_key in self.children_task_keys():
            child_task_control_dir = os.path.join(self.task_process.pipeline_work_dir, child_task_key)

            state_file_path = StateFileTracker.find_state_file_if_exists(child_task_control_dir)
            if state_file_path is not None:
                yield child_task_key, state_file_path.name
