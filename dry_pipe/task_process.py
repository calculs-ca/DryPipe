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

from dry_pipe import TaskConf
from dry_pipe.core_lib import UpstreamTasksNotCompleted, PortablePopen, func_from_mod_func, invoke_rsync, exec_remote, \
    FileCreationDefaultModes, expandvars_from_dict

from dry_pipe.task import TaskOutput, TaskInputs, TaskOutputs, TaskInput
from dry_pipe.task_lib import execute_remote_task

APPTAINER_COMMAND = "singularity"


module_logger = logging.getLogger(__name__)


class TaskFailedException (Exception):
    pass


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
            tail_all=False,
            is_python_call=False,
            from_remote=False
    ):

        self.wait_for_completion = wait_for_completion
        self.record_history = False
        self.tail = tail
        self.tail_all = tail_all
        self.has_ended = False
        self.as_subprocess = as_subprocess
        self.test_mode = test_mode
        self.run_python_calls_in_process = run_python_calls_in_process
        self.env = {}
        self.control_dir = control_dir
        self.task_key = os.path.basename(control_dir)
        self.pipeline_work_dir = os.path.dirname(control_dir)
        self.pipeline_instance_dir = os.path.dirname(self.pipeline_work_dir)
        self.pipeline_instance_name = os.path.basename(self.pipeline_instance_dir)
        self.pipeline_output_dir = os.path.join(self.pipeline_instance_dir, "output")
        self.task_output_dir = os.path.join(self.pipeline_output_dir, self.task_key)
        self.no_dynamic_steps = from_remote
        self.slurm_array_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")

        if not is_python_call:
            # override causes problems for python_call
            self._override_control_dir_if_child_task()

        try:
            self.task_conf = None
            self.task_conf = TaskConf.from_json_file(self.control_dir)
        except Exception as ex:
            # ensure this gets logged
            self._create_task_logger().exception(ex)
            raise ex

        if no_logger:
            self.task_logger = logging.getLogger('dummy')
        else:
            self.task_logger = self._create_task_logger()

        try:

            self.task_logger.debug("SLURM_ARRAY_TASK_ID: '%s'", self.slurm_array_task_id)

            self.task_conf = TaskConf.from_json_file(self.control_dir)

            self._override_task_confs_if_applicable()

            module_logger.debug(
                f"TaskProcess(%s, as_subprocess=%s, wait_for_completion=%s)",
                self.task_key, self.as_subprocess, self.wait_for_completion
            )
            self.task_logger.debug(f"pipeline_instance_dir %s", self.pipeline_instance_dir)

            if self.pipeline_instance_dir == "":
                raise Exception(f"pipeline_instance_dir can't be empty string")

            task_inputs, task_outputs = self._unserialize_and_resolve_inputs_outputs(ensure_all_upstream_deps_complete)

            self.inputs = TaskInputs(self.task_key, task_inputs)
            self.outputs = TaskOutputs(self.task_key, task_outputs)

            self.task_logger.debug(f"will iterate env")

            for k, v in self.iterate_task_env():
                v = str(v)
                self.task_logger.debug("env var %s = %s", k, v)
                self.env[k] = v

            self.task_logger.debug(f"done iterating env")

            command_before_task = self.task_conf.command_before_task

            if command_before_task is not None:
                self.exec_cmd_before_launch(command_before_task)

            self.task_logger.debug(f"normal TaskProcess constructor end")
        except Exception as ex:
            if not no_logger:
                self.task_logger.exception(ex)
            raise ex
        finally:
            self.task_logger.debug(f"TaskProcess constructor finally")

    def launched_from_cli_with_tail(self):
        return self.tail or self.tail_all

    def drypipe_log_file(self):
        return os.path.join(self.control_dir, "drypipe.log")

    def is_debug(self):
        if self.test_mode:
            return True
        if os.environ.get("DRYPIPE_TASK_DEBUG") == "True":
            return True
        if self.task_conf is not None:
            if self.task_conf.extra_env is not None:
                if self.task_conf.extra_env.get("DRYPIPE_TASK_DEBUG") == "True":
                    return True
        return False

    def _create_task_logger(self):

        if self.is_debug():
            logging_level = logging.DEBUG
        else:
            logging_level = logging.INFO

        logger = logging.getLogger(f"task-logger-{os.path.basename(self.control_dir)}")
        logger.setLevel(logging_level)

        if len(logger.handlers) == 0:
            file_handler = logging.FileHandler(filename=self.drypipe_log_file())
            file_handler.setLevel(logging_level)
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S%z')
            )
            logger.addHandler(file_handler)

            if self.tail_all:
                h = logging.StreamHandler(sys.stdout)
                h.setLevel(logging_level)
                h.setFormatter(
                    logging.Formatter(
                        "drypipe.log - %(asctime)s - %(levelname)s - %(message)s",
                        datefmt='%H:%M:%S%z'
                    )
                )
                logger.addHandler(h)

        logger.info("log level: %s", logging.getLevelName(logging_level))
        return logger

    def __repr__(self):
        return f"{self.task_key}"

    def _override_task_confs_if_applicable(self):
        self._override_task_conf_from_json(os.path.join(self.control_dir, "task-conf-overrides.json"))
        if self.is_array_child_task():
            array_parent_dir = self._array_parent_control_dir()
            self.task_logger.debug(f"task is array child task of parent task %s", array_parent_dir)
            self._override_task_conf_from_json(os.path.join(array_parent_dir, "task-conf-overrides.json"))

    def _override_task_conf_from_json(self, overrides_file):
        if not os.path.exists(overrides_file):
            self.task_logger.debug("no overrides in %s", overrides_file)
            return
        else:
            self.task_logger.debug("will apply overrides from %s", overrides_file)
            with open(overrides_file) as f:
                o = json.load(f)
                external_files_root = o.get("external_files_root")
                if external_files_root is not None:
                    self.task_conf.external_files_root = external_files_root
                    self.task_logger.debug(
                        "task_conf.external_files_root overriden %s", self.task_conf.external_files_root
                    )
                is_on_remote_site = o.get("is_on_remote_site")
                if is_on_remote_site is not None and is_on_remote_site:
                    self.task_conf.is_on_remote_site = True
                    self.task_logger.debug(
                        "task_conf.is_on_remote_site overriden %s", self.task_conf.is_on_remote_site
                    )


    def run(self, array_limit=None, by_pipeline_runner=False):

        if not self.as_subprocess:
            module_logger.debug(f"will run %s INSIDE process", self.task_key)
            self.launch_task(array_limit=array_limit)
        else:
            pipeline_cli = os.path.join(self.pipeline_work_dir, "cli")
            if self.wait_for_completion:
                cmd = [pipeline_cli, "task", self.control_dir, "--wait"]
            else:
                cmd = [pipeline_cli, "task", self.control_dir]

            if by_pipeline_runner:
                cmd.append("--by-runner")

            if module_logger.getEffectiveLevel() == logging.DEBUG:
                module_logger.debug(f"will launch sub process '%s'", " ".join(cmd))

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
        self.task_logger.info("will exit")
        logging.shutdown()
        os._exit(0)

    def sbatch_options(self):
        if self.task_conf.sbatch_options is not None and len(self.task_conf.sbatch_options) > 0:
            yield " ".join(self.task_conf.sbatch_options)

    def _children_task_keys(self):
        with open(os.path.join(self.control_dir,  "task-keys.tsv")) as f:
            for line in f:
                yield line.strip()

    def _get_drypipe_arg(self, name, mod_func):

        if self.task_conf.ssh_remote_dest is not None:
            rps = self.task_conf.remote_pipeline_specs(self.pipeline_instance_dir)
        else:
            rps = None

        def _rps():
            if rps is None:
                raise Exception(
                    f"Task({self.task_key}) python_call, has argument {mod_func} has argument {name} " +
                    " that requires task_conf.ssh_remote_dest to be not None"
                )
            return rps

        if name == "__user_at_host":
            return _rps().user_at_host
        elif name == "__remote_base_dir":
            return _rps().remote_base_dir
        elif name == "__ssh_key_file":
            return _rps().ssh_key_file
        elif name == "__task_logger":
            return self.task_logger
        elif name == "__children_task_keys":
            return self._children_task_keys()
        elif name == "__remote_pipeline_work_dir":
            return _rps().remote_instance_work_dir
        elif name == "__task_process":
            return self
        else:
            return None

    def call_python(self, mod_func, python_call):

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
            ** self.env,
            ** inputs_by_name,
            ** file_outputs_by_name,
            ** self._local_copy_adjusted_file_env_vars(),
            ** var_outputs_by_name
        }

        def get_arg(k):

            v = self._get_drypipe_arg(k, mod_func)

            if v is not None:
                return v

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
            for k, v in python_call.signature.parameters.items()
            if not k == "kwargs"
        ]

        args = [v for _, v in args_tuples]
        args_names = [k for k, _ in args_tuples]
        self.task_logger.debug("args list: %s", args_names)

        if "kwargs" not in python_call.signature.parameters:
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
            out_vars = python_call.func(* args, ** kwargs)
        except Exception as ex:
            self.task_logger.exception(ex)
            raise TaskFailedException()

        if out_vars is not None and not isinstance(out_vars, dict):
            raise Exception(
                f"function {python_call.mod_func()} called by task {self.task_key} {type(out_vars)}" +
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
            raise ex

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
                                      f"dependency on {i.name}: {i.name_in_upstream_task} not satisfied"
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
                        self.task_logger.debug(f"will resolve %s: %s", i.name, i.file_name)

                        prefix = self.task_conf.external_files_root
                        if prefix is not None:
                            self.task_logger.debug(f"prefix : %s", prefix)
                            # truncating the 2nd arg is necessary, or else the resulting path is 1st arg
                            resolved_file_name = os.path.join(prefix, i.file_name[1:])
                        else:
                            resolved_file_name = i.file_name

                        yield i, i.name, resolved_file_name
                    else:
                        self.task_logger.debug(f"will resolve non abs file %s: %s", i.name, i.file_name)
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

    def iterate_task_env(self):

        def _iter_env():
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
            yield "__pipeline_instance_name", self.pipeline_instance_name
            yield "__pipeline_work_dir", self.pipeline_work_dir
            yield "__control_dir", self.control_dir
            yield "__task_control_dir", self.control_dir
            yield "__task_key", self.task_key
            yield "__task_output_dir", self.task_output_dir

            yield "__scratch_dir", self.resolve_scratch_dir()

            yield "__output_var_file", os.path.join(self.control_dir, "output_vars")
            yield "__out_log", os.path.join(self.control_dir, "out.log")

            # stderr defaults to stdout, by default
            yield "__err_log", os.path.join(self.control_dir, "out.log")

            #if self.task_conf.ssh_remote_dest is not None:
            #    yield "__is_remote", "True"
            #else:
            #    yield "__is_remote", "False"

            yield "__is_on_remote_site", self.task_conf.is_on_remote_site

            container = self.task_conf.container
            if container is not None and container != "":
                yield "__is_singularity", "True"
            else:
                yield "__is_singularity", "False"

            self.task_logger.debug("resolved input vars")
            for k, v in self.inputs._task_inputs.items():
                yield k, v.resolved_value

            self.task_logger.debug("file output vars")
            for _, k, f in self.outputs.iterate_file_task_outputs(self.task_output_dir):
                yield k, f

        env_in_dict = dict(_iter_env())

        extra_env = self.task_conf.extra_env
        if extra_env is not None:
            self.task_logger.debug("extra_env vars from task-conf.json")
            for k, v0 in extra_env.items():
                v1 = os.path.expandvars(v0)
                # os.path.expandvars will leave missing vars unchanged, expandvars_from_dict will do it:
                v2 = expandvars_from_dict(v1, env_in_dict)
                env_in_dict[k] = v2

        for k, v in env_in_dict.items():
            yield k, v


    def resolve_scratch_dir(self):
        scratch_dir = os.environ.get('SLURM_TMPDIR')
        if scratch_dir is None:
            return os.path.join(self.task_output_dir, "scratch")
        else:
            return scratch_dir

    def run_python(self, mod_func, container=None):

        env = self.env
        python_bin = self.task_conf.python_bin
        if python_bin is None:
            python_bin = sys.executable
            #pp = env.get("PYTHONPATH")
            #env["PYTHONPATH"] = self.pipeline_work_dir

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
                APPTAINER_COMMAND,
                "exec",
                self.resolve_container_path(container)
            ] + cmd

        has_failed = False

        self.task_logger.info("run_python: %s", ' '.join(cmd))

        sub_process_env = {** os.environ, ** env}

        if "PYTHONPATH" not in sub_process_env:
            sub_process_pythonpath = []
        else:
            sub_process_pythonpath = sub_process_env["PYTHONPATH"].split(":")

        sub_process_pythonpath.insert(0, self.pipeline_work_dir)

        sub_process_env["PYTHONPATH"] = ":".join(sub_process_pythonpath)

        self.task_logger.info("PYTHONPATH: %s", sub_process_env["PYTHONPATH"])

        with open(env['__out_log'], 'a') as out:
            with open(env['__err_log'], 'a') as err:
                with PortablePopen(cmd, stdout=out, stderr=err, env=sub_process_env) as p:
                    try:
                        p.wait()
                        if p.popen.returncode != 0:
                            self.task_logger.info(f"python_call process returned {p.popen.returncode}")
                            has_failed = True
                    except Exception as ex:
                        has_failed = True
                        self.task_logger.exception(ex)
                    finally:
                        if has_failed:
                            step_number, control_dir, state_file, state_name = self.read_task_state()
                            self._transition_state_file(state_file, "failed", step_number)
        if has_failed:
            if self.as_subprocess:
                self._exit_process()
            else:
                raise TaskFailedException()

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

    def read_task_state(self, control_dir=None, state_file=None, non_existant_ok=False):

        if control_dir is None:
            control_dir = self.env["__control_dir"]

        if state_file is None:
            glob_exp = os.path.join(control_dir, "state.*")
            state_file = list(glob.glob(glob_exp))

            if len(state_file) == 0:
                if non_existant_ok:
                    state_file = Path(control_dir, "state.ready")
                    state_file.touch()
                    return 0, control_dir, state_file, "ready"
                else:
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


    def transition_to_step_started(self, state_file, step_number, previous_state_name=None):

        if previous_state_name == "failed":
            with open(self.env['__out_log'], 'a') as out:
                out.write(f"\n================ step {step_number} restarted after failure =====================\n\n")
                exit(1)

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

        has_var_outputs = self.outputs.has_var_outputs()

        if has_var_outputs:
            dump_env = f' ; python3 -c "import os, json; print(json.dumps(dict(os.environ)))"'
        else:
            dump_env = ''

        out = env['__out_log']
        err = env['__err_log']

        cmd = ["bash", "-c", f". {script} 1>> {out} 2>> {err}{dump_env}"]

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
                if has_var_outputs:
                    out = p.stdout_as_string()
                    step_output_vars = json.loads(out)
                    task_output_vars = dict(self.iterate_out_vars_from())
                    for o in self.outputs.iterate_non_file_outputs():
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

    def _create_local_scratch_and_rsync_inputs(self):

        Path(self._local_inputs_root()).mkdir(exist_ok=True)
        Path(self._local_outputs_root()).mkdir(exist_ok=True)

        with NamedTemporaryFile("w", prefix="zzz") as tf:
            for fi in self.dependent_file_list():
                tf.write(fi)
                tf.write("\n")
            tf.flush()

            pid = self.pipeline_instance_dir

            invoke_rsync(f"rsync --files-from={tf.name} {pid}/ {self._local_inputs_root()}")

    def _rsync_outputs_from_scratch(self):
        invoke_rsync(
            f"rsync -a --dirs {self._local_outputs_root()}/ {self.pipeline_output_dir}/{self.task_key}"
        )

    def _resolve_steps(self):
        if self.task_conf.ssh_remote_dest is not None and not self.task_conf.is_on_remote_site:
            if self.is_slurm_array_parent():
                yield {"call": "python", "module_function": "dry_pipe.task_lib:upload_array"}
                yield {"call": "python", "module_function": "dry_pipe.task_lib:execute_remote_task"}
                yield {"call": "python", "module_function": "dry_pipe.task_lib:download_array"}

            else:
                yield {"call": "python", "module_function": "dry_pipe.task_lib:execute_remote_task"}
        elif self.is_slurm_array_parent():
            yield {"call": "python", "module_function": "dry_pipe.task_lib:run_array"}
        else:
            yield from self.task_conf.step_invocations

    def _run_steps(self):

        step_number, control_dir, state_file, state_name = self.read_task_state(non_existant_ok=True)

        step_invocations = list(self._resolve_steps())

        if self._is_work_on_local_copy():
            self._create_local_scratch_and_rsync_inputs()

        try:

            for i in range(step_number, len(step_invocations)):

                step_invocation = step_invocations[i]
                state_file, step_number = self.transition_to_step_started(
                    state_file, step_number, previous_state_name=state_name
                )

                call = step_invocation["call"]

                if call == "python":
                    module_function = step_invocation["module_function"]
                    if self.run_python_calls_in_process or module_function.startswith("dry_pipe.task_lib:"):
                        python_call = func_from_mod_func(module_function)
                        self.call_python(module_function, python_call)
                    else:
                        self.run_python(module_function, step_invocation.get("container"))
                elif call == "bash":
                    self.run_script(os.path.expandvars(step_invocation["script"]), step_invocation.get("container"))
                else:
                    raise Exception(f"unknown step invocation type: {call}")

                state_file, step_number = self.transition_to_step_completed(state_file, step_number)

            if self._is_work_on_local_copy():
                self._rsync_outputs_from_scratch()

            self.transition_to_completed(state_file)
        except TaskFailedException as tfe:
            self._transition_state_file(state_file, "failed", step_number)

    """
    def run_array(self, limit):

        from dry_pipe.slurm_array_task import SlurmArrayParentTask

        step_number, _, state_file, _ = self.read_task_state(non_existant_ok=True)
        try:
            sapt = SlurmArrayParentTask(self)

            all_children_completed = sapt.run_array(False, False, limit)

            if all_children_completed:
                self.transition_to_completed(state_file)
        except Exception as ex:
            self.task_logger.exception(ex)
            self._transition_state_file(state_file, "failed", step_number)
            raise ex
    """

    def sbatch_cmd_lines(self):

        if self.task_conf.executer_type != "slurm":
            raise Exception(f"not a slurm task")

        yield "sbatch"

        if self.wait_for_completion:
            yield "--wait"

        sacc = self.task_conf.slurm_account
        if sacc is not None:
            yield f"--account={sacc}"

        yield from self.sbatch_options()

        yield f"--output={self.control_dir}/out.log"


        yield "--export={0}".format(",".join([f"DRYPIPE_TASK_CONTROL_DIR={self.control_dir}"]))
        yield "--signal=B:USR1@50"
        yield "--parsable"
        yield f"{self.pipeline_instance_dir}/.drypipe/cli"

    def submit_sbatch_task(self):

        p = PortablePopen(
            list(self.sbatch_cmd_lines()),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )

        p.popen.wait()

        for l in p.read_stdout_lines():
            print(l)

    def is_array_child_task(self):
        return self.slurm_array_task_id is not None

    def _control_dir_from_env(self):
        return os.environ.get("DRYPIPE_TASK_CONTROL_DIR")

    def _array_parent_control_dir(self):

        if not self.is_array_child_task():
            raise Exception(f"can't call when non child array")

        return self._control_dir_from_env()

    def _override_control_dir_if_child_task(self):
        if self.is_array_child_task():

            self.no_dynamic_steps = True

            array_index_2_task_key = os.environ.get("DRYPIPE_TASK_KEY_FILE_BASENAME")

            control_dir_from_env = self._control_dir_from_env()

            if control_dir_from_env is None:
                raise Exception(f"child array slurm task {self.control_dir} has no env DRYPIPE_TASK_CONTROL_DIR")

            def children_task_keys():
                with open(os.path.join(control_dir_from_env, array_index_2_task_key)) as f:
                    for line in f:
                        yield line.strip()

            slurm_array_task_id = int(self.slurm_array_task_id)
            c = 0
            _drypipe_dir = os.path.dirname(control_dir_from_env)

            for task_key in children_task_keys():
                if c == slurm_array_task_id:
                    p = os.path.join(_drypipe_dir, task_key)
                    self.control_dir = p
                    self.task_output_dir = os.path.join(self.pipeline_output_dir, task_key)
                    return
                else:
                    c += 1

            raise Exception(f"Error: no task_key for SLURM_ARRAY_TASK_ID={slurm_array_task_id}")

    def _launch_and_tail(self, launch_func):
        def func():
            flf = os.path.join(self.control_dir, "out.log")

            while not os.path.exists(flf):
                time.sleep(1)
                if self.has_ended:
                    break

            if not self.has_ended:
                with open(flf) as f:
                    for line in tail_file(f, 1):
                        print(f"out.log - {line}")
                        if self.has_ended:
                            break

        t = Thread(target=func)
        t.start()
        launch_func()

    def is_slurm_array_parent(self):
        is_slurm_parent = self.task_conf.is_slurm_parent
        return is_slurm_parent is not None and is_slurm_parent

    def launch_task(self, array_limit=None):

        exit_process_when_done = self.as_subprocess

        if not os.path.exists(self.task_output_dir):
            Path(self.task_output_dir).mkdir(
                parents=True, exist_ok=True,
                mode=FileCreationDefaultModes.pipeline_instance_directories
            )

            if self._is_work_on_local_copy():
                Path(self.task_output_dir, "scratch").mkdir(
                    parents=True, exist_ok=True,
                    mode=FileCreationDefaultModes.pipeline_instance_directories
                )

        def task_func_wrapper():
            try:
                self.task_logger.debug("task func started")
                self._run_steps()
                self.task_logger.info("task completed")
            except Exception as ex:
                if not exit_process_when_done:
                    raise ex
                self.task_logger.exception(ex)
            finally:
                self.has_ended = True
                if exit_process_when_done and not self.launched_from_cli_with_tail():
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


def tail_file(file, delay=1.0):
    line_terminators = ("\r\n", "\n", "\r")
    trailing = True

    while 1:
        where = file.tell()
        line = file.readline()
        if line:
            if trailing and line in line_terminators:
                trailing = False
                continue

            if line[-1] in line_terminators:
                line = line[:-1]
                if line[-1:] == "\r\n" and "\r\n" in line_terminators:
                    line = line[:-1]

            trailing = False
            yield line
        else:
            trailing = True
            file.seek(where, 0)
            time.sleep(delay)
