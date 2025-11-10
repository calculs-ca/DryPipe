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

from dry_pipe.slurm_arrays import ArrayTaskManager, SAcctParser, SQueueParser
from dry_pipe import TaskConf, RemotePipelineSpecs, AutoRestartManager
from dry_pipe.core_lib import UpstreamTasksNotCompleted, PortablePopen, func_from_mod_func, invoke_rsync, \
    FileCreationDefaultModes, expandvars_from_dict, TimeLogger

from dry_pipe.task import TaskOutput, TaskInputs, TaskOutputs, TaskInput

APPTAINER_COMMAND = "apptainer"


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
            from_remote=False,
            alternate_logger=None,
            use_remote_drypipe_log=False,
            for_dry_run=False
    ):

        self.slurm_job_id = os.environ.get("SLURM_JOB_ID")
        self.slurm_array_job_id = os.environ.get("SLURM_ARRAY_JOB_ID")
        self.slurm_array_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")

        self.wait_for_completion = wait_for_completion
        self.tail = tail
        self.tail_all = tail_all
        self.has_ended = False
        self.as_subprocess = as_subprocess
        self.test_mode = test_mode
        self.run_python_calls_in_process = run_python_calls_in_process
        self.env = {}
        self.is_array_spawn = os.environ.get("ARRAY_SPAWN") == "True"

        if not is_python_call:
            # override causes problems for python_call
            self.control_dir = self._override_control_dir_if_child_task(control_dir)
        else:
            self.control_dir = control_dir

        self.task_key = os.path.basename(self.control_dir)
        self.pipeline_work_dir = os.path.dirname(self.control_dir)
        self.pipeline_instance_dir = os.path.dirname(self.pipeline_work_dir)
        self.pipeline_instance_name = os.path.basename(self.pipeline_instance_dir)
        self.pipeline_output_dir = os.path.join(self.pipeline_instance_dir, "output")
        self.task_output_dir = os.path.join(self.pipeline_output_dir, self.task_key)
        self.command_before_task_has_run = False
        self.is_on_remote_site = False
        self.use_remote_drypipe_log = use_remote_drypipe_log
        self.for_dry_run = for_dry_run


        try:
            self.task_conf = None
            self.task_conf = TaskConf.from_json_file(self.control_dir)
        except Exception as ex:
            # ensure this gets logged
            self._create_task_logger().exception(ex)
            raise ex

        if no_logger:
            self.task_logger = module_logger
        elif alternate_logger is not None:
            self.task_logger = alternate_logger
        else:
            self.task_logger = self._create_task_logger()

        try:

            self.task_logger.debug("SLURM_ARRAY_TASK_ID: '%s'", self.slurm_array_task_id)

            self.task_conf = TaskConf.from_json_file(self.control_dir)

            self._override_task_confs_if_applicable()

            if self.pipeline_instance_dir == "":
                raise Exception(f"pipeline_instance_dir can't be empty string")

            task_inputs, task_outputs = self._unserialize_and_resolve_inputs_outputs(ensure_all_upstream_deps_complete)

            self.inputs = TaskInputs(self.task_key, task_inputs)
            self.outputs = TaskOutputs(self.task_key, task_outputs)


            for k, v in self.iterate_task_env():
                v = str(v)
                # consider sub logger, activatable by env var
                #self.task_logger.debug("env var %s = %s", k, v)
                self.env[k] = v

            for name, i in task_inputs.items():
                if i.is_constant() and i.type == "str":
                    v = expandvars_from_dict(i.value, self.env)
                    i.resolved_value = v
                    self.env[i.name] = v


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
        if self.use_remote_drypipe_log:
            return os.path.join(self.control_dir, "drypipe-remote.log")
        else:
            return os.path.join(self.control_dir, "drypipe.log")

    def is_debug(self):
        if os.environ.get("DRYPIPE_TASK_DEBUG") == "True":
            return True
        if self.task_conf is not None:
            if self.task_conf.extra_env is not None:
                if self.task_conf.extra_env.get("DRYPIPE_TASK_DEBUG") == "True":
                    return True
        return False

    def create_time_logger(self, label, logger_func):
        return TimeLogger(label, logger_func)

    def _create_task_logger(self):

        if self.is_debug():
            logging_level = logging.DEBUG
        else:
            logging_level = logging.INFO

        logger = logging.getLogger(f"task-logger-{os.path.basename(self.control_dir)}")
        logger.propagate = False
        logger.setLevel(logging_level)

        file_handler = logging.FileHandler(filename=self.drypipe_log_file())
        file_handler.setLevel(logging_level)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S%z')
        )

        for h in logger.handlers:
            h.close()
        logger.handlers.clear()
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


        logger.debug("log level: %s", logging.getLevelName(logging_level))
        return logger

    def __repr__(self):
        return f"{self.task_key}"

    def _override_task_confs_if_applicable(self):
        self._override_task_conf_from_site_env()
        if self.is_array_child_task():
            array_parent_dir = self._array_parent_control_dir()
            self.task_logger.debug(f"task is array child task of parent task %s", array_parent_dir)

    def _override_task_conf_from_site_env(self):

        site_env_file = Path(self.pipeline_work_dir, "site.env")

        if not site_env_file.exists():
            self.task_logger.info("NOT on remote site")
        else:
            self.task_logger.info("is on remote site")
            self.is_on_remote_site = True
            with open(site_env_file) as f:
                for l in f.readlines():
                    l = l.strip()
                    if l.startswith("#"):
                        continue
                    if "=" in l:
                        k, v = [s.strip() for s in l.split("=")]
                        if k == "DRYPIPE_EXTERNAL_FILES_ROOT":
                            self.task_logger.debug("external files root: %s", v)
                            self.task_conf.external_files_root = v

    def run(self, array_limit=None, by_pipeline_runner=False):

        if not self.as_subprocess:
            self.launch_task(array_limit=array_limit)
        else:
            pipeline_cli = os.path.join(self.pipeline_work_dir, "cli")
            pid = self.pipeline_instance_dir
            cmd = [pipeline_cli, "task", f"--pipeline-instance-dir={pid}", f"--task-key={self.task_key}"]
            if self.wait_for_completion:
                cmd.append("--wait")

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
        self.task_logger.info("will exit")
        logging.shutdown()
        os._exit(0)

    def children_task_keys(self):
        with open(os.path.join(self.control_dir,  "task-keys.tsv")) as f:
            for line in f:
                yield line.strip()

    def gen_internal_file_deps(self, external_file_deps):

        def g(task_key):
            p = TaskProcess(
                os.path.join(self.pipeline_work_dir, task_key),
                ensure_all_upstream_deps_complete=True
            )

            for _, file in p.inputs.rsync_file_list_produced_upstream():
                yield file

            for i in p.inputs.rsync_file_sets_produced_upstream():
                with open(Path(p.pipeline_work_dir, i.upstream_task_key,"task-conf.json")) as tc:
                    z = json.load(tc)
                    for o in z['outputs']:
                        if o['name'] == i.name:
                            fs = o['file_set']
                            for aa in Path(p.pipeline_output_dir, i.upstream_task_key).glob(fs['pattern']):
                                qq = aa.relative_to(p.pipeline_instance_dir)
                                yield qq.__str__()

            for file in p.inputs.rsync_output_var_file_list_produced_upstream():
                yield file

            for _, file in p.inputs.pre_existing_non_produced_file_list():
                if os.path.isabs(file):
                    external_file_deps.append(file)
                else:
                    yield file
            yield f".drypipe/{task_key}/task-conf.json"

            for step in p.task_conf.step_invocations:
                if step["call"] == "bash":
                    _, script = step["script"].rsplit("/", 1)
                    yield f".drypipe/{task_key}/{script}"

        if self.is_slurm_array_parent():
            yield f".drypipe/{self.task_key}/task-keys.tsv"
            for child_task_key in self.children_task_keys():
                yield from g(child_task_key)
                #yield f".drypipe/{child_task_key}/state.ready"

        yield from g(self.task_key)

        yield ".drypipe/cli"
        yield ".drypipe/cli-init.sh"

        for py_file in glob.glob(os.path.join(os.path.dirname(__file__), "*.py")):
            yield f".drypipe/dry_pipe/{os.path.basename(py_file)}"


    def _get_drypipe_arg(self, name, mod_func):

        if name == "__remote_pipeline_specs":
            if self.task_conf.ssh_remote_dest is not None:
                return RemotePipelineSpecs(self)
            else:
                raise Exception(
                    f"Task({self.task_key}) python_call {mod_func} has argument {name} " +
                    " that requires task_conf.ssh_remote_dest to be not None"
                )
        elif name == "__task_logger":
            return self.task_logger
        elif name == "__children_task_keys":
            return self.children_task_keys()
        elif name == "__task_process":
            return self
        elif name == "__task_conf":
            return self.task_conf
        else:
            return None

    def dump_function_call_in_stdout(self, func_log):
        with open(os.path.join(self.control_dir, "out.log"), mode="a") as out:
            out.write(f"================ {func_log} ====================\n")


    def call_python(self, mod_func, python_call):

        pythonpath_in_env = os.environ.get("PYTHONPATH")

        if pythonpath_in_env is not None:
            for p in pythonpath_in_env.split(":"):
                if not os.path.exists(p):
                    self.task_logger.warning(
                        f"WARNING: path {p} in PYTHONPATH does not exist, if running in apptainer, ensure proper mount"
                    )

        inputs_by_name = {}
        file_outputs_by_name = {}
        # python_calls can access outputs of previous ones with input args
        var_outputs_by_name = {}

        task_output_vars = dict(self.iterate_out_vars_from())

        for k, v in self.inputs._task_inputs.items():
            inputs_by_name[k] = v.resolved_value

        for o, k, f in self.outputs.iterate_file_task_outputs(self.task_output_dir):
            file_outputs_by_name[k] = f

        for o in self.outputs.iterate_non_file_outputs():
            v = task_output_vars.get(o.name)
            if v is None:
                v = os.environ.get(o.name)
            if v is not None:
                var_outputs_by_name[o.name] = o.parse(v)

        all_function_input_candidates = {
            ** os.environ,
            ** self.env,
            ** inputs_by_name,
            ** file_outputs_by_name,
            ** self._local_copy_adjusted_file_env_vars(),
            ** var_outputs_by_name,
            ** python_call.fixed_args
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

        self.dump_function_call_in_stdout(func_log)

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
                    self.task_logger.debug("%s task_input:", i.as_string())
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
                o.task_key = self.task_key

                if o.type == 'file_set':
                    o.task_output_dir = self.task_output_dir
                elif o.type == 'file':
                    o.set_resolved_value(os.path.join(self.task_output_dir, o.produced_file_name))
                else:
                    v = unparsed_out_vars.get(o.name)
                    if v is not None:
                        o.set_resolved_value(v)

                task_outputs[o.name] = o


        return task_inputs, task_outputs

    def resolve_task(self, state_file):

        class ResolvedTask:
            def __init__(self, tp):
                self.task_process = tp
                self.key = state_file.task_key
                self.inputs = tp.inputs
                self.outputs = tp.outputs
                self.state_file = state_file

            def __str__(self):
                return f"Task(key={self.key})"

            def refresh_state(self):
                self.state_file.reload()

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

            def glob_output(self, pattern):
                return Path(state_file.output_dir()).glob(pattern)

            def step_idx(self):
                return state_file.step_idx()

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
                    s = pc.read()
                    try:
                        pipeline_conf_json = json.loads(s)
                    except json.decoder.JSONDecodeError:
                        raise Exception(f"bad JSON in config file {pipeline_conf}: '{s}'")

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

            if self.is_remote_execution_on_remote_site():
                if self.task_conf.remote_pipeline_code_dir is not None:
                    yield "__pipeline_code_dir", self.task_conf.remote_pipeline_code_dir

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

            yield "__is_on_remote_site", self.is_on_remote_site

            container = self.task_conf.container
            if container is not None and container != "":
                yield "__is_singularity", "True"
            else:
                yield "__is_singularity", "False"

            for k, v in self.inputs._task_inputs.items():
                yield k, v.resolved_value

            for _, k, f in self.outputs.iterate_file_task_outputs(self.task_output_dir):
                yield k, f

        env_in_dict = dict(_iter_env())

        extra_env = self.task_conf.extra_env
        if extra_env is not None:
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


    def _apptainer_cmd(self, container, cmd):

        def apt_cmd():
            yield APPTAINER_COMMAND
            yield "exec"
            if self.task_conf.apptainer_exec_args is not None:
                if isinstance(self.task_conf.apptainer_exec_args, str):
                    yield self.task_conf.apptainer_exec_args
                elif isinstance(self.task_conf.apptainer_exec_args, list):
                    for a in self.task_conf.apptainer_exec_args:
                        yield a
            yield self.resolve_container_path(container)

        return list(apt_cmd()) + cmd

    def run_python(self, mod_func, container=None):

        self.exec_cmd_before_launch_if_applies()

        env = self.env
        python_bin = self.task_conf.python_bin
        if python_bin is None:
            python_bin = sys.executable
            self.task_logger.debug(f"no python_bin defined in task-conf.json, will use %s", python_bin)
        else:
            self.task_logger.debug(f"will use python_bin from task-conf.json: %s", python_bin)

        switches = "-u"
        cmd = [
            python_bin,
            switches,
            "-m",
            "dry_pipe.cli",
            "call",
            mod_func,
            f"--pipeline-instance-dir={self.pipeline_instance_dir}",
            f"--task-key={self.task_key}"
        ]

        if container is not None:
            cmd = self._apptainer_cmd(container, cmd)

            self._set_apptainer_bind_in_env(env)

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
        self.task_logger.info("APPTAINER_BIND: %s", sub_process_env.get("APPTAINER_BIND"))

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


    def exec_cmd_before_launch_if_applies(self):
        if self.task_conf.command_before_task is not None and not self.command_before_task_has_run:
            self.exec_cmd_before_launch(self.task_conf.command_before_task)
            self.command_before_task_has_run = True


    def exec_cmd_before_launch(self, command_before_task):

        pythonpath_b4 = self.env.get("PYTHONPATH")
        apptainer_bind_b4 = self.env.get("APPTAINER_BIND")

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
                v0 = v
                if k == "PYTHONPATH":
                    if v is None or v == "":
                        break
                    if pythonpath_b4 is not None and pythonpath_b4 != "":
                        v = f"{v}:{pythonpath_b4}"
                elif k == "APPTAINER_BIND":
                    v = apptainer_bind_b4
                v1 = v

                self.task_logger.debug("env var %s override %s  -> %s: ", k, v0, v1)

                self.env[k] = v

    @classmethod
    def read_task_state_from(cls, control_dir, state_file=None, non_existant_ok=False):
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
                ls_files = ','.join([os.path.basename(f) for f in state_file])
                raise Exception(f"more than one state file found in {control_dir}: {ls_files}")

            state_file = state_file[0]

        base_file_name = os.path.basename(state_file)
        name_parts = base_file_name.split(".")
        state_name = name_parts[1]

        if len(name_parts) == 3:
            step_number = int(name_parts[-1])
        else:
            step_number = 0

        return step_number, control_dir, state_file, state_name


    def read_task_state(self, control_dir=None, state_file=None, non_existant_ok=False):
        if control_dir is None:
            control_dir = self.env["__control_dir"]
        return self.read_task_state_from(control_dir=control_dir, state_file=state_file, non_existant_ok=non_existant_ok)

    def rewind_to_step(self, i):
        step_number, control_dir, state_file, state_name = self.read_task_state()
        self.task_logger.info("rewind to step %d", i)
        self._transition_state_file(state_file, "waiting", i)


    def _transition_state_file(self, state_file, next_state_name, step_number=None, update_slurm_job_name=False):

        #self.task_logger.debug("_transition_state_file: %s", state_file)

        control_dir = os.path.dirname(state_file)

        if step_number is None:
            next_step_number = None
            next_state_basename = f"state.{next_state_name}"
        else:
            next_step_number = step_number
            next_state_basename = f"state.{next_state_name}.{next_step_number}"

        next_name = next_state_basename[6:]

        if update_slurm_job_name:
            self._update_job_name(f"{self.task_key}:{next_name}")
        else:
            if self.is_array_child_task() or self.is_array_spawn:
                if next_state_name in ["failed", "timed-out", "completed"]:
                    self._update_job_name(f"{self.task_key}:{next_name}")

        next_state_file = os.path.join(control_dir, next_state_basename)

        self.task_logger.info("will transition to: %s", next_state_basename)
        #self.task_logger.debug("next_state_file: %s", next_state_file)

        os.rename(
            state_file,
            next_state_file
        )

        return next_state_file, next_step_number


    def transition_to_step_started(self, state_file, step_number, previous_state_name=None, is_pre_launch=False):

        if previous_state_name == "failed":
            with open(self.env['__out_log'], 'a') as out:
                out.write(f"\n================ step {step_number} restarted after failure =====================\n\n")


        pre_launch_flag = "_" if is_pre_launch else ""

        update_slurm_job_name = False
        if self.is_array_child_task() and not is_pre_launch:
            update_slurm_job_name = True

        return self._transition_state_file(state_file, f"{pre_launch_flag}step-started", step_number, update_slurm_job_name)


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
        return self._transition_state_file(
            state_file,
            "completed",
            update_slurm_job_name=self.is_array_child_task()
        )


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

        self.task_logger.info("output vars written: %s", ",".join(all_vars))

    def resolve_container_path(self, container):

        def _log_resolved_path(container_path):
            self.task_logger.debug(f"resolved container path: {container_path}")

        if os.path.exists(container):
            _log_resolved_path(container)
            return container

        if os.path.isabs(container):
            self.task_logger.error(f"container file not found: {container}")
            raise TaskFailedException()

        if "__pipeline_code_dir" in self.env:
            p = Path(self.env["__pipeline_code_dir"], "containers", container)
            if p.exists():
                p = p.__str__()
                _log_resolved_path(p)
                return p

        self.task_logger.error(f"container file not found: {container}")
        raise TaskFailedException()

    def _resolve_script(self, script, env):

        expanded_script_path = expandvars_from_dict(script, env)

        if os.path.exists(expanded_script_path):
            self.task_logger.debug("expanded script: %s resolves to: %s", script, expanded_script_path)
            return expanded_script_path

        p = os.path.join(
            env["__control_dir"],
            os.path.basename(script)
        )

        self.task_logger.debug("script: %s resolves to: %s", script, p)

        return p

    def run_script(self, script, container=None):

        self.exec_cmd_before_launch_if_applies()

        env = {
            ** self.env,
            ** self._local_copy_adjusted_file_env_vars()
        }

        script = self._resolve_script(script, env)

        has_var_outputs = self.outputs.has_var_outputs()

        if has_var_outputs:
            dump_env = f' ; python3 -c "import os, json; print(json.dumps(dict(os.environ)))"'
        else:
            dump_env = ''

        out = env['__out_log']
        err = env['__err_log']

        cmd = ["bash", "-c", f". {script} 1>> {out} 2>> {err}{dump_env}"]

        if container is not None:
            cmd = self._apptainer_cmd(container, cmd)

        self._set_apptainer_bind_in_env(env)

        self.task_logger.info("run_script: %s", " ".join(cmd))

        self.dump_function_call_in_stdout(f"{self.task_key}: {script}")

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
                        prev_value = task_output_vars.get(o.name)
                        self.task_logger.debug(
                            "script exported output var %s = %s, previous value %s", o.name, v, prev_value
                        )
                        if v is not None:
                            task_output_vars[o.name] = v
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
            for var_name, file in self.inputs.pre_existing_non_produced_file_list():
                yield var_name, os.path.join(local_inputs, file)

            for var_name, file in self.inputs.rsync_file_list_produced_upstream():
                yield var_name, os.path.join(local_inputs, file)

            local_outputs = self._local_outputs_root()
            for _, var_name, file in self.outputs.iterate_file_task_outputs(local_outputs):
                yield var_name, file

        return dict(gen())

    def dependent_file_list(self):
        for var_name, file in self.inputs.pre_existing_non_produced_file_list():
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

    def sleep_schedule(self, value_when_absent):
        custom_sleep_schedule = self.env.get("DRYPIPE_SLEEP_SCHEDULE")

        if custom_sleep_schedule is not None:
            res = [int(s) for s in custom_sleep_schedule.split(",")]
        else:
            res = value_when_absent

        self.task_logger.info("sleep schedule: %s", res)

        return res

    def is_remote_execution_on_master_site(self):
        return self.task_conf.ssh_remote_dest is not None and not self.is_on_remote_site

    def is_remote_execution_on_remote_site(self):
        return self.task_conf.ssh_remote_dest is not None and self.is_on_remote_site

    def _resolve_steps(self):

        if (
                self.task_conf.ssh_remote_dest is None
            or
                self.is_on_remote_site
            or
                not self.is_slurm_array_parent()
        ):
            if self.task_conf.ssh_remote_dest is not None and not self.is_on_remote_site:
                pass
            elif not self.is_slurm_array_parent():
                yield from self.task_conf.step_invocations
                return

        def g():

            rsync_or_globus = "globus" if self.task_conf.globus_transfer is not None else "rsync"

            if self.is_remote_execution_on_master_site():
                yield f"upload_task_inputs_{rsync_or_globus}"

                if self.is_slurm_array_parent():
                    yield "submit_remote_array"
                    yield "watch_remote_array"
                else:
                    yield "execute_remote_task"
                    yield "poll_remote_task"

                yield f"download_task_outputs_{rsync_or_globus}"
            else:
                if self.is_slurm_array_parent():
                    yield "submit_local_array"
                    yield "watch_local_array"

        for f in g():
            yield {"call": "python", "module_function": f"dry_pipe.task_lib:{f}"}


    def _launch_next_step_on_new_sbatch_if_required(self, step_invocation, state_file, step_number):

        if "sbatch_options" not in step_invocation:
            return False
        else:

            state_file_bn = os.path.basename(state_file)

            if state_file_bn.startswith("state._step-started"):
                return False

            self.transition_to_step_started(state_file, step_number, is_pre_launch=True)
            try:

                job_name = f"{self.task_key}:{state_file_bn[6:]}"
                sbo = step_invocation["sbatch_options"] + [f"--job-name={job_name}"]
                cmd = list(self.sbatch_cmd_lines(sbo, is_spawn=True))
                self.task_logger.info("will spawn next step: %s", " ".join(cmd))

                with PortablePopen(cmd) as p:
                    p.wait_and_raise_if_non_zero()
                    job_id = p.stdout_as_string().strip()

                    self._update_job_name(f"{job_name}>{job_id}")

                    self.task_logger.info("launched job_id %s", job_id)
                    return True
            except Exception as ex:
                self.task_logger.error("fail task launch", exc_info=ex)
                raise TaskFailedException()


    def _run_steps(self):

        step_number, control_dir, state_file, state_name = self.read_task_state(non_existant_ok=True)

        step_invocations = list(self._resolve_steps())

        if self._is_work_on_local_copy():
            self._create_local_scratch_and_rsync_inputs()

        skip_transition_to_completed = False

        try:

            for i in range(step_number, len(step_invocations)):

                step_invocation = step_invocations[i]

                if self._launch_next_step_on_new_sbatch_if_required(step_invocation, state_file, step_number):
                    skip_transition_to_completed = True
                    break

                state_file, step_number = self.transition_to_step_started(
                    state_file, step_number, previous_state_name=state_name
                )

                call = step_invocation["call"]

                with self.create_time_logger(f"STEP-{i}", self.task_logger.info):
                    if call == "python":
                        module_function = step_invocation["module_function"]
                        self.task_logger.debug("step %s, %s %s", i, call, module_function)
                        if self.run_python_calls_in_process or module_function.startswith("dry_pipe."):
                            python_call = func_from_mod_func(module_function)
                            self.call_python(module_function, python_call)
                        else:
                            self.run_python(module_function, step_invocation.get("container"))
                    elif call == "bash":
                        self.task_logger.debug("step %s, %s %s,", i, call, step_invocation["script"])
                        self.run_script(os.path.expandvars(step_invocation["script"]), step_invocation.get("container"))
                    else:
                        raise Exception(f"unknown step invocation type: {call}")

                state_file, step_number = self.transition_to_step_completed(state_file, step_number)

            if self._is_work_on_local_copy():
                self._rsync_outputs_from_scratch()

            if not skip_transition_to_completed:
                self.transition_to_completed(state_file)
        except TaskFailedException as tfe:
            self._transition_state_file(state_file, "failed", step_number)

    def sbatch_cmd_lines(self, override_options=None, is_spawn=False):

        #if self.task_conf.executer_type != "slurm":
        #    raise Exception(f"not a slurm task")

        yield "sbatch"

        if self.wait_for_completion:
            yield "--wait"

        sacc = self.task_conf.slurm_account
        if sacc is not None:
            yield f"--account={sacc}"

        if override_options is not None:
            yield from override_options
        else:
            yield from self.task_conf.sbatch_options

        yield f"--output={self.control_dir}/out.log"

        def job_env():
            yield f"DRYPIPE_TASK_CONTROL_DIR={self.control_dir}"
            if is_spawn:
                yield "ARRAY_SPAWN=True"

        yield "--export={0}".format(",".join(job_env()))
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
        return self.slurm_array_task_id is not None and "DRYPIPE_TASK_KEY_FILE_BASENAME" in os.environ

    def _control_dir_from_env(self):
        return os.environ.get("DRYPIPE_TASK_CONTROL_DIR")

    def _array_parent_control_dir(self):

        if not self.is_array_child_task():
            raise Exception(f"can't call when non child array")

        return self._control_dir_from_env()

    def _override_control_dir_if_child_task(self, control_dir):

        if not self.is_array_child_task():
            return control_dir
        else:
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
                    return os.path.join(_drypipe_dir, task_key)
                else:
                    c += 1

            raise Exception(f"Error: no task_key for SLURM_ARRAY_TASK_ID={slurm_array_task_id}")

    def _delete_array_child_launch_log_if_empty(self):
        launch_log = Path(
            self._array_parent_control_dir(),
            f"launch-{self.slurm_array_job_id}_{self.slurm_array_task_id}.out"
        )
        if launch_log.exists() and launch_log.stat().st_size == 0:
            launch_log.unlink()
        else:
            # should be rare, since launch error makes it unlikely to make it here
            self.task_logger.warning("non empty launch log")


    def _task_job_id(self):
        if self.is_array_child_task():
            return f"{self.slurm_array_job_id}_{self.slurm_array_task_id}"
        else:
            return self.slurm_job_id

    def _update_job_name(self, name):
        this_task_job_id = self._task_job_id()
        self.task_logger.info("Will rename slurm job %s to %s", this_task_job_id, name)
        with PortablePopen(["scontrol", "update", f"JobId={this_task_job_id}", f"JobName={name}"]) as p:
            p.wait_and_raise_if_non_zero()

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
                with self.create_time_logger("TASK", self.task_logger.info):
                    self._run_steps()
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
            is_slurm = self.slurm_job_id is not None
            if (not is_slurm) and os.fork() != 0:
                # launching process, die to let the child run in the background
                exit(0)
            else:
                # forked child, or slurm job
                if is_slurm:
                    if self.is_array_child_task():
                        #step_number, control_dir, state_file, state_name = self.read_task_state()
                        #self._update_job_name(f"{self.task_key}:{state_name}.{step_number}")
                        self._delete_array_child_launch_log_if_empty()

                os.setpgrp()
                self.register_signal_handlers()
                Thread(target=task_func_wrapper).start()
                signal.pause()

    def reset_restart_accounting(self):
        p = Path(self.control_dir, "restarts.tsv")
        if p.exists():
            with open(p, "a") as f:
                f.writelines("RESET\n")

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

    def file_sets_rsync_list_file(self):
        return os.path.join(self.control_dir, "file-sets-rsync-list.txt")

    def generate_rsync_list_for_file_sets(self):

        if self.is_slurm_array_parent():
            from dry_pipe.slurm_array_task import SlurmArrayParentTask
            array_parent_task = SlurmArrayParentTask(self)
            task_keys_iterator = array_parent_task.children_task_keys()
        else:
            task_keys_iterator = [self.task_key]


        with open(self.file_sets_rsync_list_file(), "w") as rsync_list_file:

            for task_key in task_keys_iterator:
                task_process = TaskProcess(
                    os.path.join(self.pipeline_work_dir, task_key),
                    ensure_all_upstream_deps_complete=False,
                    no_logger=True
                )

                for f in task_process.outputs.rsync_filter_list(task_process.task_output_dir):
                    rsync_list_file.write(f)
                    rsync_list_file.write(f"\n")

    def create_array_task_manager(self):

        arm = None
        if self.task_conf.auto_restart_condition_regexp_per_log_file is not None:
             arm = AutoRestartManager(
                 self.task_conf.auto_restart_condition_regexp_per_log_file,
                 for_dry_run=self.for_dry_run
             )

        if self.task_conf.use_squeue or os.environ.get("DRYPIPE_USE_SQUEUE") == "True":
            parser = SQueueParser()
        else:
            parser = SAcctParser()

        return ArrayTaskManager(self, arm, parser, for_dry_run=self.for_dry_run)

    def _set_apptainer_bind_in_env(self, env, script=None):

        def _root_dir(d):
            p = Path(d)
            return os.path.join(p.parts[0], p.parts[1])

        def _fs_type(file):

            stat_cmd = f"stat -f -L -c %T {file}"
            with PortablePopen(stat_cmd.split()) as p:
                p.wait_and_raise_if_non_zero()
                return p.stdout_as_string().strip()

        apptainer_bindings = []

        if script is not None:
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
        if new_bind is None:
            self.task_logger.info("APPTAINER_BIND not set")
        else:
            self.task_logger.info("APPTAINER_BIND=%s", new_bind)

    def remote_task_helper(self):
        return RemotePipelineSpecs(self)

    def is_task_logger_debug_level(self):
       return self.task_logger.getEffectiveLevel() == logging.DEBUG

    def fetch_remote_state(self):
        if self.is_remote_execution_on_master_site():
            remote_task_helper = RemotePipelineSpecs(self)
            if self.is_slurm_array_parent():
                # TODO: don't fetch logs for the whole pipeline`:
                remote_task_helper.fetch_remote_logs()
                remote_task_helper.fetch_remote_array_states_and_reconcile()
            else:
                # TODO: implement
                pass

    def upload_drypipe_for_remote_instance(self):
        if self.is_remote_execution_on_master_site():
            remote_task_helper = RemotePipelineSpecs(self)
            if self.is_slurm_array_parent():
                remote_task_helper.upsync_drypipe_code()
                #TODO: implement
        else:
            raise Exception(f"{self.task_key} is not a remote task, or not calling from master site")


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
