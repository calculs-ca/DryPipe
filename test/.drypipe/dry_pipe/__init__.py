import collections
import inspect
import json
import os
import copy
import re
import shutil
import sys
import textwrap
from pathlib import Path
from tempfile import TemporaryDirectory

from dry_pipe.core_lib import PortablePopen, exec_remote, invoke_rsync

from dry_pipe.task import Task, TaskStep, TaskInput, TaskOutput, FileSet
from dry_pipe.state_file_tracker import StateFileTracker


class DryPipe:

    annotated_python_task_by_name = {}

    @staticmethod
    def python_call(tests=()):
        """
        annotation for methods that are called by Tasks, i.e. to go in a task's calls(...) clause
        :param tests: test cases
        A test case is a dict where keys map to arguments of the function.

        .. highlight:: python
        .. code-block:: python
            @DryPipe.python_call(test=[{"a":123, "b": "abc"}, {"a": 987, "b": "z"}])
            def my_func(a, b, test=None):
                if test is not None:
                    print(f"I'm being tested, a={a} and b={b}")
                else:
                    print("normal call")

        Ex, to run the 2nd test above with the CLI:

        .. highlight:: shell
        .. code-block:: shell
            drypipe test <module>.my_func:1

        THe above call is equivalent to:
        .. highlight:: python
        .. code-block:: python
            t = {"a": 987, "b": "z"}
            my_func(**t, t)


        where idx is
        """
        return lambda func: PythonCall(func, tests)

    @staticmethod
    def create_pipeline(
        generator_of_tasks,
        pipeline_code_dir=None,
        task_conf=None,
        containers_dir=None,
        env_vars=None,
        remote_task_confs=None,
        task_groupers={
            "by_task_type": Task.key_grouper
        },
        pipeline_code_dir_ls_command=None
    ):
        """
        :param generator_of_tasks: a python generator function that generates Tasks
        :param pipeline_code_dir: The code source root of the pipeline. Defaults to the directory containing the python file containing the DAG generator
        :param task_conf:
        :param containers_dir:
        :param env_vars:
        :param remote_task_confs: when a list of TaskConf is given, drypipe prepare-remote-sites will
               upload (rsync) $containers_dir and $pipeline_code_dir to all remote sites
        :param task_groupers:
        .. highlight:: python
        .. code-block:: python

            DryPipe.create_pipeline(
                my_dag_generator,
                task_groupers={
                    "group_by_task_key_last_char": lambda task_key: task_key[-1]
                }
            )
        """

        from dry_pipe.pipeline import Pipeline

        return Pipeline(
            generator_of_tasks,
            pipeline_code_dir,
            task_conf,
            containers_dir,
            task_groupers,
            pipeline_code_dir_ls_command
        )

    @staticmethod
    def pipeline_code_dir_for(task_generator_func):
        return os.path.dirname(os.path.abspath(inspect.getmodule(task_generator_func).__file__))

    @staticmethod
    def load_pipeline(pipeline_instance_dir):
        raise NotImplementedError()



class DryPipeDsl:

    def __init__(self, task_by_keys={}, task_conf=None, pipeline_instance=None, task_namespance_prefix=""):

        self.pipeline_instance = pipeline_instance
        self.task_conf = task_conf or TaskConf("process")
        self.task_namespance_prefix = task_namespance_prefix
        self.task_by_keys = task_by_keys

    def file_in_pipeline_instance_dir(self, file_name, must_exist=True):
        f = os.path.join(self.pipeline_instance.pipeline_instance_dir, file_name)
        if must_exist and not os.path.exists(f):
            raise Exception(f"file not found {f}")
        return f

    def sub_pipeline(self, pipeline, namespace_prefix):
        """
        :param pipeline: the sub pipeline
        :param namespace_prefix: The prefix for all task keys of the sub pipeline
        :return: an instance of SubPipeline, must be yielded
        """
        return SubPipeline(pipeline, namespace_prefix, self)


class TaskBuilder:


    def __init__(
         self, key, _consumes={}, _produces={},
         dsl=None, task_steps=[],
         task_conf=None, pipeline_instance=None, is_slurm_array_child=None,
         is_slurm_parent=None, max_simultaneous_jobs_in_slurm_array=None, children_tasks=None,
         state_file_tracker=None, downstream_resets=()
    ):

        for illegal_char in ['"', "'", "{", "}", " "]:
            if illegal_char in key:
                raise Exception(f"illegal character {illegal_char} in task key {key}")

        self.key = key
        self.dsl = dsl
        self._consumes = _consumes
        self._produces = _produces
        self.task_steps = task_steps
        self.task_conf = task_conf
        self.pipeline_instance = pipeline_instance
        self.state_file_tracker = state_file_tracker
        self.is_slurm_array_child = is_slurm_array_child
        self.is_slurm_parent = is_slurm_parent
        self.children_tasks = children_tasks
        self.max_simultaneous_jobs_in_slurm_array = max_simultaneous_jobs_in_slurm_array
        self.downstream_resets = downstream_resets

    def slurm_array_parent(self, children_tasks, max_simultaneous_jobs=None):
        """
        :param children_tasks:
        :param max_simultaneous_jobs:
        :return:
            200 child tasks, and max_simultaneous_jobs=None will give:
            --array=0-200
            432 child tasks, and max_simultaneous_jobs=50 will give:
            --array=0-200%50
        """
        return TaskBuilder(** {
            ** vars(self),
            ** {
                "is_slurm_parent": True,
                "max_simultaneous_jobs_in_slurm_array": max_simultaneous_jobs
            }
        }).inputs(children_tasks=children_tasks)

    def inputs(self, *args, **kwargs):
        """
        The inputs clause
        :param args:
        :param kwargs:
        :return a new :py:meth:`dry_pipe.TaskBuilder` with the added inputs declaration
        """

        def g_o(k, v):
            if v.is_file():
                yield k, TaskInput(k, v.type, upstream_task_key=v.task_key,name_in_upstream_task=v.produced_file_name)
            else:
                yield k, TaskInput(k, v.type, upstream_task_key=v.task_key, name_in_upstream_task=v.name)

        def deps():
            for k, v in kwargs.items():
                if isinstance(v, int):
                    yield k, TaskInput(k, 'int', value=v)
                elif isinstance(v, str):
                    yield k, TaskInput(k, 'str', value=v)
                elif isinstance(v, float):
                    yield k, TaskInput(k, 'float', value=v)
                elif isinstance(v, TaskOutput):
                    yield from g_o(k, v)
                elif isinstance(v, Path):
                    yield k, TaskInput(k, 'file', file_name=str(v))
                elif isinstance(v, list):
                    yield k, TaskInput(k, 'task-list', value=v)
                else:
                    raise Exception(
                        f"inputs can only take DryPipe.file() or inputs(a_file=other_task.out.name_of_file()" +
                        f"task(key={self.key}) was given {type(v)}"
                    )

        def deps_from_args():
            for o in args:
                if isinstance(o, TaskOutput):
                    yield from g_o(o.name, o)
                elif isinstance(o, Task):
                    yield o.key, TaskInput(f"task:{o.key}", 'task', value=o.key)
                else:
                    raise Exception(
                        f"bad arg: {o} passed to {self}"
                    )

        for o, count in collections.Counter([k for k, _ in deps_from_args()]).items():
            if count > 1:
                raise Exception(f"duplicate variable {o} in task(key={self.key}).inputs(...) clause")

        return TaskBuilder(** {
            ** vars(self),
            ** {"_consumes": {
                    ** self._consumes,
                    ** dict(deps_from_args()),
                    ** dict(deps())
                }
            }
        })

    def outputs(self, *args, **kwargs):
        """
        The produces clause
        :param args:
        :param kwargs:
        :return a new :py:meth:`dry_pipe.TaskBuilder` with the added produces declaration
        """

        if len(args) > 0:
            raise Exception(
                f"DryPipe.produces(...) can't take positional args, use the form produce(var_name=...)"
            )

        def outputs():
            for k, v in kwargs.items():
                if v == int:
                    yield k, TaskOutput(k, 'int', task_key=self.key)
                elif v == str:
                    yield k, TaskOutput(k, 'str', task_key=self.key)
                elif v == float:
                    yield k, TaskOutput(k, 'float', task_key=self.key)
                elif isinstance(v, Path):
                    yield k, TaskOutput(k, 'file', task_key=self.key, produced_file_name=v.name)
                elif isinstance(v, FileSet):
                    yield k, TaskOutput(k, 'file_set', task_key=self.key, file_set=v)
                else:
                    raise Exception(
                        f"invalid arg in Task({self.key}).inputs({k})\n"+
                        f"produces takes only DryPipe.file or DryPipe.vars, ex:\n " +
                        "1:    task(...).produces(var_name=DryPipe.file('abc.tsv'))\n"
                        " 2:    task(...).produces(vars=DryPipe.vars(x=123,s='a'))"
                    )

        return TaskBuilder(** {
            ** vars(self),
            ** {"_produces": dict(list(outputs()))}
        })

    def calls(self, *args, **kwargs):
        """
        Adds a calls() to the task declaration
        :param args: a python function annotated with :py:meth:`.python_call` or a bash snippet with a proper shebang
        :return a new :py:meth:`dry_pipe.TaskBuilder` with the added call
        """

        task_conf = self.task_conf
        if "container" in kwargs:
            task_conf = task_conf.override(container=kwargs["container"])

        task_step = None

        if len(args) == 1:
            a = args[0]
            if type(a) == str:
                if a.endswith(".sh"):
                    task_step = TaskStep(task_conf, shell_script=a)
                else:
                    script_text = textwrap.dedent(a)
                    if re.match("\\n(\\w*)#!/.*", script_text):
                        start_idx = script_text.find("#!")
                        task_step = TaskStep(task_conf, shell_snippet=script_text[start_idx:-1])
                    else:
                        raise Exception(
                            f"invalid arg to clause:\n ...calls({a})\nvalid arg is a script file (.sh suffix), " +
                            "or a code block with shebang (ex):\n" +
                            f"#!/usr/bin/env bash"
                            "echo '...something...'"
                        )
            if isinstance(a, PythonCall):
                python_bin = kwargs.get("python_bin") or self.dsl.task_conf.python_bin or sys.executable
                task_conf = task_conf.override(python_bin=python_bin)
                task_step = TaskStep(task_conf, python_call=a)

        if task_step is None:
            raise Exception(
                f"invalid args, task.calls(...) can take a single a single positional argument, was given: {args}"
            )

        if "sbatch_options" in kwargs:

            if len(self.task_steps) == 0:
                raise Exception(
                    f"declaring sbatch_options on the first step serves no purpose, "+
                    "please declare sbatch_options in task_conf"
                )

            sbo = kwargs["sbatch_options"]
            if sbo is not None:
                def raiz(s):
                    raise Exception(
                        f"sbatch_options must be a list of strings or ints or floats, was given: {s}"
                    )
                if not isinstance(sbo, list):
                    raiz(sbo)
                bad_args = Utils.filter_non_str_int_float_arg(sbo)
                if bad_args is None:
                    task_step.sbatch_options = sbo
                else:
                    raiz(bad_args)

        return TaskBuilder(** {
            ** vars(self),
            "task_steps": self.task_steps + [task_step]
        })

    def __call__(self):
        return Task(
            self.key,
            self._consumes,
            self._produces,
            self.pipeline_instance,
            self.task_steps,
            self.task_conf,
            self.is_slurm_array_child,
            self.max_simultaneous_jobs_in_slurm_array,
            self.is_slurm_parent,
            self.state_file_tracker,
            self.downstream_resets
        )


def host_has_sbatch():

    with PortablePopen(
        ["which", "sbatch"]
    ) as p:

        p.wait()

        if p.returncode != 0:
            return False

        out = p.stdout.read().strip()

        if out == "":
            return False

        if out.endswith("/sbatch"):
            return True

        raise Exception(f"Funny return from which sbatch: {out}")

class RemotePipelineSpecs:

    def __init__(self, task_process):

        self.task_process = task_process
        task_conf = task_process.task_conf
        pipeline_instance_dir = task_process.pipeline_instance_dir

        self.user_at_host, self.remote_base_dir, self.ssh_key_file = task_conf.parse_ssh_remote_dest()

        self.task_conf = task_conf

        self.remote_instance_work_dir = os.path.join(
            self.remote_base_dir,
            os.path.basename(pipeline_instance_dir),
            ".drypipe"
        )

        self.remote_control_dir = os.path.join(self.remote_instance_work_dir, self.task_process.task_key)

        self.remote_cli = os.path.join(self.remote_instance_work_dir, "cli")

        self.ssh_remote_dest = f"{self.user_at_host}:{self.remote_base_dir}"

        self.absolute_pid = os.path.abspath(pipeline_instance_dir)

        self.pid_base_name = os.path.basename(pipeline_instance_dir)

        self.remote_pid = os.path.join(self.remote_base_dir, self.pid_base_name)

        self.task_logger = task_process.task_logger

        if task_conf.run_as_group is None:
            self.rsync_chown_arg = ""
        else:
            user = self.user_at_host.split("@")[0].strip()
            self.rsync_chown_arg = f"--chown={user}:{task_conf.run_as_group}"


    def __str__(self, *args, **kwargs):
        return f"RemotePipelineSpecs({self.user_at_host},...)"

    def _write_site_args_into(self, f):
        f.write(f"DRYPIPE_EXTERNAL_FILES_ROOT={self.remote_pid}/external-file-deps\n")

    def gen_remote_site_env_file(self):
        overrides_file = os.path.join(self.task_process.control_dir, f"site.env")
        with open(overrides_file, "w") as f:
            self._write_site_args_into(f)

        return overrides_file

    def lock_file_for_remote_site(self):
        rbd = self.remote_base_dir.replace("/", "_")
        return Path(self.task_process.pipeline_work_dir, f"site-{self.user_at_host}{rbd}.lock")

    def gen_and_upload_task_conf_remote_overrides(self, upload_cmd):
        overrides_basename = "site.env"
        with TemporaryDirectory(dir=self.task_process.control_dir) as tmp_dir:
            overrides_file = os.path.join(tmp_dir, overrides_basename)
            with open(overrides_file, "w") as tmp_overrides:
                self._write_site_args_into(tmp_overrides)

            # make a copy, just for transparency (self documenting)
            shutil.copy(
                overrides_file,
                os.path.join(self.task_process.control_dir, f"site-{self.user_at_host}.env")
            )
            dst = f"{self.user_at_host}:{self.remote_pid}/.drypipe/site.env"

            upload_cmd(overrides_file, dst)

    def dump_unique_files_in_file(self, files, dep_file_name):
        dep_file_path = os.path.join(self.task_process.control_dir, dep_file_name)
        uniq_files = set()
        with open(dep_file_path, "w") as tf:
            for dep_file in files:
                if dep_file not in uniq_files:
                    tf.write(dep_file)
                    tf.write("\n")
                    uniq_files.add(dep_file)

        return dep_file_path

    def gen_result_files(self):
        from dry_pipe.task_process import TaskProcess

        def g(task_key):
            p = TaskProcess(
                os.path.join(self.task_process.pipeline_work_dir, task_key),
                ensure_all_upstream_deps_complete=False
            )
            for file in p.outputs.rsync_file_list():
                yield file

        if self.task_process.is_slurm_array_parent():
            for child_task_key in self.task_process.children_task_keys():
                yield from g(child_task_key)
        else:
            yield from g(self.task_process.task_key)

        yield f".drypipe/{self.task_process.task_key}/file-sets-rsync-list.txt"

    def reconcile_local_array_states_with_remote_state(self, remote_exec_result):

        for child_task_key_task_state in remote_exec_result.split("\n"):
            child_task_key_task_state = child_task_key_task_state.strip()
            if child_task_key_task_state == "":
                continue
            if child_task_key_task_state.startswith("implicit") and "=" in child_task_key_task_state:
                continue
            child_task_key, child_task_state = child_task_key_task_state.split("/")
            child_task_control_dir = os.path.join(self.task_process.pipeline_work_dir, child_task_key)
            child_state_file_path = StateFileTracker.find_state_file_path_if_exists(child_task_control_dir)
            if child_state_file_path is not None:
                actual_state = os.path.join(child_task_control_dir, child_task_state)
                os.rename(child_state_file_path.path, actual_state)

    def fetch_remote_array_states_and_reconcile(self):
        remote_cli = os.path.join(self.remote_instance_work_dir, "cli")
        remote_exec_result = exec_remote(self.user_at_host, [
            "python3",
            remote_cli,
            "list-states",
            f"--pipeline-instance-dir={self.remote_pid}",
            f"--task-key={self.task_process.task_key}"
        ])

        self.task_logger.debug("remote states:\n %s", remote_exec_result)
        self.reconcile_local_array_states_with_remote_state(remote_exec_result)
        # TODO : reconcile logs

    def fetch_remote_logs(self):

        remote_src = f"{self.user_at_host}:{self.remote_base_dir}/{self.pid_base_name}/.drypipe"

        dst = f"{self.absolute_pid}/.drypipe"

        cmd = f"rsync --prune-empty-dirs -va --update --include='*/' --include='*/*.log' --exclude='*' {remote_src}/ {dst}/"

        self.task_logger.debug("rsync remote logs: %s", cmd)

        invoke_rsync(cmd)

    def fetch_remote_task_logs(self):

        task_key = self.task_process.task_key

        remote_src = f"{self.user_at_host}:{self.remote_base_dir}/{self.pid_base_name}/.drypipe/{task_key}"

        dst = f"{self.absolute_pid}/.drypipe/{task_key}"

        cmd = f"rsync --prune-empty-dirs -va --update --include='*/' --include='*/*.log' --exclude='*' {remote_src}/ {dst}/"

        self.task_logger.debug("rsync remote logs: %s", cmd)

        invoke_rsync(cmd)
    def fetch_sacct_dumps(self):

        remote_src = f"{self.user_at_host}:{self.remote_base_dir}/{self.pid_base_name}/.drypipe"

        dst = f"{self.absolute_pid}/.drypipe"

        cmd = f"rsync --prune-empty-dirs -va --update --include='*/' --include='*/*.sacct.out' --exclude='*' {remote_src}/ {dst}/"

        self.task_logger.debug("rsync remote sacct dumps: %s", cmd)

        invoke_rsync(cmd)

    def upsync_drypipe_code(self):

        remote_dst = f"{self.user_at_host}:{self.remote_base_dir}/{self.pid_base_name}/.drypipe/dry_pipe/"

        src = f"{self.task_process.pipeline_work_dir}/dry_pipe/"

        cmd = f"rsync -a --include='*.py' {src} {remote_dst}"

        self.task_logger.debug("rsync upload drypipe code: %s", cmd)

        invoke_rsync(cmd)


    def remote_exec(self, cmd, args=()):

        remote_cli = os.path.join(self.remote_instance_work_dir, "cli")

        def g():
            yield "python3"
            yield remote_cli
            yield cmd
            yield f"--pipeline-instance-dir={self.remote_pid}"
            yield f"--task-key={self.task_process.task_key}"
            yield from args

        cmd = list(g())

        if self.task_conf.run_as_group is not None:
            cmd = " ".join(cmd)
            cmd = [
                "newgrp", self.task_conf.run_as_group, "<<<", f"'{cmd}'"
            ]

        return exec_remote(
            self.user_at_host,
            cmd,
            logger_func=self.task_logger.info
        )

    def remote_exec_json_results(self, cmd, args=()):
        res = self.remote_exec(cmd, args)
        res = res.strip()
        for line in res.split("\n"):
            if not line.startswith("{"):
                continue
            return json.loads(line)

        raise Exception(f"Remote execution returned no JSON results {res}")


class TaskConf:
    """
    :param executer_type: 'process' or 'slurm'
    :param ssh_remote_dest: me@a-host:PORT:/path/to/ssh-private-key
    :param slurm_account: a slurm username
    :param sbatch_options: list of string for every sbatch option, ex: ["--time=0:1:00", "--mem=30G", "--cpus-per-task=24"]
    :param container: the file name of the container, ex: my-container.sif (without path, containers
        the file must exist in
          $__containers_dir/<container>
        the default value of $__containers_dir is $__pipeline_code_dir/containers
        Note: for remote tasks (when ssh_remote_dest is defined, the container file must exist in `remote_containers_dir`)
    :param command_before_task:
    :param remote_pipeline_code_dir:
    :param python_bin:
     By default, will use the same python bin as the one running the cli
     to run in a specific virtualenv:
        /path/to/my_virtualenv/bin/python
     to run the task in a miniconda env:
        /path/to/my-miniconda3/envs/<my-conda-env>/bin/python
     if running in a container, it should point to the python executable in the container, most likel:
        /usr/bin/python3
    :param remote_base_dir:
        the remote directory that will contain the $__pipeline_instance_dir structure
    :param remote_containers_dir:
        the directory containing the container sif files
    :param python_interpreter_switches:
        extra switches to add to the python executable that will launch the python call (only applies to PythonCall)

    :auto_restart_condition_regexp_per_log_file
      Only implemented for array tasks.
      Regexps used for deciding if a failed task should be auto restarted, ex:
      { "drypipe.log": [".*BrokenPipeError.*"],
        "out.log": ["Bus\\ error", None]
      }
      special value None means that the absence of the log file is sufficient reason for restart

    """

    @staticmethod
    def default(extra_env=None):
        return TaskConf("process",extra_env=extra_env)

    def __init__(
            self,
            executer_type=None,
            ssh_remote_dest=None,
            slurm_account=None,
            sbatch_options=[],
            container=None,
            command_before_task=None,
            remote_pipeline_code_dir=None,
            python_bin=None,
            remote_base_dir=None,
            remote_containers_dir=None,
            init_bash_command=None,
            python_interpreter_switches=["-u"],
            fields_from_json=None,
            extra_env=None,
            label=None,
            work_on_local_file_copies=None,
            run_as_group=None,
            apptainer_exec_args=None,
            globus_transfer=None,
            globus_local_path_rewrite=None,
            auto_restart_condition_regexp_per_log_file=None,
            downstream_resets=(),
            use_squeue=False,
            external_files_root=None
    ):

        self.is_slurm_parent = False
        self.is_on_remote_site = False
        self.external_files_root = external_files_root

        if init_bash_command is not None:
            raise Exception(f"init_bash_command is deprecated")

        if executer_type is None:
            executer_type = "process"

        if executer_type not in ["slurm", "process"]:
            raise Exception(f"invalid executer_type: {executer_type}")

        if executer_type == "slurm":
            if slurm_account == "":
                raise Exception("slurm_account can't be '', use None to omit slurm --account argument")

        if executer_type == "process" and slurm_account is not None:
            raise Exception(f"can't specify slurm_account when executer_type is not 'slurm'")

        if fields_from_json is not None:
            self.__dict__.update(fields_from_json)
            if "command_before_task" not in fields_from_json:
                self.command_before_task = None
            return

        if isinstance(sbatch_options, str):
            sbatch_options = [sbatch_options]

        self.executer_type = executer_type
        self.ssh_remote_dest = ssh_remote_dest
        self.slurm_account = slurm_account
        self.sbatch_options = sbatch_options
        self.container = container
        self.command_before_task = command_before_task
        self.remote_pipeline_code_dir = remote_pipeline_code_dir
        self.python_bin = python_bin
        self.remote_base_dir = remote_base_dir
        self.remote_containers_dir = remote_containers_dir
        self.init_bash_command = init_bash_command
        self.python_interpreter_switches = python_interpreter_switches
        self.extra_env = extra_env
        self.label = label
        self.work_on_local_file_copies = work_on_local_file_copies
        #self.hash_code = None
        self.inputs = []
        self.outputs = []
        self.run_as_group = run_as_group
        self.apptainer_exec_args = apptainer_exec_args
        self.globus_transfer = globus_transfer
        self.globus_local_path_rewrite = globus_local_path_rewrite
        self.auto_restart_condition_regexp_per_log_file = auto_restart_condition_regexp_per_log_file
        self.downstream_resets = downstream_resets
        self.use_squeue = use_squeue

        if extra_env is not None:
            if not isinstance(extra_env, dict):
                raise Exception(f"extra_env must be a dict of strings, got {type(extra_env)}")
            for k, v in extra_env.items():
                if not isinstance(v, str):
                    raise Exception(f"invalid value given to extra_env['{k}']: {type(v)}, must be str")

        if self.python_bin is None:
            if self.is_remote():
                self.python_bin = "/usr/bin/python3"
            else:
                self.python_bin = sys.executable

    def parse_ssh_remote_dest(self):
        """

         :param ssh_specs:

         me@somehost.org:/remote-base-dir

         me@somehost.org:/remote-base-dir:/x/y/.ssh/id_rsa

        :return:
        """

        ssh_specs_parts = self.ssh_remote_dest.split(":")

        if len(ssh_specs_parts) == 2:
            user_at_host, remote_base_dire = ssh_specs_parts
            ssh_key_file = "~/.ssh/id_rsa"
        elif len(ssh_specs_parts) == 3:
            user_at_host, remote_base_dire, ssh_key_file = ssh_specs_parts
        else:
            raise Exception(
                f"invalid format for ssh_remote_dest {self.ssh_remote_dest} should be: <user>@<host>:/<dir>(:ssh_key)?"
            )

        if user_at_host.endswith("/"):
            user_at_host = user_at_host[:-1]

        return user_at_host, remote_base_dire, ssh_key_file

    def hash_values(self):
        yield self.executer_type
        if self.container is not None:
            yield self.container
        if self.extra_env is not None:
            for k, v in self.extra_env.items():
                yield k
                yield v
        if self.init_bash_command is not None:
            yield self.init_bash_command
        if self.sbatch_options is not None:
            for o in self.sbatch_options:
                yield str(o)
        if self.slurm_account is not None:
            yield self.slurm_account
        if self.python_bin is not None:
            yield self.python_bin
        if self.command_before_task is not None:
            yield self.command_before_task
        if self.run_as_group is not None:
            yield self.run_as_group
        if self.globus_local_path_rewrite is not None:
            yield self.globus_local_path_rewrite
        if self.globus_transfer is not None:
            yield self.globus_transfer
        if self.auto_restart_condition_regexp_per_log_file is not None:
            for k, v in self.auto_restart_condition_regexp_per_log_file.items():
                h1 = k
                h2 = ",".join([str(s) for s in v])
                yield f"{h1}:{h2}"


    def as_json(self):
        return dict(
            (key, value)
            for key, value in self.__dict__.items() if not callable(value) and not key.startswith('__')
        )

    def save_as_json(self, control_dir, digest):

        with open(os.path.join(control_dir, "task-conf.json"), "w") as tc_file:
            d = self.as_json()
            d = {
                "digest": digest,
                **d
            }
            tc_file.write(json.dumps(d, indent=2))

    @staticmethod
    def from_json_file(control_dir):
        with open(os.path.join(control_dir, "task-conf.json")) as f:
            return TaskConf(fields_from_json=json.load(f))

    def is_remote(self):
        return self.ssh_remote_dest is not None

    def is_slurm(self):
        return self.executer_type == "slurm"

    def is_process(self):
        return self.executer_type == "process"

    def has_container(self):
        return self.container is not None

    def _ensure_is_remote(self):
        if not self.is_remote():
            raise Exception(f"can't call this on this non remote TaskConf")

    def uses_singularity(self):
        return self.container is not None

    def override(self, **kwargs):
        fields = vars(self).copy()

        for f in ["is_slurm_parent", "is_on_remote_site", "inputs", "outputs"]:
            del fields[f]

        for k, v in kwargs.items():
            if k not in fields:
                raise Exception(f"{k} is not a valid TaskConf field")

        tc = TaskConf(**{
            **fields,
            **kwargs
        })

        if self.is_slurm_parent:
            tc.is_slurm_parent = True

        return tc

    def with_sbatch_options(self, account=None, mem=None, time=None, cpu_per_task=None, partition=None):

        sbatch_options = []

        if mem is not None:
            sbatch_options.append(f"--mem={mem}")

        if time is not None:
            sbatch_options.append(f"--time={time}")

        if cpu_per_task is not None:
            sbatch_options.append(f"--cpus-per-task={cpu_per_task}")

        if account is not None:
            sbatch_options.append(f"--account={account}")

        if partition is not None:
            sbatch_options.append(f"--partition={partition}")

        return self.override(sbatch_options=sbatch_options)



class AutoRestartManager:

    def __init__(self, auto_restart_condition_regexp_per_log_file, max_restart=3, for_dry_run=False):

        self.max_restart = max_restart
        self.for_dry_run = for_dry_run

        if auto_restart_condition_regexp_per_log_file is None:
            self.auto_restart_condition_regexp_per_log_file = None
        else:
            def compile_regexp(f, pattern):
                try:
                    return re.compile(pattern)
                except Exception as ex:
                    raise Exception(f"Failed to compile regex pattern '{pattern}', for file '{f}'")

            self.auto_restart_condition_regexp_per_log_file = {
                f: [
                    None if r is None else compile_regexp(f, r)
                    for r in regexen
                ]
                for f, regexen in auto_restart_condition_regexp_per_log_file.items()
            }

    def control_dir(self, state_file):
        return state_file.control_dir()

    def restart_file(self, state_file):
        return Path(self.control_dir(state_file), "restarts.tsv")


    def _last_line_of_prev_restarts_per_file_and_restart_count(self, state_file):

        restarts = self.restart_file(state_file)

        last_line_of_prev_restarts_per_file = {
            f: None
            for f in self.auto_restart_condition_regexp_per_log_file.keys()
        }

        restart_decisions_for_missing_log_files = set([])

        if not restarts.exists():
            return last_line_of_prev_restarts_per_file, restart_decisions_for_missing_log_files, 0
        else:

            restart_counter = [0]

            def g():

                yield from last_line_of_prev_restarts_per_file.items()

                with open(restarts, "r") as f:
                    rows = []
                    for line in f:
                        line = line.strip()
                        if line == "":
                            continue
                        if line.startswith("RESET"):
                            rows.clear()
                            continue
                        row = [
                            l.strip() for l in line.split("\t", maxsplit=3)
                        ]
                        log_name, line_in_log, matching_line = row
                        if matching_line == "MISSING":
                            restart_decisions_for_missing_log_files[log_name] = True
                        else:
                            rows.append(row)

                    for row in rows:
                        log_name, line_in_log, ignore = row
                        restart_counter[0] = restart_counter[0] + 1
                        yield log_name, int(line_in_log) if line_in_log != "None" else None

            last_line_of_prev_restarts_per_file = dict(g())

        return  last_line_of_prev_restarts_per_file, restart_decisions_for_missing_log_files, restart_counter[0]

    def _record_restart(self, state_file, log_file_name, line_number, matching_line):
        if self.for_dry_run:
            return
        with open(self.restart_file(state_file), "a") as f:
            f.write(f"{log_file_name}\t{line_number}\t{matching_line.strip()}\n")

    def should_restart_with_details(self, state_file, logger):

        last_line_of_prev_restarts_per_file, restart_decisions_for_missing_log_files, restart_count = \
            self._last_line_of_prev_restarts_per_file_and_restart_count(state_file)

        if restart_count >= self.max_restart:
            logger.info("%s has reached max relaunch %s", state_file.task_key, restart_count)
            return False, 0, restart_count, None

        for f, regexen in self.auto_restart_condition_regexp_per_log_file.items():
            for r in regexen:
                log_file = Path(self.control_dir(state_file), f)

                if r is None:
                    # a task without a log is abnormal, most often as a result of a restartable error,
                    # we restart, but at most once
                    if not log_file.exists() and restart_count == 0:

                        if f in restart_decisions_for_missing_log_files:
                            continue

                        self._record_restart(state_file, f, None, "MISSING_FILE")
                        logger.info(
                            "%s has no log %s, will relaunch", state_file.task_key, log_file
                        )
                        return True, None, restart_count, f
                    else:
                        continue
                else:
                    if not log_file.exists():
                        continue

                match_starting_at_line = last_line_of_prev_restarts_per_file[f]

                with open(log_file) as log_file_h:
                    c = 0

                    last_line_of_match_occurrence = None

                    for line in log_file_h:
                        c += 1
                        if match_starting_at_line is not None and c <= match_starting_at_line:
                            continue

                        if r.match(line):
                            last_line_of_match_occurrence = c

                    if last_line_of_match_occurrence is not None:
                        logger.info(
                            "will relaunch %s, relaunches so far: %s", state_file.task_key, restart_count
                        )
                        self._record_restart(state_file, f, last_line_of_match_occurrence, line)
                        return True, c, restart_count, f

        logger.debug("will NOT relaunch %s", state_file.task_key)

        return False, None, restart_count, None

    def should_restart(self, state_file, logger):
        should_restart, matching_line_number, restart_count, matching_log_filename = \
            self.should_restart_with_details(state_file, logger)
        return should_restart


class ApptainerConf:

    def __init__(self):
        self.exec_args = [
            "--no-mount"
        ]

class SubPipeline:
    def __init__(self, pipeline, task_namespance_prefix, dsl):
        self.pipeline = pipeline
        self.task_namespance_prefix = task_namespance_prefix
        self.dsl = dsl

    def wait_for_tasks(self, *args):
        args = [f"{self.task_namespance_prefix}{a}" for a in args]
        return self.dsl.wait_for_tasks(*args)


class PythonCall:

    def __init__(self, func, tests=()):
        self.func = func
        self.signature = inspect.signature(self.func)
        self.tests = tests
        self.fixed_args = {}

    def signature_spec(self):
        raise Exception(f"implement me")

    def mod_func(self):
        mod = inspect.getmodule(self.func)
        file_name = os.path.basename(mod.__file__)
        func_name = self.func.__name__
        importable_module_name = f"{mod.__package__}.{file_name[:-3]}:{func_name}"
        return importable_module_name



class Utils:


    @staticmethod
    def filter_non_str_int_float_arg(it):

        def g():
            for i in it:
                if not (isinstance(i, str) or isinstance(i, int) or isinstance(i, float)):
                    yield i

        l = list(g())
        if len(l) == 0:
            return None
        else:
            return ",".join(g())
