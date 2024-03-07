import collections
import inspect
import json
import os
import re
import sys
import textwrap
from pathlib import Path

from dry_pipe.core_lib import PortablePopen

from dry_pipe.task import Task, TaskStep, TaskInput, TaskOutput


class DryPipe:

    annotated_python_task_by_name = {}

    @staticmethod
    def python_call(tests=[]):
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


    def __init__(self, key, _consumes={}, _produces={},
                 dsl=None, task_steps=[],
                 task_conf=None, pipeline_instance=None, is_slurm_array_child=None,
                 is_slurm_parent=None, max_simultaneous_jobs_in_slurm_array=None, children_tasks=None):

        self.key = key
        self.dsl = dsl
        self._consumes = _consumes
        self._produces = _produces
        self.task_steps = task_steps
        self.task_conf = task_conf
        self.pipeline_instance = pipeline_instance
        self.is_slurm_array_child = is_slurm_array_child
        self.is_slurm_parent = is_slurm_parent
        self.children_tasks = children_tasks
        self.max_simultaneous_jobs_in_slurm_array = max_simultaneous_jobs_in_slurm_array

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
                #elif isinstance(v, FileSet):
                #    yield k, v
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
            task_conf = task_conf.override_container(kwargs["container"])

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
                task_conf = task_conf.override_python_bin(python_bin)
                task_step = TaskStep(task_conf, python_call=a)

        if task_step is None:
            raise Exception(
                f"invalid args, task.calls(...) can take a single a single positional argument, was given: {args}"
            )

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
            self.is_slurm_parent
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

    def __init__(self, task_conf, pipeline_instance_dir):

        self.user_at_host, self.remote_base_dir, self.ssh_key_file = task_conf.parse_ssh_remote_dest()

        self.remote_instance_work_dir = os.path.join(
            self.remote_base_dir,
            os.path.basename(pipeline_instance_dir),
            ".drypipe"
        )

        self.remote_cli = os.path.join(self.remote_instance_work_dir, "cli")



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
    """

    @staticmethod
    def default():
        return TaskConf("process")

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
            work_on_local_file_copies=None
    ):
        if init_bash_command is not None:
            raise Exception(f"init_bash_command is deprecated")

        if executer_type is None:
            executer_type = "process"

        if executer_type not in ["slurm", "process"]:
            raise Exception(f"invalid executer_type: {executer_type}")

        if executer_type == "slurm":
            if slurm_account is None:
                raise Exception("slurm_account must be specified when executer_type is slurm")
            if slurm_account == "":
                raise Exception("slurm_account can't be ''")

        if executer_type == "process" and slurm_account is not None:
            raise Exception(f"can't specify slurm_account when executer_type is not 'slurm'")

        if fields_from_json is not None:
            self.__dict__.update(fields_from_json)
            if "command_before_task" not in fields_from_json:
                self.command_before_task = None
            return

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
        self.is_slurm_parent = False
        #self.hash_code = None
        self.inputs = []
        self.outputs = []

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

    def remote_pipeline_specs(self, pipeline_instance_dir):
        return RemotePipelineSpecs(self, pipeline_instance_dir)

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

    def save_as_json(self, control_dir, digest):
        def as_json():
            return dict(
                (key, value)
                for key, value in self.__dict__.items() if not callable(value) and not key.startswith('__')
            )

        with open(os.path.join(control_dir, "task-conf.json"), "w") as tc_file:
            d = as_json()
            d = {
                "digest": digest,
                **d
            }
            tc_file.write(json.dumps(d, indent=2))

    @staticmethod
    def from_json_file(control_dir):
        with open(os.path.join(control_dir, "task-conf.json")) as f:
            return TaskConf(fields_from_json=json.loads(f.read()))

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

    def override_container(self, container):
        return TaskConf(
            self.executer_type,
            self.ssh_remote_dest,
            self.slurm_account,
            self.sbatch_options,
            container,
            self.command_before_task,
            self.remote_pipeline_code_dir,
            self.python_bin,
            self.remote_base_dir,
            self.remote_containers_dir,
            self.init_bash_command,
            self.python_interpreter_switches,
            extra_env=self.extra_env
        )

    def override_python_bin(self, python_bin):
        return TaskConf(
            self.executer_type,
            self.ssh_remote_dest,
            self.slurm_account,
            self.sbatch_options,
            self.container,
            self.command_before_task,
            self.remote_pipeline_code_dir,
            python_bin,
            self.remote_base_dir,
            self.remote_containers_dir,
            self.init_bash_command,
            self.python_interpreter_switches,
            extra_env=self.extra_env
        )

    def override_executer(self, executer_type):
        return TaskConf(
            executer_type,
            self.ssh_remote_dest,
            self.slurm_account,
            self.sbatch_options,
            self.container,
            self.command_before_task,
            self.remote_pipeline_code_dir,
            self.python_bin,
            self.remote_base_dir,
            self.remote_containers_dir,
            self.init_bash_command,
            self.python_interpreter_switches,
            extra_env=self.extra_env
        )


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

    def __init__(self, func, tests=[]):
        self.func = func
        self.signature = inspect.signature(self.func)
        self.tests = tests

    def signature_spec(self):
        raise Exception(f"implement me")

    def mod_func(self):
        mod = inspect.getmodule(self.func)
        file_name = os.path.basename(mod.__file__)
        func_name = self.func.__name__
        importable_module_name = f"{mod.__package__}.{file_name[:-3]}:{func_name}"
        return importable_module_name
