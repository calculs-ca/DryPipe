import inspect
import json
import os
import re
import sys
import textwrap
from fnmatch import fnmatch

from dry_pipe.script_lib import PortablePopen
from dry_pipe.utils import bash_shebang
from dry_pipe.internals import \
    Executor, Local, PreExistingFile, IndeterminateFile, ProducedFile, \
    Slurm, IncompleteVar, Val, OutputVar, \
    ValidationError, FileSet, TaskMatcher, PythonCall, Wait, SubPipeline

from dry_pipe.task import Task, TaskStep
from dry_pipe.task_state import TaskState


class DryPipe:

    annotated_python_task_by_name = {}

    @staticmethod
    def python_call(tests=[]):
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
        }
    ):
        """
        :param generator_of_tasks:
        :param pipeline_code_dir:
        :param task_conf:
        :param containers_dir:
        :param env_vars:
        :param remote_task_confs: when a list of TaskConf is given, drypipe prepare-remote-sites will
               upload (rsync) $containers_dir and $pipeline_code_dir to all remote sites
        :param task_groupers:
        example:
            task_groupers={
                "group_by_task_key_last_char": lambda task_key: task_key[-1]
            }
        """

        from dry_pipe.pipeline import Pipeline

        return Pipeline(
            generator_of_tasks, pipeline_code_dir, task_conf, containers_dir, env_vars, remote_task_confs,
            task_groupers
        )

    @staticmethod
    def pipeline_code_dir_for(task_generator_func):
        return os.path.dirname(os.path.abspath(inspect.getmodule(task_generator_func).__file__))


class TaskMatchOutList:
    def __init__(self, task_match_out, product_var_name):
        self.task_match_out = task_match_out
        self.product_var_name = product_var_name

    def fetch(self):
        return [
            task.out.__getattr__(self.product_var_name).fetch()
            for task in self.task_match_out.task_match.tasks
        ]

class TaskMatchOut:
    def __init__(self, task_match):
        self.task_match = task_match

    def __getattr__(self, name):
        task = self.task_match.tasks[0]
        product_var_name = task.out.__getattr__(name)
        if product_var_name is None:
            raise Exception(f"task {task}, does not produce '{product_var_name}'")
        return TaskMatchOutList(self, name)


class TaskMatch:
    def __init__(self, pattern, tasks):
        self.pattern = pattern
        self.tasks = tasks
        self.out = TaskMatchOut(self)


class DryPipeDsl:

    def __init__(self, task_by_keys={}, task_conf=None, pipeline_instance=None, task_namespance_prefix=""):

        self.pipeline_instance = pipeline_instance
        self.task_conf = task_conf or TaskConf("process")
        self.task_namespance_prefix = task_namespance_prefix
        self.task_by_keys = task_by_keys

    def sub_pipeline(self, pipeline, namespace_prefix):
        return SubPipeline(pipeline, namespace_prefix, self)

    def with_completed_matching_tasks(self, *patterns):

        task_matchers = []

        for pattern in patterns:

            tasks = []

            for key, task in self.task_by_keys.items():
                if fnmatch(key, pattern):
                    self.pipeline_instance.dag_determining_tasks_ids.add(key)
                    if not task.has_completed():
                        return []
                    else:
                        tasks.append(task)

            if len(tasks) == 0:
                raise Exception(f"with_completed_matching_tasks({pattern}) matched zero tasks")

            task_matchers.append(TaskMatch(pattern, tasks))

        if len(task_matchers) == 1:
            yield task_matchers[0]
        else:
            yield tuple(task_matchers)

    def with_completed_tasks(self, *args):

        def it_completed_tasks():
            for a in args:
                if isinstance(a, str):
                    key = a
                elif isinstance(a, Task):
                    key = a.key
                else:
                    raise ValidationError(
                        f"illegal argument {type(a)} given to with_tasks, must be Task or a string Task key"
                    )

                self.pipeline_instance.dag_determining_tasks_ids.add(key)

                t = self.task_by_keys.get(key)

                if t is None:
                    break
                else:
                    state = t.get_state()
                    if state is not None and state.is_completed():
                        yield t
                    else:
                        break

        number_of_tasks = len(args)

        if number_of_tasks == 0:
            raise ValidationError(f"must supply at least one task key")

        completed_tasks = list(it_completed_tasks())

        if number_of_tasks == len(completed_tasks):
            if number_of_tasks == 1:
                yield completed_tasks[0]
            else:
                yield tuple(completed_tasks)

    def wait_for(self, tasks):
        return Wait(tasks)

    def var(self, type=str, may_be_none=False):
        return IncompleteVar(type, may_be_none)

    def val(self, v):
        return Val(v)

    def file(self, name, manage_signature=None):

        if type(name) != str:
            raise ValidationError(f"invalid file name, must be a string {name}")

        return IndeterminateFile(name, manage_signature)

    def fileset(self, glob_pattern):

        return FileSet(glob_pattern)

    def matching_tasks(self, task_keys_glob_pattern):
        raise Exception(f"deprecated")
        #return TaskMatcher(task_keys_glob_pattern)

    def task(self,
             key,
             task_conf=None):

        if task_conf is None:
            task_conf = self.task_conf

        if key is None or key == "":
            raise Exception(f"invalid key given to task(...): '{key}'")

        key = f"{self.task_namespance_prefix}{key}"

        return TaskBuilder(
            key=key,
            _produces={},
            _consumes={},
            dsl=self,
            task_conf=task_conf,
            pipeline_instance=self.pipeline_instance
        )


class TaskBuilder:


    def __init__(self, key, _consumes={}, _produces={},
                 dsl=None, task_steps=[],
                 _upstream_task_completion_dependencies=None, _props=None, task_conf=None, pipeline_instance=None):

        self.key = key
        self.dsl = dsl
        self._props = _props or {}
        self._consumes = _consumes
        self._upstream_task_completion_dependencies = _upstream_task_completion_dependencies or []
        self._produces = _produces
        self.task_steps = task_steps
        self.task_conf = task_conf
        self.pipeline_instance = pipeline_instance

    def _deps_from_kwargs(self, kwargs):

        def deps():
            for k, v in kwargs.items():
                if isinstance(v, IndeterminateFile):
                    yield k, v
                elif isinstance(v, ProducedFile) or isinstance(v, Val) or isinstance(v, OutputVar):
                    yield k, v
                elif isinstance(v, TaskMatcher):
                    yield k, v
                else:
                    raise ValidationError(
                        f"_consumes can only take DryPipe.file() or _consumes(a_file=other_task.out.name_of_file()" +
                        f"task(key={self.key}) was given {type(v)}",
                        ValidationError.consumes_has_invalid_kwarg_type
                    )
        return {
            ** self._consumes,
            ** dict(deps())
        }

    def consumes(self, *args, **kwargs):

        def upstream_task_completion_dependencies():
            for o in args:
                if isinstance(o, Task):
                    yield o
                else:
                    raise ValidationError(
                        f"{self}._consumes(...) must be a list of key=value, or a task, whose completion is depended upon"
                    )

        return TaskBuilder(** {
            ** vars(self),
            ** {"_upstream_task_completion_dependencies": list(upstream_task_completion_dependencies())},
            ** {"_consumes": {
                ** self._consumes,
                ** dict(self._deps_from_kwargs(kwargs))
                }
            }
        })

    def props(self, **kwargs):
        return TaskBuilder(** {
            ** vars(self),
            ** {"_props": kwargs}
        })

    def produces(self, *args, **kwargs):

        if len(args) > 0:
            raise ValidationError(
                f"DryPipe.produces(...) can't take positional args, use the form produce(var_name=...)",
                ValidationError.produces_cant_take_positional_args
            )

        def outputs():
            for k, v in kwargs.items():
                if isinstance(v, IndeterminateFile):
                    yield k, v
                elif isinstance(v, IncompleteVar):
                    yield k, v
                elif isinstance(v, FileSet):
                    yield k, v
                else:
                    raise ValidationError(
                        f"produces takes only DryPipe.file or DryPipe.vars, ex:\n " +
                        "1:    task(...).produces(var_name=DryPipe.file('abc.tsv'))\n"
                        " 2:    task(...).produces(vars=DryPipe.vars(x=123,s='a'))",
                        ValidationError.produces_only_takes_files
                    )

        return TaskBuilder(** {
            ** vars(self),
            ** {"_produces": dict(list(outputs()))}
        })

    def calls(self, *args, **kwargs):

        task_conf = self.task_conf
        container = kwargs.get("container")
        if container is not None:
            task_conf = task_conf.override_container(container)

        executer_type = kwargs.get("executer_type")
        if executer_type is not None:
            task_conf = task_conf.override_executer(executer_type)

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
                        raise ValidationError(
                            f"invalid arg to clause:\n ...calls({a})\nvalid arg is a script file (.sh suffix), " +
                            "or a code block with shebang (ex):\n" +
                            f"{bash_shebang}"
                            "echo '...something...'"
                        )
            if isinstance(a, PythonCall):
                python_bin = kwargs.get("python_bin") or self.dsl.task_conf.python_bin or sys.executable
                task_conf = task_conf.override_python_bin(python_bin)
                task_step = TaskStep(task_conf, python_call=a)

        if task_step is None:
            raise ValidationError(
                f"invalid args, task.calls(...) can take a sigle a single positional argument, was given: {args}",
                ValidationError.call_has_bad_arg
            )

        return TaskBuilder(** {
            ** vars(self),
            "task_steps": self.task_steps + [task_step]
        })

    def __call__(self):
        return Task(self)


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


class TaskConf:

    @staticmethod
    def default():
        return TaskConf("process")

    def __init__(
            self,
            executer_type=None,
            ssh_specs=None,
            slurm_account=None,
            sbatch_options=[],
            container=None,
            command_before_launch_container=None,
            remote_pipeline_code_dir=None,
            python_bin=None,
            remote_base_dir=None,
            remote_containers_dir=None,
            init_bash_command=None,
            python_interpreter_switches=["-u"],
            fields_from_json=None
    ):

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
            return

        self.executer_type = executer_type
        self.ssh_specs = ssh_specs
        self.slurm_account = slurm_account
        self.sbatch_options = sbatch_options
        self.container = container
        self.command_before_launch_container = command_before_launch_container
        self.remote_pipeline_code_dir = remote_pipeline_code_dir
        self.python_bin = python_bin
        self.remote_base_dir = remote_base_dir
        self.remote_containers_dir = remote_containers_dir
        self.init_bash_command = init_bash_command
        self.python_interpreter_switches = python_interpreter_switches

        if self.ssh_specs is not None:
            ssh_specs_parts = self.ssh_specs.split(":")
            if len(ssh_specs_parts) == 2:
                ssh_username_ssh_host, self.key_filename = ssh_specs_parts
            elif len(ssh_specs_parts) == 1:
                ssh_username_ssh_host = ssh_specs_parts[0]
                self.key_filename = "~/.ssh/id_rsa"
            else:
                raise Exception(f"bad ssh_specs format: {self.ssh_specs}")

            self.ssh_username, self.ssh_host = ssh_username_ssh_host.split("@")

            self.remote_site_key = ":".join([
                self.ssh_username,
                self.ssh_host,
                self.remote_base_dir
            ])

    def as_json(self):
        return dict(
            (key, value)
            for key, value in self.__dict__.items() if not callable(value) and not key.startswith('__')
        )

    @staticmethod
    def from_json(json_dict):
        return TaskConf(fields_from_json=json_dict)

    @staticmethod
    def from_json_file(json_file):
        with open(json_file) as f:
            return TaskConf.from_json(json.loads(f.read()))

    def is_remote(self):
        return self.ssh_specs is not None

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
            self.ssh_specs,
            self.slurm_account,
            self.sbatch_options,
            container,
            self.command_before_launch_container,
            self.remote_pipeline_code_dir,
            self.python_bin,
            self.remote_base_dir,
            self.remote_containers_dir,
            self.init_bash_command,
            self.python_interpreter_switches
        )

    def override_python_bin(self, python_bin):
        return TaskConf(
            self.executer_type,
            self.ssh_specs,
            self.slurm_account,
            self.sbatch_options,
            self.container,
            self.command_before_launch_container,
            self.remote_pipeline_code_dir,
            python_bin,
            self.remote_base_dir,
            self.remote_containers_dir,
            self.init_bash_command,
            self.python_interpreter_switches
        )

    def override_executer(self, executer_type):
        return TaskConf(
            executer_type,
            self.ssh_specs,
            self.slurm_account,
            self.sbatch_options,
            self.container,
            self.command_before_launch_container,
            self.remote_pipeline_code_dir,
            self.python_bin,
            self.remote_base_dir,
            self.remote_containers_dir,
            self.init_bash_command,
            self.python_interpreter_switches
        )

    _remote_ssh_executers = {}

    @staticmethod
    def _get_or_create_remote_ssh(ssh_username, ssh_host, remote_base_dir, key_filename, command_before_launch_container):
        from dry_pipe.ssh_executer import RemoteSSH

        return RemoteSSH(ssh_username, ssh_host, remote_base_dir, key_filename, command_before_launch_container)

    def create_executer(self):

        def remote_ssh():
            from dry_pipe.ssh_executer import RemoteSSH

            if self.remote_base_dir is None:
                raise Exception("A task_conf with ssh must have remote_base_dir not None")

            return RemoteSSH(
                self.ssh_username, self.ssh_host, self.remote_base_dir, self.key_filename,
                self.command_before_launch_container
            )

        if self.executer_type == "process":
            if self.is_remote():
                return remote_ssh()
            else:
                return Local(self.command_before_launch_container)
        else:
            if self.is_remote():
                e = remote_ssh()
                e.slurm = Slurm(self.slurm_account, self.sbatch_options)
                return e
            else:
                return Slurm(self.slurm_account, self.sbatch_options)
