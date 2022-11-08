import glob
import inspect
import os
import subprocess
import getpass
import logging.config

from dry_pipe.script_lib import PortablePopen
from dry_pipe.utils import count_cpus

LOCAL_PROCESS_IDENTIFIER_VAR = "____DRY_PIPE_TASK"
logger = logging.getLogger(__name__)


class ValidationError(Exception):
    def __init__(self, message, code=None):
        super(ValidationError, self).__init__(message)
        self.code = code

    consumes_has_bad_positional_arg_val = 0

    consumes_has_bad_positional_arg = 1

    consumes_has_invalid_kwarg_type = 2

    produces_cant_take_positional_args = 3

    produces_only_takes_files = 4

    call_has_bad_arg = 5


class Val:

    def __init__(self, value):
        t = type(value)
        if t == int or t == str or t == float:
            self.value = value
            self.typez = t
        elif t == type(int):
            raise ValidationError(f"invalid val type dsl.val(int), you probably want to use 'dsl.var(int)'")
        elif t == t == type(str):
            raise ValidationError(f"invalid val type dsl.var(str), you probably want to use 'dsl.var(str)'")
        elif type(float):
            raise ValidationError(f"invalid val type, dsl.var(float) you probably want to use 'dsl.var(float)'")
        else:
            raise ValidationError(f"invalid val type: {type(value)}, must be int, string, float")

    def type_str(self):
        return _type_str(self.typez)

    def to_hash(self):
        return str(self.value)

    def serialized_value(self):

        if type(self.value) == str:
            return f'"{self.value}"'

        return f"{self.value}"


class IncompleteVar:

    def __init__(self, type=str, may_be_none=True):
        self.type = type
        self.may_be_none = may_be_none


def _type_str(t):
    if t == int:
        return "int"
    elif t == str:
        return "str"
    elif t == float:
        return "float"


class OutputVar:

    def __init__(self, type, may_be_none, name, producing_task):

        supported = [str, int, float]

        if type not in supported:
            raise ValidationError(
                f"var {name} in {producing_task} has invalid type. Supported types are: " +
                ', '.join([_type_str(t) for t in supported])
            )

        self.name = name
        self.type = type
        self.producing_task = producing_task
        self.may_be_none = may_be_none

    def type_str(self):
        return _type_str(self.type)

    def format_for_python(self, v):

        if self.type == int:
            return str(v)
        elif self.type == str:
            return f'"{v}"'
        elif self.type == float:
            return str(v)

        raise Exception(f"unsupported type {self.type}")

    def unformat_for_python(self, v):

        if self.type == int:
            return int(v)
        elif self.type == str:
            s = v.rstrip('\"\'')
            s = s.lstrip('\"\'')
            return s
        elif self.type == float:
            return float(v)

        raise Exception(f"unsupported type {self.type}")

    def input_var(self, var_name_in_consuming_task):
        return InputVar(self, var_name_in_consuming_task)

    def fetch(self):
        varz = self.producing_task.out_vars_by_name()
        v = varz.get(self.name)
        return self.unformat_for_python(v)


class InputVar:

    def __init__(self, output_var, var_name_in_consuming_task):
        self.output_var = output_var
        self.var_name_in_consuming_task = var_name_in_consuming_task

    def write(self, string_value):
        return string_value


class Executor:

    def key(self):
        return None

    def is_remote(self):
        return False


def _process_iter(names):
    import psutil

    for it in psutil.process_iter(['pid', 'exe', 'username', 'environ']):
        yield it


def _is_access_denied(ex):
    from psutil import AccessDenied
    return isinstance(ex, AccessDenied)

class Local(Executor):

    def count_running_tasks(self):

        current_user = getpass.getuser()

        c = 0

        for it in _process_iter(['pid', 'exe', 'username', 'environ']):

            if it.username() != current_user:
                continue
            try:

                if not it.exe().endswith("/bash"):
                    continue

                v = it.environ().get(LOCAL_PROCESS_IDENTIFIER_VAR)

                if v is not None: # and v.startswith("____"):
                    c += 1

            except Exception as ex:
                if not _is_access_denied(ex):
                    raise ex

        return c

    def kill_task(self, task_key):

        current_user = getpass.getuser()

        for it in _process_iter(['pid', 'exe', 'username', 'environ']):

            if it.username() != current_user:
                continue
            try:

                if not it.exe().endswith("/bash"):
                    continue

                v = it.environ().get(LOCAL_PROCESS_IDENTIFIER_VAR)

                if v is not None:
                    if v == f"____{task_key}":
                        it.kill()

            except Exception as ex:
                if not _is_access_denied(ex):
                    raise ex

        #pids = list(get_pids())
        #print(f"pids: {pids}")
        #for pid in pids:
        #    os.kill(pid. signal.SIGKILL)

    def __init__(self, before_execute_bash):
        self.before_execute_bash = before_execute_bash
        self.cpu_count = count_cpus()

    def has_cpu_capacity_to_launch(self):

        return self.count_running_tasks() <= self.cpu_count

    fail_silently_for_test = False

    """
        experimental, for scenario:
        + launcher runs in a LXC container
        + launcher runs tasks with singularity,launch on the physical host of the LXC container  
    """
    def __command_on_sibling_container(self, task, user, back_ground):
        return [
            f"ssh {user}@127.0.0.1 'nohup {task.v_abs_script_file()} " +
            f"  >{task.v_abs_out_log()} 2>{task.v_abs_err_log()} {back_ground}'"
        ]

    def execute(self, task, touch_pid_file_func, wait_for_completion=False, fail_silently=False):
        if wait_for_completion:
            cmd = [task.v_abs_script_file(), "--wait"]
        else:
            cmd = [task.v_abs_script_file()]

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


class Slurm(Executor):

    def __init__(self, account, sbatch_options):

        if account is None:
            raise Exception(f"account account can't be None")

        self.account = account
        self.sbatch_options = sbatch_options

    def execute(self, task, touch_pid_file_func, wait_for_completion=False, fail_silently=False):

        env = {
            **os.environ
        }

        if wait_for_completion:
            cmd = [f"bash -c 'export SBATCH_WAIT=True && {task.v_abs_sbatch_launch_script()}'"]
        else:
            cmd = [task.v_abs_sbatch_launch_script()]

        with PortablePopen(
            cmd,
            shell=True,
            env=env
        ) as p:
            p.wait_and_raise_if_non_zero()


class IndeterminateFile:

    def __init__(self, file_path, manage_signature):
        self.file_path = file_path.strip()
        self.manage_signature = manage_signature

    def produced_file(self, var_name, producing_task):
        return ProducedFile(self.file_path, var_name, self.manage_signature, producing_task)

    def pre_existing_file(self):
        return PreExistingFile(self.file_path, self.manage_signature)


class FileSet:

    def __init__(self, glob_pattern):
        self.glob_pattern = glob_pattern

    def out_file_set(self, producing_task, name_in_producing_task):
        return OutFileSet(self, producing_task, name_in_producing_task)


class OutFileSet:

    def __init__(self, file_set, producing_task, name_in_producing_task):
        self.file_set = file_set
        self.producing_task = producing_task
        self.name_in_producing_task = name_in_producing_task

    def fetch(self):
        if not self.producing_task.get_state().is_completed():
            raise Exception(f"task must be complete before out value can be fetched")

        producing_task_work_dir = self.producing_task.v_abs_work_dir()

        pattern = os.path.join(producing_task_work_dir, self.file_set.glob_pattern)

        def make_path_relative_to_pipeline_instance_dir(p):
            return os.path.relpath(p, producing_task_work_dir)

        for f in glob.glob(pattern):
            f = make_path_relative_to_pipeline_instance_dir(f)
            yield ProducedFile(f, self.name_in_producing_task, True, self.producing_task)


class SubPipeline:
    def __init__(self, pipeline, task_namespance_prefix, dsl):
        self.pipeline = pipeline
        self.task_namespance_prefix = task_namespance_prefix
        self.dsl = dsl

    def with_completed_tasks(self, *args):
        args = [f"{self.task_namespance_prefix}{a}" for a in args]
        return self.dsl.with_completed_tasks(*args)


class TaskMatcher:

    def __init__(self, task_keys_glob_pattern):
        self.task_keys_glob_pattern = task_keys_glob_pattern


class PreExistingFile:

    def __init__(self, file_path, manage_signature=False):

        if file_path is None or file_path == "" or file_path == ".":
            raise ValidationError(f"invalid file {file_path}")

        self.file_path = file_path
        self.manage_signature = manage_signature

    def to_hash(self):
        return self.file_path

    def value(self):
        return self.file_path

    def absolute_path(self, task):

        if os.path.isabs(self.file_path):
            return self.file_path

        return self.file_path


class ProducedFile:

    def __init__(self, file_path, var_name, manage_signature, producing_task, is_dummy=False):

        if file_path is None or file_path == "" or file_path == ".":
            raise ValidationError(f"invalid file {file_path}")

        if var_name is None or var_name == "":
            raise ValidationError(f"can't be {var_name}")

        self.var_name = var_name
        self.file_path = file_path
        self.producing_task = producing_task
        self.manage_signature = manage_signature
        self.is_dummy = is_dummy

    def to_hash(self):
        return self.file_path

    def value(self):
        return self.file_path

    def absolute_path(self, task):

        if os.path.isabs(self.file_path):
            return self.file_path

        return os.path.join(task.work_dir(), self.file_path)

    def input_file(self, var_name_in_consuming_task):
        return InputFile(self, var_name_in_consuming_task)


class InputFile:

    def __init__(self, produced_file, var_name_in_consuming_task):
        self.produced_file = produced_file
        self.var_name_in_consuming_task = var_name_in_consuming_task


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


class Wait:

    def __init__(self, tasks):
        self.tasks = tasks

    def is_ready(self):
        for t in self.tasks:
            if not t.has_completed():
                return False

        return True

class TaskProps:

    def __init__(self, task, props):
        self.props = props
        self.task = task

    def __getattr__(self, name):
        if name not in self.props:
            raise ValidationError(f"{self.task} has no prop {name}")
        return self.props.get(name)


def flatten(the_list):
    return [item for sublist in the_list for item in sublist]


class MessageUtils:

    @staticmethod
    def safe_utf8(s):
        try:
            return s.decode("utf8")
        except UnicodeDecodeError as ex:
            return str(s)

