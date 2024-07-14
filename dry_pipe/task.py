import fnmatch
import glob
import os
import pathlib
from hashlib import blake2b
from tempfile import NamedTemporaryFile

from dry_pipe.core_lib import FileCreationDefaultModes, PortablePopen, invoke_rsync


class Task:

    @staticmethod
    def key_grouper(task_key):
        if "." not in task_key:
            return task_key
        return task_key.split(".")[0]

    def __init__(self,
        key,
        inputs,
        outputs,
        pipeline_instance,
        task_steps,
        task_conf,
        is_slurm_array_child,
        max_simultaneous_jobs_in_slurm_array,
        is_slurm_parent
    ):
        self.key = key
        self.inputs = TaskInputs(self, inputs)
        self.outputs = TaskOutputs(self, outputs)
        self.pipeline_instance = pipeline_instance
        self.task_steps = task_steps
        self.task_conf = task_conf
        self.is_slurm_array_child = is_slurm_array_child
        self.max_simultaneous_jobs_in_slurm_array = max_simultaneous_jobs_in_slurm_array
        self.is_slurm_parent = is_slurm_parent


    """            
        dsl.query_all_or_nothing("my-array-tasks.*", state="ready")
    """

    def upstream_dep_keys(self):
        return [
            i.upstream_task_key
            for i in self.inputs
            if i.upstream_task_key is not None
        ]

    def __str__(self):
        return f"Task(key={self.key})"

    def _visit_input_and_output_files(self, collect_deps_and_outputs_func):
        raise Exception(f"to implement")

    def compute_hash_code(self):

        format_version = "1"

        d = blake2b(digest_size=20)

        sep = ",".encode("utf8")
        def h(s):
            d.update(s.encode("utf8"))
            d.update(sep)

        h(format_version)

        h(self.key)

        for v in self.inputs.hash_values():
            h(v)

        for v in self.outputs.hash_values():
            h(v)

        for s in self.task_steps:
            for v in s.hash_values():
                h(v)

        for v in self.task_conf.hash_values():
            h(v)

        if self.is_slurm_array_child:
            h('t')
        else:
            h('f')

        if self.max_simultaneous_jobs_in_slurm_array is not None:
            h(str(self.max_simultaneous_jobs_in_slurm_array))

        return d.hexdigest()

    def save_if_hash_has_changed(self, state_file, hash_code):

        def hash_changed():
            with open(state_file.task_conf_file()) as f:
                i = 0
                for line in f:
                    if '"digest"' in line:
                        _, digest_in_file = line.split(":")
                        digest_in_file = digest_in_file.replace(",", "").replace('"', '').strip()
                        return hash_code != digest_in_file
                    else:
                        i += 1
                    if i > 3:
                        raise Exception(f"'digest' not found in {state_file.task_conf_file}, expected on line 2")

        if hash_changed():
            self.save(state_file, hash_code)

    def save(self, state_file, hash_code):

        pipeline_instance_dir = state_file.tracker.pipeline_instance_dir
        control_dir = state_file.control_dir()
        output_dir = state_file.output_dir()

        class ShallowPipelineInstance:
            def __init__(self):
                self.pipeline_instance_dir = pipeline_instance_dir

        self.pipeline_instance = ShallowPipelineInstance()

        pathlib.Path(control_dir).mkdir(
            parents=True, exist_ok=True, mode=FileCreationDefaultModes.pipeline_instance_directories)

        step_invocations = [
            self.task_steps[step_number].get_invocation(control_dir, self, step_number)
            for step_number in range(0, len(self.task_steps))
        ]

        self.task_conf.is_slurm_parent = self.is_slurm_parent
        self.task_conf.inputs = self.inputs.as_json()
        self.task_conf.outputs = self.outputs.as_json()
        self.task_conf.step_invocations = step_invocations
        self.task_conf.save_as_json(control_dir, digest=hash_code)

        if self.is_slurm_parent:
            with open(os.path.join(control_dir, "task-keys.tsv"), "w") as tc:
                for t in self.inputs.children_tasks.value:
                    tc.write(t.key)
                    tc.write("\n")


class TaskStep:

    def __init__(self, task_conf, shell_script=None, python_call=None, shell_snippet=None):
        self.task_conf = task_conf
        self.shell_script = shell_script
        self.python_call = python_call
        self.shell_snippet = shell_snippet

    def hash_values(self):
        if self.shell_script is not None:
            yield self.shell_script
        if self.python_call is not None:
            yield self.python_call.mod_func()
        if self.shell_snippet is not None:
            yield self.shell_snippet

    def get_invocation(self, control_dir, task, step_number):

        container = self.task_conf.container

        if self.python_call is not None:
            call = {
                "call": "python",
                "module_function": self.python_call.mod_func()
            }
        else:
            if self.shell_snippet is not None:

                s = f"step-{step_number}.sh"
                script_or_snippet_file = \
                    f"$__pipeline_instance_dir/.drypipe/{task.key}/{s}"

                step_script = os.path.join(control_dir, s)

                with open(step_script, "w") as _step_script:
                    _step_script.write(self.shell_snippet)
                os.chmod(step_script, 0o764)
            elif self.shell_script is not None:
                script_or_snippet_file = f"$__pipeline_code_dir/{self.shell_script}"
            else:
                raise Exception("shouldn't have got here")

            call = {
                "call": "bash",
                "script": script_or_snippet_file
            }

        if container is not None:
            call["container"] = container

        return call


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
        self.type = type
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
        return self.type == 'file'

    def is_upstream_output(self):
        return self.upstream_task_key is not None

    def is_upstream_var_output(self):
        return self.upstream_task_key is not None and self.type in ['str', 'int', 'float']

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


class FileSet:
    def __init__(self, pattern, exclude_pattern):
        self.pattern = pattern
        self.exclude_pattern = exclude_pattern


class TaskOutput:

    @staticmethod
    def from_json(json_dict):
        j = json_dict
        produced_file_name = j.get("produced_file_name")
        file_set = j.get("file_set")
        if file_set is not None:
            file_set = FileSet(file_set["pattern"], file_set.get("exclude_pattern"))
        return TaskOutput(
            j["name"],
            j["type"],
            produced_file_name=produced_file_name,
            file_set=file_set
        )

    def as_string(self):
        p = "" if self.produced_file_name is None else f",{self.produced_file_name}"
        return f"TaskOutput({self.name},{self.type}{p})"

    def __init__(self, name, type, produced_file_name=None, task_key=None, file_set=None):
        if type not in ['file', 'str', 'int', 'float', 'file_set']:
            raise Exception(f"invalid type {type}")

        if type != 'file' and produced_file_name is not None:
            raise Exception(f"non file output can't have not None produced_file_name: {produced_file_name}")

        self.name = name
        self.type = type
        self.produced_file_name = produced_file_name
        self._resolved_value = None
        self.task_key = task_key
        self.file_set = file_set
        self.task_output_dir = None

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

        if self.file_set is not None:
            r["file_set"] = {
                "pattern": self.file_set.pattern,
                "exclude_pattern": self.file_set.exclude_pattern
            }

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
    def __iter__(self):

        if self.task_output_dir is None:
            raise Exception(f"{self.name} was not resolved, task must complete before using an output variable")

        if self.file_set is None:
            raise Exception(
                f"task {self.task_key} output {self.name} of type {self.type} is not iterable. " +
                "Only dsl.file_set(...) are iterable"
            )

        for f in pathlib.Path(self.task_output_dir).glob(self.file_set.pattern):
            if f.is_dir() or f.match(self.file_set.exclude_pattern):
                continue
            else:
                yield f


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
            for o in self._task_inputs.values()
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

    def rsync_output_var_file_list_produced_upstream(self):

        upstream_output_var_deps = {
            i.upstream_task_key
            for i in self._task_inputs.values()
            if i.is_upstream_var_output()
        }

        for task_key in upstream_output_var_deps:
            yield f".drypipe/{task_key}/output_vars"

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
            for o in self._task_outputs.values()
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

    def has_var_outputs(self):
        for _ in self.iterate_non_file_outputs():
            return True
        return False

    def rsync_file_list(self):
        has_output_var = False
        for o in self._task_outputs.values():
            if o.is_file():
                yield f"output/{self.task_key}/{o.produced_file_name}"
            else:
                has_output_var = True

        if has_output_var:
            yield f".drypipe/{self.task_key}/output_vars"

    def has_file_sets(self):
        for o in self._task_outputs.values():
            if o.file_set is not None:
                return True

    def rsync_filter_list(self, task_output_dir):
        pipeline_out = pathlib.Path(task_output_dir).parent
        for o in self._task_outputs.values():
            if o.file_set is not None:
                for f in o:
                    yield f"output/{pathlib.Path(f).relative_to(pipeline_out)}"

    def iterate_file_task_outputs(self, task_output_dir):
        for o in self._task_outputs.values():
            if o.is_file():
                yield o, o.name, os.path.join(task_output_dir, o.produced_file_name)
