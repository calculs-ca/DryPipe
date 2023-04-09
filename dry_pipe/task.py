import json
import os
import pathlib

import textwrap

from dry_pipe.core_lib import FileCreationDefaultModes

from dry_pipe.core_lib import task_script_header


class Task:

    @staticmethod
    def key_grouper(task_key):
        if "." not in task_key:
            return task_key
        return task_key.split(".")[0]

    def __init__(self, task_builder):

        self.key = task_builder.key
        self.inputs = TaskInputs(self, task_inputs=task_builder._consumes.values())
        self.outputs = TaskOutputs(self, task_outputs=task_builder._produces)
        self.pipeline_instance = task_builder.pipeline_instance
        self.task_steps = task_builder.task_steps
        self.task_conf = task_builder.task_conf
        self.is_slurm_array_child = task_builder.is_slurm_array_child
        self.max_simultaneous_jobs_in_slurm_array = task_builder.max_simultaneous_jobs_in_slurm_array


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

    def prepare(self, control_dir):

        def task_conf_file():
            return os.path.join(control_dir, "task-conf.json")

        step_invocations = [
            self.task_steps[step_number].get_invocation(control_dir, self, step_number)
            for step_number in range(0, len(self.task_steps))
        ]

        with open(task_conf_file(), "w") as f:
            task_conf_json = self.task_conf.as_json()

            task_conf_json["inputs"] = [
                i.as_json() for i in self.inputs
            ]

            task_conf_json["outputs"] = [
                o.as_json() for o in self.outputs
            ]

            task_conf_json["step_invocations"] = step_invocations

            f.write(json.dumps(task_conf_json, indent=2))

        shell_script_file = os.path.join(control_dir, "task")

        with open(shell_script_file, "w") as f:
            f.write(task_script_header())
            f.write('core_lib.handle_main()\n')
            f.write('logging.shutdown()\n')

        os.chmod(shell_script_file, FileCreationDefaultModes.pipeline_instance_scripts)


    def compute_hash_code(self):
        return "123"

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

        pathlib.Path(output_dir).mkdir(
            parents=True, exist_ok=True, mode=FileCreationDefaultModes.pipeline_instance_directories)

        pathlib.Path(os.path.join(output_dir, "scratch")).mkdir(
            parents=True, exist_ok=True, mode=FileCreationDefaultModes.pipeline_instance_directories)

        self.prepare(control_dir)




class TaskStep:

    def __init__(self, task_conf, shell_script=None, python_call=None, shell_snippet=None):
        self.task_conf = task_conf
        self.shell_script = shell_script
        self.python_call = python_call
        self.shell_snippet = shell_snippet

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


class TaskInputs:

    def __init__(self, task, task_runner=None, task_inputs=None):
        self.task = task
        self._task_inputs = task_inputs
        if task_runner is not None:
            self._task_inputs = {}

            for task_input, k, v in task_runner.resolve_upstream_and_constant_vars():
                if v is not None:
                    self._task_inputs[k] = task_input.parse(v)

    def __iter__(self):
        yield from self._task_inputs


    def __getattr__(self, name):

        p = self._task_inputs.get(name)

        if p is None:
            raise Exception(
                f"task {self.task} does not declare input '{name}' in it's consumes() clause.\n" +
                f"Use task({self.task.key}).consumes({name}=...) to specify input"
            )

        return p


class TaskOutputs:

    def __init__(self, task, task_outputs=None):
        self.task = task
        self._task_outputs = task_outputs


    def __iter__(self):
        yield from self._task_outputs.values()

    def __getattr__(self, name):

        p = self._task_outputs.get(name)

        if p is None:
            raise Exception(
                f"task {self.task} does not declare output '{name}' in it's outputs() clause.\n" +
                f"Use task({self.task.key}).outputs({name}=...) to specify outputs"
            )

        return p