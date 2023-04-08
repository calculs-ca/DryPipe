import json
import os
import pathlib

import textwrap

from dry_pipe.script_lib import FileCreationDefaultModes
from dry_pipe import bash_shebang
from dry_pipe.internals import ValidationError

from dry_pipe.script_lib import task_script_header


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

        def out_log():
            return os.path.join(control_dir, "out.log")

        def err_log():
            return os.path.join(control_dir, "err.log")

        def script_file():
            return os.path.join(control_dir, "task")

        def task_conf_file():
            return os.path.join(control_dir, "task-conf.json")

        def sbatch_launch_script():
            return os.path.join(control_dir, "sbatch-launcher.sh")

        def control_dir_relative_to_pid():
            return os.path.join(".drypipe", self.key)


        is_remote = self.is_remote()
        external_dep_file_list = []
        external_dep_file_pipeline_instance_list = []
        dependent_file_list = []
        output_file_list = []

        def collect_deps_and_outputs(dep_file, output_file, external_dep_type=None):
            if dep_file is not None:
                if os.path.isabs(dep_file):
                    if external_dep_type is None:
                        external_dep_file_list.append(dep_file)
                    elif external_dep_type == "pipeline-instance":
                        external_dep_file_pipeline_instance_list.append(dep_file)
                    else:
                        raise Exception(f"invalid remote_cache_bucket: {external_dep_type}")
                else:
                    dependent_file_list.append(dep_file)
            elif output_file is not None:
                output_file_list.append(output_file)
            else:
                raise Exception("collect_deps_and_outputs(None, None)")

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

        if is_remote:
            self._visit_input_and_output_files(collect_deps_and_outputs)

        shell_script_file = os.path.join(control_dir, "task")

        with open(shell_script_file, "w") as f:
            f.write(task_script_header())
            f.write('script_lib.handle_main()\n')
            f.write('logging.shutdown()\n')

        os.chmod(shell_script_file, FileCreationDefaultModes.pipeline_instance_scripts)

        if self.task_conf.is_slurm():
            pid_name = os.path.basename(self.pipeline_instance.pipeline_instance_dir)

            sbatch_file = os.path.join(control_dir, "sbatch-launcher.sh")

            with open(sbatch_file, "w") as f:
                f.write(f"{bash_shebang()}\n\n")

                f.write(textwrap.dedent("""
                    __script_location=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )
                                                                            
                    __arg_1=$1
                """))

                f.write("\n".join([
                    f"__job_id=$(sbatch \\",
                    f"    {' '.join(self.task_conf.sbatch_options)} \\",
                    f"    --account={self.task_conf.slurm_account} \\",
                     "    --output=$__script_location/out.log \\",
                     "    --error=$__script_location/err.log \\",
                     "    --export=__script_location=$__script_location,__is_slurm=True,DRYPIPE_TASK_DEBUG=$DRYPIPE_TASK_DEBUG \\",
                     "    --signal=B:USR1@50 \\",
                     "    --parsable \\",
                    f"    --job-name={self.key}-{pid_name} $SBATCH_EXTRA_ARGS \\",
                     "    $__script_location/task)"
                ]))

                f.write("\n\n")

                f.write("echo $__job_id > $__script_location/slurm_job_id\n")
                f.write('echo "scancel --signal=SIGTERM $__job_id" > $__script_location/kill\n')
                f.write('chmod u+x $__script_location/kill\n')

            os.chmod(sbatch_file, FileCreationDefaultModes.pipeline_instance_scripts)

        if is_remote:
            local_deps = os.path.join(control_dir, "local-deps.txt")

            def write_external_deps_file(dep_list, out_file_name):
                if len(dep_list) > 0:
                    out_file = os.path.join(control_dir, out_file_name)
                    with open(out_file, "w") as _out_file:
                        for d in dep_list:
                            _out_file.write(d)
                            _out_file.write("\n")
                    return out_file
                else:
                    return None

            external_files_deps_file = write_external_deps_file(
                external_dep_file_list, "external-deps.txt"
            )

            external_files_deps_pipeline_instance_file = write_external_deps_file(
                external_dep_file_pipeline_instance_list, "external-deps-pipeline-instance.txt"
            )

            with open(local_deps, "w") as f:

                for dep_file in dependent_file_list:
                    f.write(dep_file)
                    f.write("\n")

                step_number = 0
                for step in self.task_steps:
                    if step.shell_snippet is not None:
                        f.write(f".drypipe/{self.key}/{step_number}")
                        f.write("\n\n")
                    step_number += 1

                f.write(script_file)
                f.write("\n")
                f.write(self.task_conf_file())
                f.write("\n")
                if external_files_deps_file is not None:
                    f.write(os.path.join(control_dir_relative_to_pid(), os.path.basename(external_files_deps_file)))
                    f.write("\n")
                if external_files_deps_pipeline_instance_file is not None:
                    f.write(
                        os.path.join(control_dir_relative_to_pid(), os.path.basename(external_files_deps_pipeline_instance_file))
                    )
                    f.write("\n")

                if self.task_conf.is_slurm():
                    f.write(sbatch_launch_script())
                    f.write("\n")

            remote_outputs = os.path.join(control_dir, "remote-outputs.txt")

            with open(remote_outputs, "w") as f:

                for dep_file in output_file_list:
                    if not "*" in dep_file:
                        f.write(dep_file)
                        f.write("\n")

                f.write(os.path.join(control_dir_relative_to_pid(), "out_sigs/"))
                f.write("\n")
                f.write(os.path.join(control_dir_relative_to_pid(), "output_vars"))
                f.write("\n")
                f.write(err_log())
                f.write("\n")
                f.write(out_log())
                f.write("\n")

            remote_output_filesets = os.path.join(control_dir, "remote-output-filesets.txt")
            output_filesets = [
                dep_file for dep_file in output_file_list if "*" in dep_file
            ]

            if len(output_filesets) > 0:
                with open(remote_output_filesets, "w") as f:
                    for dep_file in output_filesets:
                        f.write("+ ")
                        f.write(os.path.basename(dep_file))
                        f.write("\n")

    def is_remote(self):
        return self.task_conf.is_remote()

    def slurm_job_id(self):

        slurm_job_id_file = self.v_abs_slurm_job_id_file()

        if not os.path.exists(slurm_job_id_file):
            return None

        with open(slurm_job_id_file) as f:
            return f.read().strip()

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

    def __init__(self, task, task_runner=None, task_inputs=None, pipeline_work_dir=None):
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

        if self._task_inputs is None:
            self._resolve()

        p = self._task_inputs.get(name)

        if p is None:
            raise ValidationError(
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
            raise ValidationError(
                f"task {self.task} does not declare output '{name}' in it's outputs() clause.\n" +
                f"Use task({self.task.key}).outputs({name}=...) to specify outputs"
            )

        return p