import collections
import datetime
import glob
import hashlib
import json
import os
import pathlib
import shutil

import textwrap

from dry_pipe.script_lib import FileCreationDefaultModes, \
    TaskInput, TaskOutput, resolve_upstream_and_constant_vars, iterate_file_task_outputs
from dry_pipe import bash_shebang
from dry_pipe.internals import ProducedFile, OutFileSet, ValidationError

from dry_pipe.script_lib import task_script_header, iterate_out_vars_from
from dry_pipe.task_state import TaskState


class Task:

    @staticmethod
    def key_grouper(task_key):
        if "." not in task_key:
            return task_key
        return task_key.split(".")[0]

    @staticmethod
    def load_from_task_state(task_state):
        return Task(None, task_state)

    def __init__(self, task_builder):

        self.inputs = TaskInputs(self, task_inputs=task_builder._consumes.values())
        self.outputs = TaskOutputs(self, task_outputs=task_builder._produces)
        self.python_bin = None
        self.conda_env = None
        self.key = task_builder.key
        self.pipeline_instance = task_builder.pipeline_instance
        self.task_steps = task_builder.task_steps
        self.task_conf = task_builder.task_conf

        self.has_python_step = False

        for s in self.task_steps:
            if s.python_call is not None:
                self.has_python_step = True
                break

    def upstream_dep_keys(self):
        return [
            i.upstream_task_key
            for i in self.inputs
            if i.upstream_task_key is not None
        ]

    def is_rehydrated(self):
        return self.out.is_rehydrated()

    def __repr__(self):
        return f"Task(key={self.key})"

    def __getattr__(self, name):
        self.out.check_for_mis_understanding_of_dsl(name)

    def suffix(self):
        return self.key.split(".")[1]

    def as_json(self):
        raise Exception(f"deprecated, use get_state().as_json()")

    def all_produced_files(self):
        for f, item in self.outputs.produces.items():
            if isinstance(item, ProducedFile):
                yield item

    def _visit_input_and_output_files(self, collect_deps_and_outputs_func):
        raise Exception(f"to implement")

    def iterate_out_vars(self):
        of = self.v_abs_output_var_file()
        if os.path.exists(of):
            yield from iterate_out_vars_from(of)

    def out_vars_by_name(self):
        return dict(self.iterate_out_vars())

    def prepare(self):

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
            self.task_steps[step_number].get_invocation(self, step_number)
            for step_number in range(0, len(self.task_steps))
        ]

        task_conf_file = self.v_abs_task_conf_file()

        with open(task_conf_file, "w") as f:
            task_conf_json = self.task_conf.as_json()

            task_conf_json["inputs"] = [
                i.as_json() for i in self.inputs
            ]

            task_conf_json["outputs"] = [
                o.as_json() for o in self.outputs
            ]

            task_conf_json["step_invocations"] = step_invocations

            f.write(json.dumps(task_conf_json, indent=2))

        setenv_file = self.v_abs_task_env_file()

        with open(setenv_file, "w") as f:
            f.write(f"{bash_shebang()}\n\n")
            f.write('__script_location=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )\n')
            f.write("export __pipeline_instance_dir=$( dirname $( dirname $__script_location))\n")

            f.write("\n")

            f.write(". $__pipeline_instance_dir/.drypipe/pipeline-env.sh\n")

            f.write(f'eval "$($__script_location/task env --with-exports)"\n')

        os.chmod(setenv_file, FileCreationDefaultModes.pipeline_instance_scripts)

        if is_remote:
            self._visit_input_and_output_files(collect_deps_and_outputs)

        shell_script_file = self.v_abs_script_file()

        with open(shell_script_file, "w") as f:
            f.write(task_script_header())
            f.write('script_lib.handle_main()\n')
            f.write('logging.shutdown()\n')

        os.chmod(shell_script_file, FileCreationDefaultModes.pipeline_instance_scripts)

        if self.task_conf.is_slurm():
            pid_name = os.path.basename(self.pipeline_instance.pipeline_instance_dir)

            with open(self.v_abs_sbatch_launch_script(), "w") as f:
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

            os.chmod(self.v_abs_sbatch_launch_script(), FileCreationDefaultModes.pipeline_instance_scripts)

        if is_remote:
            local_deps = os.path.join(self.v_abs_control_dir(), "local-deps.txt")

            def write_external_deps_file(dep_list, out_file_name):
                if len(dep_list) > 0:
                    out_file = os.path.join(self.v_abs_control_dir(), out_file_name)
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
                        f.write(self.step_script_file(step_number))
                        f.write("\n\n")
                    step_number += 1

                f.write(self.history_file())
                f.write("\n")
                f.write(self.task_env_file())
                f.write("\n")
                f.write(self.script_file())
                f.write("\n")
                f.write(self.task_conf_file())
                f.write("\n")
                if external_files_deps_file is not None:
                    f.write(os.path.join(self.control_dir(), os.path.basename(external_files_deps_file)))
                    f.write("\n")
                if external_files_deps_pipeline_instance_file is not None:
                    f.write(
                        os.path.join(self.control_dir(), os.path.basename(external_files_deps_pipeline_instance_file))
                    )
                    f.write("\n")

                if self.task_conf.is_slurm():
                    f.write(self.sbatch_launch_script())
                    f.write("\n")

            remote_outputs = os.path.join(self.v_abs_control_dir(), "remote-outputs.txt")

            with open(remote_outputs, "w") as f:

                for dep_file in output_file_list:
                    if not "*" in dep_file:
                        f.write(dep_file)
                        f.write("\n")

                f.write(self.history_file())
                f.write("\n")
                f.write(os.path.join(self.control_dir(), "out_sigs/"))
                f.write("\n")
                f.write(os.path.join(self.control_dir(), "output_vars"))
                f.write("\n")
                f.write(self.err_log())
                f.write("\n")
                f.write(self.out_log())
                f.write("\n")

            remote_output_filesets = os.path.join(self.v_abs_control_dir(), "remote-output-filesets.txt")
            output_filesets = [
                dep_file for dep_file in output_file_list if "*" in dep_file
            ]

            if len(output_filesets) > 0:
                with open(remote_output_filesets, "w") as f:
                    for dep_file in output_filesets:
                        f.write("+ ")
                        f.write(os.path.basename(dep_file))
                        f.write("\n")

    def seconds_since_last_activity(self):

        def st_mtime(f):
            p = pathlib.Path(f)
            if not p.exists():
                return 0

            return p.stat().st_mtime

        t_history_file, t_err_log, t_out_log = map(st_mtime, [
            self.v_abs_history_file(),
            self.v_abs_err_log(),
            self.v_abs_out_log()
        ])

        last_activity_time = max([t_history_file, t_err_log, t_out_log])

        if last_activity_time == 0:
            return 0

        delta = datetime.datetime.now() - datetime.datetime.fromtimestamp(last_activity_time)

        return delta.seconds

    def __is_zombie(self):

        raise Exception("implementation in progress")

        t = self.seconds_since_last_activity()

        if t == 0:
            return False

        slurm_ex = self.slurm_executor_or_none()

        if slurm_ex is None:
            return False

        slurm_job_id = self.slurm_job_id()

        if slurm_job_id is None:
            # WARNING
            raise Exception(f"")

        squeue_line = call_squeue_for_job_id(slurm_job_id)

        if squeue_line is not None:
            pathlib.Path(self.v_abs_history_file()).touch()
            return False

        task_state = self.get_state()

        if task_state.is_step_started() or task_state.is_launched() or task_state.is_scheduled():
            return True

        return False

    def is_remote(self):
        return self.task_conf.is_remote()

    def control_dir(self):
        return os.path.join(".drypipe", self.key)

    def work_dir(self):
        return os.path.join("output", self.key)

    def slurm_job_id(self):

        slurm_job_id_file = self.v_abs_slurm_job_id_file()

        if not os.path.exists(slurm_job_id_file):
            return None

        with open(slurm_job_id_file) as f:
            return f.read().strip()

    def slurm_job_id_file(self):
        return os.path.join(self.control_dir(), "slurm_job_id")

    def is_input_signature_flagged_as_changed(self):
        return os.path.exists(self._input_signature_changed_file())

    def output_var_file(self):
        return os.path.join(self.control_dir(), "output_vars")

    def task_env_file(self):
        return os.path.join(self.control_dir(), "task-env.sh")

    def task_conf_file(self):
        return os.path.join(self.control_dir(), "task-conf.json")

    def pid_file(self, pid):
        return os.path.join(self.control_dir(), f"{pid}.pid")

    def scratch_dir(self):
        if not self.task_conf.is_slurm():
            return os.path.join(self.work_dir(), "scratch")

        return None

    def script_file(self):
        return os.path.join(self.control_dir(), "task")

    def step_script_file(self, step_number):
        return os.path.join(self.control_dir(), f"step-{step_number}.sh")

    def sbatch_launch_script(self):
        return os.path.join(self.control_dir(), "sbatch-launcher.sh")

    def history_file(self):
        return os.path.join(self.control_dir(), "history.tsv")

    def input_hash_file(self, input_hash):
        return os.path.join(self.control_dir(), f"in.{input_hash}.sig")

    def abs_from_pipeline_instance_dir(self, path):
        return os.path.join(self.pipeline_instance.pipeline_instance_dir, path)

    def abs_from_pipeline_code_dir(self, path):
        return os.path.join(self.dsl.pipeline_code_dir, path)

    def pipeline_code_dir(self):
        return self.dsl.pipeline_code_dir

    def v_abs_control_dir(self):
        return self.abs_from_pipeline_instance_dir(self.control_dir())

    def v_abs_work_dir(self):
        return self.abs_from_pipeline_instance_dir(self.work_dir())

    def abs_path_of_produced_file(self, file_name):
        return os.path.join(self.v_abs_work_dir(), file_name)

    def compute_hash_code(self):
        return "123"

    def save(self, pipeline_instance_dir, hash_code):

        class ShallowPipelineInstance:
            def __init__(self):
                self.pipeline_instance_dir = pipeline_instance_dir

        self.pipeline_instance = ShallowPipelineInstance()
        self._create_state_file_and_control_dir()
        self.prepare()

    def __getattr__(self, name):

        def fail():
            raise AttributeError(f"{self.__class__} has no attribute {name}")

        is_v_abs = name.startswith("v_abs_")
        is_v_exp = name.startswith("v_exp_")

        if not (is_v_abs or is_v_exp):
            fail()

        func_name = name[6:]
        func = getattr(self, func_name)

        if func is None:
            fail()

        if is_v_abs:
            def f(*args, **kwargs):
                d = func(*args, **kwargs)
                if d is None:
                    return None
                return os.path.join(self.pipeline_instance.pipeline_instance_dir, d)
            return f
        elif is_v_exp:
            def f(*args, **kwargs):
                return f"$__pipeline_instance_dir/{func(*args, **kwargs)}"
            return f
        else:
            raise Exception(f"how have we got here ?")

    def re_queue(self):
        task_state = self.get_state()
        task_state.transition_to_prepared(force=True)

    def has_completed(self):

        control_dir = self.v_abs_control_dir()

        if not os.path.exists(control_dir):
            return False

        f = [f for f in glob.glob(os.path.join(control_dir, "state.*"))]

        cnt = len(f)

        if cnt == 0:
            return False

        task_state = TaskState(os.path.abspath(f[0]))

        return task_state.is_completed()

    def get_state(self):
        control_dir = self.v_abs_control_dir()
        if not os.path.exists(control_dir):
            return None

        f = [f for f in glob.glob(os.path.join(control_dir, "state.*"))]

        cnt = len(f)

        if cnt == 1:
            return TaskState(os.path.abspath(f[0]))
        elif cnt == 0:
            return None

        raise Exception(f"expected one task state file (state.*) in {self}, got {len(f)}")

    def _create_state_file_and_control_dir(self):

        for d in [
            self.v_abs_control_dir()
        ]:
            pathlib.Path(d).mkdir(
                parents=True, exist_ok=True, mode=FileCreationDefaultModes.pipeline_instance_directories)

        if not os.path.exists(self.v_abs_work_dir()):
            pathlib.Path(self.v_abs_work_dir()).mkdir(
                parents=True, exist_ok=True, mode=FileCreationDefaultModes.pipeline_instance_directories)

        if self.v_abs_scratch_dir() is not None:
            pathlib.Path(self.v_abs_scratch_dir()).mkdir(
                parents=True, exist_ok=True, mode=FileCreationDefaultModes.pipeline_instance_directories)

    def reset_logs(self):

        if os.path.exists(self.v_abs_err_log()):
            os.remove(self.v_abs_err_log())

        if os.path.exists(self.v_abs_out_log()):
            os.remove(self.v_abs_out_log())

        if os.path.exists(self.v_abs_control_error_log()):
            os.remove(self.v_abs_control_error_log())

    def out_log(self):
        return os.path.join(self.control_dir(), "out.log")

    def err_log(self):
        return os.path.join(self.control_dir(), "err.log")

    def control_error_log(self):
        return os.path.join(self.control_dir(), "control-err.log")

    def log_control_error(self, text, truncate=False):

        mode = "a"

        if truncate:
            mode = "w"

        with open(self.v_abs_control_error_log(), mode) as f:
            f.write("\n")
            f.write(text)

    def pid_file(self, pid):
        return os.path.join(self.control_dir(), f"{pid}.pid")

    def clean(self):
        shutil.rmtree(self.v_abs_control_dir(), ignore_errors=True)
        shutil.rmtree(self.v_abs_work_dir(), ignore_errors=True)

    def launch(self, executer, wait_for_completion=False, fail_silently=False):

        def touch_pid_file(pid):
            #pathlib.Path(self.v_abs_pid_file(pid)).touch(exist_ok=False)
            #TODO: figure out if PID tracking should be done
            pass

        executer.execute(self, touch_pid_file, wait_for_completion, fail_silently)


class TaskStep:

    def __init__(self, task_conf, shell_script=None, python_call=None, shell_snippet=None):
        self.task_conf = task_conf
        self.shell_script = shell_script
        self.python_call = python_call
        self.shell_snippet = shell_snippet

    def get_invocation(self, task, step_number):

        container = self.task_conf.container

        if self.python_call is not None:
            call = {
                "call": "python",
                "module_function": self.python_call.mod_func()
            }
        else:
            if self.shell_snippet is not None:
                script_or_snippet_file = \
                    f"$__pipeline_instance_dir/{task.step_script_file(step_number)}"
                step_script = task.v_abs_step_script_file(step_number)
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

    def __init__(self, task, task_conf_json=None, task_inputs=None, pipeline_work_dir=None):
        self.task = task
        self._task_inputs = task_inputs
        if task_conf_json is not None:
            self._task_inputs = {}

            for task_input, k, v in resolve_upstream_and_constant_vars(
                pipeline_work_dir,
                task_conf_json
            ):
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


class MissingOutvars(Exception):
    def __init__(self, message):
        super(MissingOutvars, self).__init__(message)
        self.message = message


class MissingUpstreamDeps(Exception):
    pass


class UpstreamDepsChanged(Exception):
    pass
