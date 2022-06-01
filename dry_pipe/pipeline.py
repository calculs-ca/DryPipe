import inspect
import logging
import os
import pathlib
import shutil
import subprocess
from itertools import groupby

from dry_pipe import TaskConf, DryPipeDsl, TaskBuilder
from dry_pipe.bash import BASH_SIGN_FILES_IF_NEWER, BASH_TASK_FUNCS_AND_TRAPS, BASH_SIGN_FILES, bash_shebang
from dry_pipe.internals import ValidationError, Wait, SubPipeline
from dry_pipe.janitors import Janitor
from dry_pipe.pipeline_state import PipelineState
from dry_pipe.task import Task

logger = logging.getLogger(__name__)

class TaskSet:

    def __init__(self, pipeline_instance):

        self.pipeline_instance = pipeline_instance
        self._tasks, self._change_tracking_functions = pipeline_instance.pipeline.validated_tasks_gen(pipeline_instance)
        self._tasks_signature = self._calculate_tasks_signature()

    def __iter__(self):
        for t in self._tasks:
            yield t

    def __len__(self):
        return len(self._tasks)

    def __getitem__(self, task_key):

        task = self.get(task_key)

        if task is not None:
            return task

        raise KeyError(f"pipeline has no task {task_key}")

    def get(self, task_key):
        for task in self._tasks:
            if task.key == task_key:
                return task

    def _calculate_tasks_signature(self):
        return [
            f() for f in self._change_tracking_functions
        ]

    def regen_if_stale_else_self(self, force=False):
        sig = self._calculate_tasks_signature()

        if sig != self._tasks_signature or force:
            return TaskSet(self.pipeline_instance)
        else:
            return self


class Pipeline:

    def __init__(
            self,
            generator_of_tasks,
            pipeline_code_dir=None,
            task_conf=None,
            containers_dir=None,
            env_vars=None
    ):

        if pipeline_code_dir is None:
            pipeline_code_dir = os.path.dirname(os.path.abspath(inspect.getmodule(generator_of_tasks).__file__))

        if containers_dir is None:
            containers_dir = os.path.join(pipeline_code_dir, "containers")

        if task_conf is None:
            task_conf = TaskConf("process")

        def gen_and_validate_tasks(pipeline_instance):
            from dry_pipe.task import Task

            dsl = DryPipeDsl(task_conf=task_conf, pipeline_instance=pipeline_instance)

            try:
                iter(generator_of_tasks(dsl))
            except TypeError:
                raise ValidationError(f"function {generator_of_tasks} must return an iterable of Task (ex, via yield)")

            def _convert_non_tasks_to_tasks_and_lambdas(dsl, g):

                for t_i in g(dsl):
                    if isinstance(t_i, Task):
                        t_i.pipeline_instance = pipeline_instance
                        yield t_i
                    elif isinstance(t_i, SubPipeline):
                        dsl2 = DryPipeDsl(
                            task_conf=task_conf,
                            pipeline_instance=pipeline_instance,
                            task_namespance_prefix=t_i.task_namespance_prefix
                        )

                        for t in _convert_non_tasks_to_tasks_and_lambdas(dsl2, t_i.pipeline.generator_of_tasks):
                            yield t

                    elif isinstance(t_i, Wait):
                        for t_i in t_i.tasks:
                            yield t_i
                            yield lambda: t_i.has_completed()

                        if not t_i.is_ready():
                            break
                    elif isinstance(t_i, TaskBuilder):
                        raise ValidationError(
                            f" task(key='{t_i.key}') is incomplete, you should probably call .calls(...) on it.")
                    else:
                        raise ValidationError(
                            f"iterator has yielded an invalid type: {type(t_i)}: '{t_i}'"
                        )

            tasks = [
                task
                for task in _convert_non_tasks_to_tasks_and_lambdas(dsl, generator_of_tasks)
            ]

            # Mystery: creating lambda: task.has_completed() directly in the comprehension, doesn't work
            def check_compl(task):
                return lambda: task.has_completed()

            change_tracking_functions = [
                check_compl(task)
                for task in tasks
                if task.key in pipeline_instance.dag_determining_tasks_ids
            ]

            dup_keys = [
                k for k, task_group in groupby(tasks, lambda t: t.key) if len(list(task_group)) > 1
            ]

            if len(tasks) == 0:
                f = os.path.abspath(inspect.getmodule(generator_of_tasks).__file__)
                msg = f"pipeline {generator_of_tasks.__name__} defined in {f} yielded zero tasks"
                raise Exception(msg)

            if len(dup_keys) > 0:
                raise ValidationError(f"duplicate keys in definition: {dup_keys} value of task(key=) must be unique")

            def all_produced_files():
                for task in tasks:
                    for file in task.all_produced_files():
                        yield task, file

            def z(task, file):
                return file.absolute_path(task)

            for path, task_group in groupby(all_produced_files(), lambda t: z(*t)):
                task_group = list(task_group)
                if len(task_group) > 1:
                    task_group = ",".join(list(map(lambda t: str(t[0]), task_group)))
                    raise ValidationError(f"Tasks {task_group} have colliding output to file {path}."+
                                          " All files specified in Task(produces=) must be distinct")

            return tasks, change_tracking_functions

        self.validated_tasks_gen = gen_and_validate_tasks

        self.pipeline_code_dir = pipeline_code_dir
        self.task_conf = task_conf
        self.containers_dir = containers_dir
        self.generator_of_tasks = generator_of_tasks
        self.env_vars = env_vars

    def create_pipeline_instance(self, pipeline_instance_dir=None, task_conf=None, containers_dir=None, env_vars=None):

        if task_conf is not None or containers_dir is not None or env_vars is not None:
            p = Pipeline(
                generator_of_tasks=self.generator_of_tasks,
                pipeline_code_dir=self.pipeline_code_dir,
                task_conf=task_conf or self.task_conf,
                containers_dir=containers_dir or self.containers_dir,
                env_vars=env_vars or self.env_vars
            )
        else:
            p = self

        return PipelineInstance(p, pipeline_instance_dir or p.pipeline_code_dir)

    def pipeline_instance_iterator_for_dir(self, pipeline_instances_dir, ignore_completed=True):

        if pipeline_instances_dir is None:
            raise Exception(f"pipeline_instances_dir can't be None")

        class PipelineInstancesDirIterator:

            def __init__(self, pipeline):
                self.pipelines_cache = {}
                self.pipeline = pipeline

            def create_instance_in(self, subdir):

                pid = pathlib.Path(os.path.join(pipeline_instances_dir, subdir, ".drypipe"))
                pathlib.Path(pid).mkdir(parents=True, exist_ok=False)
                pathlib.Path(os.path.join(pid, "state.running")).touch()

            def __iter__(self):

                pipelines_by_dir = {**self.pipelines_cache}

                def gen_():

                    logger.debug("will iterate on pipelines instances in %s ", pipeline_instances_dir)

                    for pipeline_state in PipelineState.iterate_from_instances_dir(pipeline_instances_dir):

                        if pipeline_state.is_completed() and ignore_completed:
                            logger.debug("pipeline completed: %s", pipeline_state.state_file)
                            continue
                        else:
                            logger.debug("pipeline NOT completed: %s, %s", pipeline_state.state_file,
                                         pipeline_state.state_name)

                        p = pipelines_by_dir.get(pipeline_state.dir_basename())

                        if p is None:
                            p = self.pipeline.create_pipeline_instance(pipeline_state.instance_dir())

                        yield pipeline_state.dir_basename(), p

                self.pipelines_cache = dict(gen_())

                for k, pipeline in self.pipelines_cache.items():
                    yield pipeline

        return PipelineInstancesDirIterator(self)


class PipelineInstance:

    def __init__(self, pipeline, pipeline_instance_dir):
        self.pipeline = pipeline
        self.pipeline_instance_dir = pipeline_instance_dir
        self._work_dir = os.path.join(pipeline_instance_dir, ".drypipe")
        self._publish_dir = os.path.join(pipeline_instance_dir, "publish")
        self.dag_determining_tasks_ids = set()
        self.tasks = TaskSet(self)

    @staticmethod
    def _hint_file(instance_dir):
        return os.path.join(instance_dir, ".drypipe", "hints")

    @staticmethod
    def guess_pipeline_from_hints(instance_dir):
        return PipelineInstance.load_hints(instance_dir).get("pipeline")

    @staticmethod
    def write_hint_file_if_not_exists(instance_dir, module_func_pipeline):
        hint_file = PipelineInstance._hint_file(instance_dir)

        if not os.path.exists(hint_file):
            pathlib.Path(os.path.dirname(hint_file)).mkdir(exist_ok=True, parents=True)
            with open(hint_file, "w") as _f:
                _f.write(f"pipeline={module_func_pipeline}\n")

    @staticmethod
    def load_hints(instance_dir):
        hf = PipelineInstance._hint_file(instance_dir)
        if not os.path.exists(hf):
            return {}

        def hints():
            with open(hf) as _hf:
                for line in _hf.readlines():
                    line = line.strip()
                    if line != "":
                        k, v = line.split("=")
                        yield k.strip(), v.strip()

        return dict(hints())

    def hints(self):
        return PipelineInstance.load_hints(self.pipeline_instance_dir)

    def init_work_dir(self):

        pathlib.Path(self._publish_dir).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self._work_dir).mkdir(parents=True, exist_ok=True)

        pipeline_env = os.path.join(self._work_dir, "pipeline-env.sh")
        with open(pipeline_env, "w") as f:
            f.write(f"{bash_shebang()}\n\n")
            f.write(f"export __pipeline_code_dir={self.pipeline.pipeline_code_dir}\n")
            f.write(f"export __containers_dir={self.pipeline.containers_dir}\n")
            if self.pipeline.env_vars is not None:
                for k, v in self.pipeline.env_vars.items():
                    f.write(f"export {k}={v}\n")

        os.chmod(pipeline_env, 0o764)

        drypipe_bash_lib = os.path.join(self._work_dir, "drypipe-bash-lib.sh")

        with open(drypipe_bash_lib, "w") as f:
            f.write(f"{bash_shebang()}\n\n")
            f.write(BASH_TASK_FUNCS_AND_TRAPS)
            f.write(BASH_SIGN_FILES)

        os.chmod(drypipe_bash_lib, 0o764)

        if not os.path.exists(self._recalc_hash_script()):
            with open(self._recalc_hash_script(), "w") as f:
                f.write(f"{bash_shebang()}\n\n")
                f.write(BASH_SIGN_FILES_IF_NEWER)
                f.write("\n__sign_files\n")
            os.chmod(self._recalc_hash_script(), 0o764)

        self.calc_pre_existing_files_signatures()
        self.get_state().touch()


    remote_executor_cache = {}

    def remote_executors_with_task_confs(self):

        def gen():
            for task in self.tasks:
                if task.is_remote():
                    e = task.executer
                    task_conf = task.task_conf
                    unicity_key = (
                        e.server_connection_key(),
                        task_conf.remote_base_dir,
                        task_conf.remote_code_dir,
                        task_conf.remote_containers_dir
                    )

                    cached_executer = PipelineInstance.remote_executor_cache.get(unicity_key)

                    if cached_executer is None:
                        PipelineInstance.remote_executor_cache[unicity_key] = e
                    else:
                        e = cached_executer

                    yield unicity_key, (e, task_conf)

        return dict(gen()).values()

    def regen_tasks_if_stale(self, force=False):
        self.tasks = self.tasks.regen_if_stale_else_self(force)

    def instance_dir_base_name(self):
        return os.path.basename(self.pipeline_instance_dir)

    def _recalc_hash_script(self):
        return os.path.join(self._work_dir, "recalc-output-file-hashes.sh")

    def work_dir_exists(self):
        return os.path.exists(self._work_dir)

    def tasks_for_glob_expr(self, glob_expr):
        return [
            task
            for task in self.tasks
            if pathlib.PurePath(task.key).match(glob_expr)
        ]

    def tasks_for_key_prefix(self, key_prefix):
        for task in self.tasks:
            if task.key.startswith(key_prefix):
                yield task

    def clean(self):

        for task in self.tasks:
            task.clean()

    def clean_all(self):
        if os.path.exists(self._work_dir):
            shutil.rmtree(self._work_dir)

        if os.path.exists(self._publish_dir):
            shutil.rmtree(self._publish_dir)

    def get_state(self, create_if_not_exists=False):
        return PipelineState.from_pipeline_work_dir(self._work_dir, create_if_not_exists)

    def pre_existing_file_deps(self):
        def gen_deps():
            for task in self.tasks:
                for name, file in task.pre_existing_files.items():
                    yield file.file_path

        return list(gen_deps())

    def summarized_dependency_graph(self):

        def gen_deps():
            for task in self.tasks:
                for upstream_task, upstream_input_files, upstream_input_vars in task.upstream_deps_iterator():
                    yield [
                        Task.key_grouper(task.key),
                        [
                            k.produced_file.var_name
                            for k in upstream_input_files
                        ] + [
                            k.output_var.name
                            for k in upstream_input_vars
                        ],
                        Task.key_grouper(upstream_task.key)
                    ]

                for k, m in task.task_matchers.items():
                    for upstream_task in self.tasks:
                        if pathlib.PurePath(upstream_task.key).match(m.task_keys_glob_pattern):
                            yield [
                                Task.key_grouper(task.key),
                                [m.task_keys_glob_pattern],
                                Task.key_grouper(upstream_task.key)
                            ]

        def g(iterable, func):
            return groupby(sorted(iterable, key=func), key=func)

        def gen_tasks():
            for _t, tasks in g(self.tasks, lambda t: Task.key_grouper(t.key)):
                yield {
                    "key_group": _t,
                    "keys": list([t.key for t in tasks])
                }

        def gen_group_deps():
            for _d, deps in g(gen_deps(), lambda t: f"{t[0]}-{t[2]}"):
                yield next(deps)

        return {
            "taskGroups": list(gen_tasks()),
            "deps": list(gen_group_deps())
        }

    # TODO: avoid name clashes when pre existing files with the same names
    def calc_pre_existing_files_signatures(self, force_recalc=False):

        def name_file_paths_tuples():
            for task in self.tasks:
                for name, pre_existing_file in task.pre_existing_files.items():
                    p = task.abs_from_pipeline_instance_dir(pre_existing_file.absolute_path(task))

                    if not os.path.exists(p):
                        raise Exception(f"{task} depends on missing pre existing file '{name}'='{p}'")

                    yield p

        all_pre_existing_files = {
            f for f in name_file_paths_tuples()
        }

        sig_dir = os.path.join(self._work_dir, "in_sigs")

        if not os.path.exists(sig_dir):

            pathlib.Path(sig_dir).mkdir(parents=True, exist_ok=True)

            with subprocess.Popen(
                    f"{self._work_dir}/recalc-output-file-hashes.sh",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env={
                        "__control_dir": self._work_dir,
                        "__sig_dir": "in_sigs",
                        "__file_list_to_sign": ",".join(all_pre_existing_files)
                    }
            ) as p:
                p.wait()
                err = p.stderr.read()
                if p.returncode != 0:
                    raise Exception(f"Failed signing pre existing files: {err}")

    """
    Recompute output signatures (out.sig) of all tasks, and identifies tasks who's inputs has changed by 
    
    creating a filed named "in.sig.changed" in the task's control_dir.
    """

    def recompute_signatures(self, force_recalc_of_preexisting_file_signatures=True):

        self.calc_pre_existing_files_signatures(force_recalc_of_preexisting_file_signatures)

        for task in self.tasks:
            task.recompute_output_singature(self._recalc_hash_script())

        total_changed = 0

        for task in self.tasks:
            if task.has_completed():

                previously_computed_input_signature = task.input_signature()

                up_to_date_input_signature, in_sig_file_writer = task.calc_input_signature()

                if previously_computed_input_signature != up_to_date_input_signature:
                    total_changed += 1
                    in_sig_file_writer(write_changed_flag=True)
                else:
                    task.clear_input_changed_flag()

        return total_changed

    def run_sync(self, tmp_env_vars={}, fail_silently=False):

        for k, v in tmp_env_vars.items():
            os.environ[k] = v

        self.init_work_dir()

        j = Janitor(pipeline_instance=self)
        i = j.iterate_main_work(sync_mode=True, fail_silently=fail_silently)
        has_work = next(i)
        while has_work:
            has_work = next(i)

        for k, v in tmp_env_vars.items():
            del os.environ[k]


SLURM_SQUEUE_FORMAT_SPEC = "%A %L %j %l %T"


def parse_status_from_squeue_line(squeue_line):
    return squeue_line[4]


def call_squeue_for_job_id(job_id, parser=None):
    cmd = f"squeue -h -j {job_id} -o '{SLURM_SQUEUE_FORMAT_SPEC}'"

    with subprocess.Popen(
            cmd.split(" "),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
    ) as p:

        p.wait()

        if p.returncode != 0:
            raise Exception(f"call failed '{cmd}', return code: {p.returncode}\n{p.stderr}")

        out_line = p.stdout.read().strip()

        if len(out_line) == 0:
            return None
        elif len(out_line) > 1:
            raise Exception(f"squeue dared return more than one line when given {cmd}")

        line = map(lambda s: s.strip(), out_line[0].split(" "))

        if parser is not None:
            return parser(line)

        return line


def is_module_command_available():
    cmd = ["bash", "-c", "type -t module"]
    with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
    ) as p:
        p.wait()
        if p.returncode != 0:
            raise Exception(f"Failed while checking if module available {cmd} ")

        out = p.stdout.read().strip()

        return out == "function"
