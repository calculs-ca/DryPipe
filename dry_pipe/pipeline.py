import importlib
import inspect
import logging
import os
import pathlib
import shutil
from itertools import groupby

from dry_pipe import TaskConf, DryPipeDsl, TaskBuilder, script_lib, bash_shebang
from dry_pipe.internals import ValidationError, SubPipeline
from dry_pipe.janitors import Janitor
from dry_pipe.pipeline_state import PipelineState
from dry_pipe.script_lib import write_pipeline_lib_script, PortablePopen
from dry_pipe.task import Task

logger = logging.getLogger(__name__)


class TaskSet:

    def __init__(self, tasks_by_keys, task_states_signature_func):
        self._tasks_by_keys = tasks_by_keys
        self._task_states_signature_func = task_states_signature_func
        self._task_states_signature = self._task_states_signature_func()

    def __iter__(self):
        for t in self._tasks_by_keys.values():
            yield t

    def __len__(self):
        return len(self._tasks_by_keys)

    def __getitem__(self, task_key):

        task = self._tasks_by_keys.get(task_key)

        if task is not None:
            return task

        raise KeyError(f"pipeline has no task {task_key}")

    def get(self, task_key):
        return self._tasks_by_keys.get(task_key)

    def is_stale(self):
        return self._task_states_signature_func() != self._task_states_signature


class Pipeline:

    @staticmethod
    def load_from_module_func(module_func_pipeline):
        """
        :param module_func_pipeline:
            <a_module_path>:<function_name>
            a_python_module.python_file:function_returns_a_pipeine
            a_module_path must be in PYTHONPATH
        :return an instance of dry_pipe.Pipeline:
        """

        mod, func_name = module_func_pipeline.split(":")

        module = importlib.import_module(mod)

        func = getattr(module, func_name, None)

        if func is None:
            raise Exception(f"function {func_name} not found in module {mod}")

        pipeline = func()

        if not isinstance(pipeline, Pipeline):
            raise Exception(f"function {func_name} in {mod} should return a Pipeline, not {type(pipeline).__name__}")

        return pipeline

    def __init__(
            self,
            generator_of_tasks,
            pipeline_code_dir=None,
            task_conf=None,
            containers_dir=None,
            env_vars=None,
            remote_task_confs=None,
            task_groupers=None
    ):
        if pipeline_code_dir is None:
            pipeline_code_dir = os.path.dirname(os.path.abspath(inspect.getmodule(generator_of_tasks).__file__))

        if containers_dir is None:
            containers_dir = os.path.join(pipeline_code_dir, "containers")

        if task_conf is None:
            task_conf = TaskConf("process")

        def gen_task_set(pipeline_instance):

            try:
                iter(generator_of_tasks(None))
            except TypeError:
                raise ValidationError(f"function {generator_of_tasks} must return an iterator of Task (ex, via yield)")

            task_by_keys = {}

            def class_name(klass):
                return klass.__module__ + '.' + klass.__qualname__

            def _populate_task_by_keys_with_task_generator(dsl, g):

                for t_i in g(dsl):
                    if isinstance(t_i, Task):
                        # all tasks point to root pipeline_instance, even tasks of sub pipelines
                        t_i.pipeline_instance = pipeline_instance
                        task_by_keys[t_i.key] = t_i
                    elif isinstance(t_i, SubPipeline):
                        dsl2 = DryPipeDsl(
                            task_by_keys,
                            task_conf=task_conf,
                            pipeline_instance=pipeline_instance,
                            task_namespance_prefix=t_i.task_namespance_prefix
                        )
                        _populate_task_by_keys_with_task_generator(dsl2, t_i.pipeline.generator_of_tasks)
                    elif isinstance(t_i, TaskBuilder):
                        raise ValidationError(
                            f" task(key='{t_i.key}') declaration is incomplete, " +
                            "you probably need to invoke .calls(...) on it.")
                    else:
                        raise ValidationError(
                            f"iterator has yielded an invalid type: {type(t_i)}: '{t_i}', " +
                            f"valid types are {class_name(Task)} and {class_name(SubPipeline)}"
                        )

            _populate_task_by_keys_with_task_generator(
                DryPipeDsl(task_by_keys, task_conf=task_conf, pipeline_instance=pipeline_instance),
                generator_of_tasks
            )

            tasks = task_by_keys.values()

            def validate():
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

            validate()

            return TaskSet(
                task_by_keys,
                lambda: [
                    task.has_completed() for task in tasks
                    # optimization: we only need to trask the states of DAG changing tasks
                    if task.key in pipeline_instance.dag_determining_tasks_ids
                ]
            )

        self.task_set_generator = gen_task_set

        self.pipeline_code_dir = pipeline_code_dir
        self.task_conf = task_conf
        self.containers_dir = containers_dir
        self.generator_of_tasks = generator_of_tasks
        self.env_vars = env_vars
        self.remote_task_confs = remote_task_confs

        def _wrap_task_grouper(grouper_name, grouper_func):
            #if task_key.count(".") != 1:
            #    raise Exception(f"task key {task_key} violates naming convention: <prefix>.<suffix>")

            def g(task_key):
                try:
                    return grouper_func(task_key)
                except Exception as ex:
                    raise Exception(f"task grouper {grouper_name} failed on task_key: {task_key}\n{ex}")
            return g

        self.task_groupers = {
            ** {
                n: _wrap_task_grouper(n, g)
                for n, g in task_groupers.items()
            },
            "by_task_type": Task.key_grouper
        }


    def create_pipeline_instance(self, pipeline_instance_dir=None, task_conf=None, containers_dir=None, env_vars=None):

        if task_conf is not None or containers_dir is not None or env_vars is not None:
            p = Pipeline(
                generator_of_tasks=self.generator_of_tasks,
                pipeline_code_dir=self.pipeline_code_dir,
                task_conf=task_conf or self.task_conf,
                containers_dir=containers_dir or self.containers_dir,
                env_vars=env_vars or self.env_vars,
                task_groupers=self.task_groupers
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

    def prepare_remote_sites(self, printer=None):
        if self.remote_task_confs is None:
            raise Exception(f"no remote_task_confs have been assigned for this pipeline")
        for task_conf in self.remote_task_confs:
            ssh_executer = task_conf.create_executer()
            if printer is not None:
                printer(
                    f"Will rsync {self.pipeline_code_dir} to \n {task_conf}:{task_conf.remote_pipeline_code_dir}"
                )
            ssh_executer.connect()
            try:
                ssh_executer.rsync_remote_code_dir_if_applies(self, task_conf)
            finally:
                ssh_executer.close()


class PipelineInstance:

    def __init__(self, pipeline, pipeline_instance_dir):
        self.pipeline = pipeline
        self.pipeline_instance_dir = pipeline_instance_dir
        self.work_dir = os.path.join(pipeline_instance_dir, ".drypipe")
        self._publish_dir = os.path.join(pipeline_instance_dir, "publish")
        self.dag_determining_tasks_ids = set()
        self.tasks = self.pipeline.task_set_generator(self)
        self._is_remote_instance_directory_prepared = set()
        self._preparation_completed = 0

    def set_remote_instance_directory_prepared(self, server_key):
        self._is_remote_instance_directory_prepared.add(server_key)

    def is_remote_instance_directory_prepared(self, server_key):
        return server_key in self._is_remote_instance_directory_prepared

    def is_preparation_completed(self):

        if self._preparation_completed >= 2:
            return True

        def is_task_preparation_completed():
            for task in self.tasks:
                task_state = task.get_state()
                if task_state is None:
                    return False
                if task.key in self.dag_determining_tasks_ids and not task_state.is_completed():
                    return False
            return True

        if is_task_preparation_completed():
            self._preparation_completed += 1

        return self._preparation_completed >= 2

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
        pathlib.Path(self.work_dir).mkdir(parents=True, exist_ok=True)

        pipeline_env = os.path.join(self.work_dir, "pipeline-env.sh")
        with open(pipeline_env, "w") as f:
            f.write(f"{bash_shebang()}\n\n")
            f.write(f"export __pipeline_code_dir={self.pipeline.pipeline_code_dir}\n")
            f.write(f"export __containers_dir={self.pipeline.containers_dir}\n")
            if self.pipeline.env_vars is not None:
                for k, v in self.pipeline.env_vars.items():
                    f.write(f"export {k}={v}\n")

        os.chmod(pipeline_env, 0o764)

        #drypipe_cmds = os.path.join(self._work_dir, "dryfuncs")
        #with open(drypipe_cmds, "w") as _drypipe_cmds:
        #    _drypipe_cmds.write("#!/usr/bin/env python3\n\n")

        #    with open(os.path.join(os.path.dirname(__file__), "script_commands.py")) as f:
        #        _drypipe_cmds.write(f.read())

        #os.chmod(drypipe_cmds, 0o764)

        #drypipe_bash_lib = os.path.join(self._work_dir, "drypipe-bash-lib.sh")

        #with open(drypipe_bash_lib, "w") as f:
        #    f.write(f"{bash_shebang()}\n\n")
        #    f.write(BASH_TASK_FUNCS_AND_TRAPS)
        #    f.write(BASH_SIGN_FILES)

        #os.chmod(drypipe_bash_lib, 0o764)
        shutil.copy(script_lib.__file__, self.work_dir)

        script_lib_file = os.path.join(self.work_dir, "script_lib")
        with open(script_lib_file, "w") as script_lib_file_handle:
            write_pipeline_lib_script(script_lib_file_handle)
        os.chmod(script_lib_file, 0o764)

        #self.calc_pre_existing_files_signatures()
        self.get_state().touch()

    def remote_sites_task_confs(self):
        return {
            task.task_conf.remote_site_key: task.task_conf
            for task in self.tasks
            if task.is_remote()
        }.values()

    def regen_tasks_if_stale(self, force=False):
        if self.tasks.is_stale() or force:
            self.tasks = self.pipeline.task_set_generator(self)

    def instance_dir_base_name(self):
        return os.path.basename(self.pipeline_instance_dir)

    def _recalc_hash_script(self):
        return os.path.join(self.work_dir, "recalc-output-file-hashes.sh")

    def work_dir_exists(self):
        return os.path.exists(self.work_dir)

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
        if os.path.exists(self.work_dir):
            shutil.rmtree(self.work_dir)

        if os.path.exists(self._publish_dir):
            shutil.rmtree(self._publish_dir)

    def get_state(self, create_if_not_exists=False):
        return PipelineState.from_pipeline_work_dir(self.work_dir, create_if_not_exists)

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

        sig_dir = os.path.join(self.work_dir, "in_sigs")

        if not os.path.exists(sig_dir):

            pathlib.Path(sig_dir).mkdir(parents=True, exist_ok=True)

            with PortablePopen(
                    f"{self.work_dir}/recalc-output-file-hashes.sh",
                    env={
                        "__control_dir": self.work_dir,
                        "__sig_dir": "in_sigs",
                        "__file_list_to_sign": ",".join(all_pre_existing_files)
                    }
            ) as p:
                p.wait_and_raise_if_non_zero()

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

    with PortablePopen(cmd.split(" ")) as p:
        p.wait_and_raise_if_non_zero()
        out_line = p.stdout_as_string().strip()

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
    with PortablePopen(cmd) as p:
        p.wait_and_raise_if_non_zero()
        out = p.stdout_as_string().strip()
        return out == "function"
