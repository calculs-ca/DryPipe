import glob
import importlib
import inspect
import logging
import os
import pathlib
import shutil
from itertools import groupby

from dry_pipe import TaskConf, DryPipeDsl, TaskBuilder, script_lib, bash_shebang, TaskState, TaskMatch
from dry_pipe.internals import ValidationError, SubPipeline
from dry_pipe.janitors import Janitor
from dry_pipe.pipeline_runner import PipelineRunner
from dry_pipe.pipeline_state import PipelineState
from dry_pipe.script_lib import write_pipeline_lib_script, PortablePopen, FileCreationDefaultModes
from dry_pipe.state_machine import StateFileTracker, StateMachine
from dry_pipe.task import Task

logger = logging.getLogger(__name__)


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
            task_generator,
            pipeline_code_dir=None,
            task_conf=None,
            containers_dir=None,
            remote_task_confs=None,
            task_groupers=None,
            pipeline_code_dir_ls_command=None
    ):
        if pipeline_code_dir is None:
            pipeline_code_dir = os.path.dirname(os.path.abspath(inspect.getmodule(task_generator).__file__))

        if task_conf is None:
            task_conf = TaskConf("process")

        if remote_task_confs is not None:
            for tc in remote_task_confs:
                if not isinstance(tc, TaskConf):
                    raise Exception(f"invalid type {type(tc)} given to remote_task_confs")

        self.pipeline_code_dir = pipeline_code_dir
        self.containers_dir = containers_dir
        self.task_conf = task_conf
        self.remote_task_confs = remote_task_confs
        self.pipeline_code_dir_ls_command = pipeline_code_dir_ls_command

        def wrap_task_gen_to_set_task_conf_defaults(dsl):
            for task in task_generator(dsl):
                if task.task_conf is None:
                    task.task_conf = task_conf
                yield task

        self.task_generator = wrap_task_gen_to_set_task_conf_defaults

        def _wrap_task_grouper(grouper_name, grouper_func):
            #if task_key.count(".") != 1:
            #    raise Exception(f"task key {task_key} violates naming convention: <prefix>.<suffix>")

            def g(task_key):
                try:
                    return grouper_func(task_key)
                except Exception as ex:
                    raise Exception(f"task grouper {grouper_name} failed on task_key: {task_key}\n{ex}")
            return g

        if task_groupers is None:
            task_groupers = {}

        self.task_groupers = {
            ** {
                n: _wrap_task_grouper(n, g)
                for n, g in task_groupers.items()
            },
            "by_task_type": Task.key_grouper
        }


    def create_pipeline_instance(self, pipeline_instance_dir):
        return PipelineInstance(self, pipeline_instance_dir)

    @staticmethod
    def pipeline_instances_iterator(instances_dir_for_pipeline_dict, ignore_completed=True):

        if instances_dir_for_pipeline_dict is None:
            raise Exception(f"pipeline_instances_dir can't be None")

        if len(instances_dir_for_pipeline_dict) == 0:
            raise Exception(f"pipeline_instances_dir can't be empty")

        return PipelineInstancesIterator(instances_dir_for_pipeline_dict, ignore_completed)

    def prepare_remote_sites_funcs(self):
        if self.remote_task_confs is None:
            raise Exception(f"no remote_task_confs have been assigned for this pipeline")

        for task_conf in self.remote_task_confs:

            def go():
                ssh_executer = task_conf.create_executer()
                ssh_executer.rsync_remote_code_dir_if_applies(self, task_conf)

            ssh_executer = task_conf.create_executer()
            yield go, f"Will rsync {self.pipeline_code_dir} to {ssh_executer.user_at_host()}:{task_conf.remote_pipeline_code_dir}"


        for task_conf in self.remote_task_confs:
            if task_conf.container is not None:
                ssh_executer = task_conf.create_executer()
                def go():
                    ssh_executer.rsync_remote_container(task_conf)

                yield go, f"Will upload container {task_conf.container} to "+\
                          f"{ssh_executer.user_at_host()}:{task_conf.remote_containers_dir}"

    def prepare_remote_sites(self):
        for f, msg in self.prepare_remote_sites_funcs():
            f()

class PipelineInstancesIterator:

    def __init__(self, instances_dir_for_pipeline_dict, ignore_completed):
        self.pipelines_cache = {}
        self.ignore_completed = ignore_completed
        self.instances_dir_for_pipeline_dict = instances_dir_for_pipeline_dict

    def create_instance_in(self, pipeline_instances_dir, subdir):

        pid = pathlib.Path(os.path.join(pipeline_instances_dir, subdir, ".drypipe"))
        pathlib.Path(pid).mkdir(parents=True, exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)
        pathlib.Path(os.path.join(pid, "state.running")).touch()

    def __iter__(self):

        pipelines_by_dir = {**self.pipelines_cache}

        def gen_():

            for pipeline_instances_dir, pipeline in self.instances_dir_for_pipeline_dict.items():

                logger.debug("will iterate on pipeline instances in %s ", pipeline_instances_dir)

                for pipeline_state in PipelineState.iterate_from_instances_dir(pipeline_instances_dir):

                    if pipeline_state.is_completed() and self.ignore_completed:
                        logger.debug("pipeline completed: %s", pipeline_state.state_file)
                        continue
                    else:
                        logger.debug("pipeline NOT completed: %s, %s", pipeline_state.state_file,
                                     pipeline_state.state_name)

                    p = pipelines_by_dir.get(pipeline_state.dir_basename())

                    if p is None:
                        p = pipeline.create_pipeline_instance(pipeline_state.instance_dir())

                    yield pipeline_state.dir_basename(), p

        self.pipelines_cache = dict(gen_())

        for k, pipeline in self.pipelines_cache.items():
            yield pipeline


class PipelineInstance:

    def __init__(self, pipeline, pipeline_instance_dir):
        self.pipeline = pipeline
        self.state_file_tracker = StateFileTracker(pipeline_instance_dir)
        self.state_file_tracker.prepare_instance_dir({
            "__pipeline_code_dir": pipeline.pipeline_code_dir,
            "__containers_dir": pipeline.containers_dir
        })

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

    def output_dir(self):
        return self._publish_dir

    def remote_sites_task_confs(self, for_zombie_detection=False):

        def f(task):
            if task.is_remote():
                if not for_zombie_detection:
                    return True
                task_state = task.get_state()
                return task_state.is_step_started() or task_state.is_launched() or task_state.is_scheduled()
            else:
                return False

        return {
            task.task_conf.remote_site_key: task.task_conf
            for task in self.tasks
            if f(task)
        }.values()


    def get_state(self, create_if_not_exists=False):
        return PipelineState.from_pipeline_work_dir(self.state_file_tracker.pipeline_work_dir, create_if_not_exists)

    def run_sync(self, queue_only_pattern=None, fail_silently=True, sleep=1, executer_func=None):

        state_machine = StateMachine(
            self.state_file_tracker,
            lambda dsl: self.pipeline.task_generator(dsl),
            queue_only_pattern=queue_only_pattern
        )
        pr = PipelineRunner(state_machine, executer_func=executer_func)
        pr.run_sync(fail_silently=fail_silently, sleep=sleep)

        tasks_by_keys = {
            t.key: t
            for t in self.state_file_tracker.load_tasks_for_query()
        }

        return tasks_by_keys

    def query(self, glob_pattern, include_incomplete_tasks=False):
        yield from self.state_file_tracker.load_tasks_for_query(
            glob_pattern, include_non_completed=include_incomplete_tasks
        )

    def lookup_single_task_or_none(self, task_key, include_incomplete_tasks=False):
        return self.state_file_tracker.load_single_task_or_none(
            task_key, include_non_completed=include_incomplete_tasks
        )

    def lookup_single_task(self, task_key, include_incomplete_tasks=False):
        task = self.lookup_single_task_or_none(task_key, include_incomplete_tasks)
        if task is not None:
            return task
        else:
            raise Exception(f"expected a task with key {task_key}, found none")




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
