import importlib
import inspect
import logging
import os

from dry_pipe import TaskConf
from dry_pipe.pipeline_instance import PipelineInstance
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

