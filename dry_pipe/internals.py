import inspect
import os
import logging.config

logger = logging.getLogger(__name__)


class SubPipeline:
    def __init__(self, pipeline, task_namespance_prefix, dsl):
        self.pipeline = pipeline
        self.task_namespance_prefix = task_namespance_prefix
        self.dsl = dsl

    def wait_for_tasks(self, *args):
        args = [f"{self.task_namespance_prefix}{a}" for a in args]
        return self.dsl.wait_for_tasks(*args)



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

