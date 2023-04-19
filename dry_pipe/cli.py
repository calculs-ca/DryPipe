import os
import sys

from dry_pipe.core_lib import func_from_mod_func, TaskProcess


def call(mod_func):

    python_task = func_from_mod_func(mod_func)
    control_dir = os.environ["__control_dir"]
    task_runner = TaskProcess(control_dir)
    task_runner.resolve_task_env()
    task_runner.call_python(mod_func, python_task)



class CliSession:

    def __init__(self):
        self.pipeline_instance_dir = None
        self.pipeline_mod_func = None



if __name__ == '__main__':
    call(sys.argv[2])
