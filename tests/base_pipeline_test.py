import os
import shutil
import unittest
from pathlib import Path

from dry_pipe import TaskConf
from dry_pipe.pipeline import Pipeline
from dry_pipe.script_lib import TaskProcess


class TestWithDirectorySandbox(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        all_sandbox_dirs = os.path.join(
            os.path.dirname(__file__),
            "sandboxes"
        )
        self.pipeline_code_dir = os.path.dirname(__file__)
        self.pipeline_instance_dir = os.path.join(all_sandbox_dirs, self.__class__.__name__)

    def setUp(self):

        d = Path(self.pipeline_instance_dir)
        if d.exists():
            shutil.rmtree(d)



class BasePipelineTest(TestWithDirectorySandbox):

    def launch_task_in_current_process(self, state_file):

        self.assertFalse(state_file.is_completed())
        env_copy = os.environ.copy()
        try:
            os.environ["__script_location"] = os.path.join(
                state_file.tracker.pipeline_work_dir,
                state_file.task_key
            )
            r = TaskProcess()
            r.launch_task(wait_for_completion=True, exit_process_when_done=False)
        finally:
            os.environ.clear()
            for k, v in env_copy.items():
                os.environ[k] = v
            self.assertEqual(os.environ.copy(), env_copy)


    def run_pipeline(self, queue_only_pattern=None):

        pipeline = Pipeline(lambda dsl: self.dag_gen(dsl), pipeline_code_dir=self.pipeline_code_dir)
        pipeline_instance = pipeline.create_pipeline_instance(self.pipeline_instance_dir)

        if self.launches_tasks_in_process():
            executer_func = lambda state_file: self.launch_task_in_current_process(state_file)
        else:
            executer_func = None

        if self.is_fail_test():
            pipeline_instance.run_sync(sleep=0, executer_func=executer_func, queue_only_pattern=queue_only_pattern)
            tasks_by_keys = {
                t.key: t
                for t in pipeline_instance.query("*", include_incomplete_tasks=True)
            }
        else:
            tasks_by_keys = pipeline_instance.run_sync(
                sleep=0, executer_func=executer_func, queue_only_pattern=queue_only_pattern
            )

        self.validate(tasks_by_keys)

        return pipeline_instance


    def test_run_pipeline(self):
        self.run_pipeline()

    def task_conf(self):
        return TaskConf.default()

    def launches_tasks_in_process(self):
        return False

    def is_fail_test(self):
        return False

    def dag_gen(self, dsl):
        raise NotImplementedError()

    def validate(self, tasks_by_keys):
        raise NotImplementedError()
