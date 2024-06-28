import os
import shutil
import unittest
from pathlib import Path

from dry_pipe import TaskConf
from dry_pipe.pipeline import Pipeline
from dry_pipe.task_process import TaskProcess


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

    def assert_file_content_equals(self, file, expected_content):
        with open(file) as f:
            self.assertEqual(f.read().strip(), expected_content)

    def launch_task_in_current_process(self, state_file):

        self.assertFalse(state_file.is_completed())
        env_copy = os.environ.copy()
        tp = TaskProcess(state_file.control_dir(), as_subprocess=False)
        tp.run(wait_for_completion=True)
        self.assertEqual(os.environ, env_copy)

    def create_monitor(self):
        return None

    def init_pipeline_instance(self, pipeline_instance):
        pass

    def create_pipeline_instance(self, other_pipeline_instance_dir=None):
        pipeline = Pipeline(lambda dsl: self.dag_gen(dsl), pipeline_code_dir=self.pipeline_code_dir)
        if other_pipeline_instance_dir is not None:
            pipeline_instance = pipeline.create_pipeline_instance(other_pipeline_instance_dir)
        else:
            pipeline_instance = pipeline.create_pipeline_instance(self.pipeline_instance_dir)

        self.init_pipeline_instance(pipeline_instance)

        return pipeline_instance

    def run_pipeline(self, until_patterns=None):

        pipeline_instance = self.create_pipeline_instance()

        pipeline_instance.run_sync(
            until_patterns=until_patterns,
            run_tasks_in_process=self.launches_tasks_in_process(),
            monitor=self.create_monitor()
        )

        tasks_by_keys = {
            t.key: t
            for t in pipeline_instance.query("*", include_incomplete_tasks=self.is_fail_test())
        }

        self.validate(tasks_by_keys)

        return pipeline_instance


    def test_run_pipeline(self):
        self.run_pipeline()

    def task_conf(self):
        tc = TaskConf.default()
        tc.extra_env = {
            "PYTHONPATH": os.path.dirname(__file__)
        }
        return tc

    def launches_tasks_in_process(self):
        return False

    def is_fail_test(self):
        return False

    def dag_gen(self, dsl):
        raise NotImplementedError()

    def validate(self, tasks_by_keys):
        raise NotImplementedError()
