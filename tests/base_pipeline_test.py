import json
import os
import shutil
import unittest
from pathlib import Path

from dry_pipe.core_lib import SleepySpinner, PortablePopen
from dry_pipe import TaskConf
from dry_pipe.pipeline import Pipeline


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

    def create_monitor(self):
        return None

    def init_pipeline_instance(self, pipeline_instance):
        pass

    def spin_until_no_running_jobs(self, sleep_schedule=(2, 1)):
        with SleepySpinner(sleep_schedule) as ss:
            while True:
                with PortablePopen("squeue --noheader -o %j", shell=True) as p:
                    p.wait_and_raise_if_non_zero()
                    res = p.stdout_as_string().strip()
                    if res == "":
                        return
                    ss.sleep()

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
        pipeline_instance.monitor=self.create_monitor()

        pipeline_instance.run_sync(
            until_patterns=until_patterns,
            run_tasks_in_process=self.launches_tasks_in_process()
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


    def save_crash_plan(self, crash_plan):
        with open(Path(self.pipeline_instance_dir, "crash-plan.json"), "w") as f:
            f.write(json.dumps(crash_plan))

        for f in Path(self.pipeline_instance_dir).glob("crash_count*"):
            f.unlink()