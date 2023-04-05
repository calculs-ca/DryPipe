import os
import shutil
import unittest
from pathlib import Path

from dry_pipe import TaskConf
from dry_pipe.pipeline import Pipeline, PipelineInstance
from dry_pipe.pipeline_runner import PipelineRunner
from dry_pipe.state_machine import StateFileTracker, StateMachine


class BasePipelineTest(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        all_sandbox_dirs = os.path.join(
            os.path.dirname(__file__),
            "sandboxes"
        )

        self.pipeline_code_dir = os.path.dirname(__file__)

        self.pipeline_instance_dir = os.path.join(all_sandbox_dirs, self.__class__.__name__)
        #self.tasks_by_keys = {}

    def setUp(self):

        d = Path(self.pipeline_instance_dir)
        if d.exists():
            shutil.rmtree(d)

    def task_conf(self):
        return TaskConf.default()

    def create_pipeline_instance(self, instance_dir) -> PipelineInstance :
        p = Pipeline(lambda dsl: self.dag_gen(dsl), pipeline_code_dir=self.pipeline_code_dir)
        return p.create_pipeline_instance(instance_dir)

    def test_run_pipeline(self):
        pi = self.create_pipeline_instance(self.pipeline_instance_dir)
        if self.is_fail_test():
            pi.run_sync(sleep=0)
            tasks_by_keys = {
                t.key: t
                for t in pi.query("*", include_incomplete_tasks=True)
            }
        else:
            tasks_by_keys = pi.run_sync(sleep=0)

        self.validate(tasks_by_keys)

    def is_fail_test(self):
        return False

    def dag_gen(self, dsl):
        raise NotImplementedError()

    def validate(self, tasks_by_keys):
        raise NotImplementedError()
