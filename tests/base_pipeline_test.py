import os
import shutil
import unittest
from pathlib import Path

from dry_pipe import TaskConf
from dry_pipe.pipeline_runner import PipelineRunner
from dry_pipe.state_machine import StateFileTracker, StateMachine


class BasePipelineTest(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        all_sandbox_dirs = os.path.join(
            os.path.dirname(__file__),
            "sandboxes"
        )

        self.pipeline_instance_dir = os.path.join(all_sandbox_dirs, self.__class__.__name__)
        self.tasks_by_keys = {}

    def setUp(self):

        d = Path(self.pipeline_instance_dir)
        if d.exists():
            shutil.rmtree(d)
        self.run_pipeline()

    def task_conf(self):
        return TaskConf.default()

    def run_pipeline(self):
        Path(self.pipeline_instance_dir).mkdir(exist_ok=False)
        t = StateFileTracker(self.pipeline_instance_dir)
        state_machine = StateMachine(t, lambda dsl: self.dag_gen(dsl))
        pr = PipelineRunner(state_machine)
        pr.run_sync()

        self.tasks_by_keys = {
            t.key: t
            for t in t.load_completed_tasks_for_query()
        }

    def dag_gen(self, dsl):
        raise NotImplementedError()

    def test_validate(self):
        raise NotImplementedError()
