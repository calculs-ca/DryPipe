import os
import shutil
import unittest
from pathlib import Path

from pipeline import Pipeline
from service import PipelineRunner
from tests.pipeline_tests_with_multiple_tasks import PipelineWithVariablePassing
from tests.pipeline_tests_with_single_tasks import PipelineWithSingleBashTask, PipelineWithSinglePythonTask


class TestWithDirectorySandbox2(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        all_sandbox_dirs = os.path.join(
            os.path.dirname(__file__),
            "sandboxes"
        )

        self.dir = os.path.join(all_sandbox_dirs, self.__class__.__name__)

    def setUp(self):

        d = Path(self.dir)
        if d.exists():
            shutil.rmtree(d)


class TestPipeline:

    def __init__(self, base_pipeline_test):
        self.base_pipeline_test = base_pipeline_test
        self.pipeline_instance = None

    def create_pipeline_instance(self, pipeline_instance_dir):
        p = Pipeline(lambda dsl: self.base_pipeline_test.dag_gen(dsl))
        self.pipeline_instance = p.create_pipeline_instance(pipeline_instance_dir)
        return self.pipeline_instance


    def validate_pipeline_instance(self):

        tasks_by_keys = {
            t.key: t
            for t in self.pipeline_instance.query("*")
        }

        self.base_pipeline_test.validate(tasks_by_keys)



class ServiceRunnerTest1(TestWithDirectorySandbox2):

    def test(self):

        def prepare(parent_dir, instance_name):
            instance_dir = Path(self.dir, parent_dir, instance_name)
            instance_dir.mkdir(parents=True)
            wd = instance_dir.joinpath(".drypipe")
            wd.mkdir(parents=True)
            wd.joinpath("state.ready").touch()
            return instance_dir

        prepare("a", "a1")
        prepare("a", "a2"),
        prepare("b", "b1")
        prepare("c", "c1")
        prepare("c", "c2")

        from dry_pipe import TaskConf

        class C(PipelineWithSinglePythonTask):
            def task_conf(self):
                return TaskConf.default()

        a = TestPipeline(PipelineWithSingleBashTask())
        b = TestPipeline(C())
        c = TestPipeline(PipelineWithVariablePassing())

        d = {
            str(Path(self.dir, "a")): a,
            str(Path(self.dir, "b")): b,
            str(Path(self.dir, "c")): c
        }

        pipeline_runner = PipelineRunner(
            d,
            run_sync=True,
            run_tasks_in_process=True,
            sleep_schedule=[0, 0, 0,0,0,None]
        )

        pipeline_runner.watch()

        for p in pipeline_runner.pipeline_instances:
            p.pipeline.validate_pipeline_instance()
