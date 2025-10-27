import os
import shutil
import unittest
from pathlib import Path

from dry_pipe.pipeline import Pipeline, PipelineType
from dry_pipe.service import PipelineRunner
from pipeline_tests_with_slurm_arrays import PipelineWithPartialArrayMatch
from dry_pipe_tests.pipeline_tests_with_local_slurm import python_path_for_tests
from dry_pipe_tests.pipeline_tests_with_multiple_tasks import PipelineWithVariablePassing
from dry_pipe_tests.pipeline_tests_with_single_tasks import PipelineWithSingleBashTask, PipelineWithSinglePythonTask


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

    def create_pipeline_instance(self, pipeline_instance_dir, logger=None):
        p = Pipeline(lambda dsl: self.base_pipeline_test.dag_gen(dsl))
        self.pipeline_instance = p.create_pipeline_instance(pipeline_instance_dir, logger)
        return self.pipeline_instance


    def validate_pipeline_instance(self, pipeline_instance):

        tasks_by_keys = {
            t.key: t
            for t in pipeline_instance.query("*")
        }

        self.base_pipeline_test.validate(tasks_by_keys)


def test_conf_generator():
    s = ServiceRunnerTest1()
    yield from s.create_runner_conf(dif_suffix="_service")


class BaseServiceRunnerTest(TestWithDirectorySandbox2):

    def is_log_level_debug(self):
        return False

    def custom_sleep_schedule(self):
        return 0

    def prepare(self, parent_dir, instance_name):
        instance_dir = Path(self.dir, parent_dir, instance_name)
        instance_dir.mkdir(parents=True, exist_ok=True)
        pipeline_work_dir = instance_dir.joinpath(".drypipe")
        pipeline_work_dir.mkdir(parents=True, exist_ok=True)

        sf = list(pipeline_work_dir.glob("state.*"))

        if len(sf) == 0:
            state_file = pipeline_work_dir.joinpath("state.ready")
            state_file.touch()

        return instance_dir


    def test(self):

        pipeline_runner = PipelineRunner(
            self.create_runner_conf(),
            run_sync=True,
            run_tasks_in_process=True,
            sleep_schedule=[0, 0, 0,0,0,None]
        )

        pipeline_runner.watch()

        for pid, pipeline_accessor in pipeline_runner.pipeline_instances.items():

            state = pipeline_accessor.latest_state()

            self.assertEqual(state, "stopped")

            pipeline_accessor.pipeline_type.post_run_validator(pipeline_accessor.pipeline_instance)


class ServiceRunnerTest1(BaseServiceRunnerTest):


    def create_runner_conf(self, dif_suffix=""):
        self.dir = self.dir + dif_suffix
        self.prepare("a", "a1")
        self.prepare("a", "a2"),
        self.prepare("b", "b1")
        self.prepare("c", "c1")
        self.prepare("c", "c2")

        from dry_pipe import TaskConf

        class C(PipelineWithSinglePythonTask):
            def task_conf(self):
                tc = TaskConf.default()
                tc.extra_env = {
                    "PYTHONPATH": python_path_for_tests,
                    "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                    "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
                }
                return tc


        a = TestPipeline(PipelineWithSingleBashTask())
        b = TestPipeline(C())
        c = TestPipeline(PipelineWithVariablePassing())

        yield str(Path(self.dir, "a")), PipelineType("a", a, lambda : None, {}, {}, None, a.validate_pipeline_instance)
        yield str(Path(self.dir, "b")), PipelineType("b", b, lambda : None, {}, {}, None, b.validate_pipeline_instance)
        yield str(Path(self.dir, "c")), PipelineType("c", c, lambda : None, {}, {}, None, c.validate_pipeline_instance)




class ServiceRunnerTest2(BaseServiceRunnerTest):

    def create_runner_conf(self, dif_suffix=""):
        self.dir = self.dir + dif_suffix


        a = PipelineWithPartialArrayMatch()
        a.pipeline_instance_dir = self.prepare("a", "a1")
        a.init_instance()

        p = TestPipeline(a)


        yield str(Path(self.dir, "a")), PipelineType("a", p, lambda : None, {}, {}, None, p.validate_pipeline_instance)
