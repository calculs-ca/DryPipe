import os
import shutil
import unittest
from pathlib import Path

from base_pipeline_test import BasePipelineTest
from dry_pipe.pipeline import PipelineInstance
from dry_pipe.script_lib import launch_task, load_task_conf_dict, UpstreamTasksNotCompleted
import pipeline_tests_with_single_tasks
import pipeline_tests_with_multiple_tasks


class TaskLaunchTest(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        all_sandbox_dirs = os.path.join(
            os.path.dirname(__file__),
            "sandboxes"
        )

        self.pipeline_code_dir = os.path.dirname(__file__)
        self.pipeline_instance_dir = os.path.join(all_sandbox_dirs, self.__class__.__name__)
        self.pipeline_instance: PipelineInstance = None
        self.exceptions_raised = set()

    def setUp(self):

        d = Path(self.pipeline_instance_dir)
        if d.exists():
            shutil.rmtree(d)

        t = self.pipeline_test()
        self.pipeline_instance = t.create_pipeline_instance(self.pipeline_instance_dir)

    def prepare_env_and_launch_task(self, state_file, task_conf_json):

        self.assertFalse(state_file.is_completed())
        env_copy = os.environ.copy()
        try:
            os.environ["__script_location"] = os.path.join(
                self.pipeline_instance.state_file_tracker.pipeline_work_dir,
                state_file.task_key
            )
            load_task_conf_dict()
            launch_task(wait_for_completion=True, task_conf_dict=task_conf_json, exit_process_when_done=False)
        finally:
            os.environ.clear()
            for k, v in env_copy.items():
                os.environ[k] = v
            self.assertEqual(os.environ.copy(), env_copy)

    def test_run(self):

        def e(state_file):
            self.prepare_env_and_launch_task(state_file, state_file.load_task_conf_json())

        self.pipeline_instance.run_sync(executer_func=e)

        self.validate_after_all_launched()

    def validate_after_all_launched(self):
        for task in self.pipeline_instance.query("*", include_incomplete_tasks=False):
            self.assertIsNotNone(task)
            self.assertTrue(task.is_completed())

    def pipeline_test(self) -> BasePipelineTest:
        raise NotImplementedError()


class BashTaskLauncherTest(TaskLaunchTest):

    def pipeline_test(self):
        return pipeline_tests_with_single_tasks.PipelineWithSingleBashTask()


class PythonTaskLauncherTest(TaskLaunchTest):

    def pipeline_test(self):
        return pipeline_tests_with_single_tasks.PipelineWithSinglePythonTask()

class PipelineWithVariablePassingTaskLauncherTest(TaskLaunchTest):

    def pipeline_test(self) -> BasePipelineTest:
        return pipeline_tests_with_multiple_tasks.PipelineWithVariablePassing()

class EnsureFailOfLaunchWhenUnsatisfiedUpstreamDependencyTest(TaskLaunchTest):

    def test_run(self):

        self.pipeline_instance.run_sync(queue_only_pattern="*")

        consume_and_produce_a_var = self.pipeline_instance.lookup_single_task(
            "consume_and_produce_a_var",
            include_incomplete_tasks=True
        )

        def f():
            self.prepare_env_and_launch_task(
                consume_and_produce_a_var.state_file,
                consume_and_produce_a_var.state_file.load_task_conf_json()
            )

        self.assertRaises(UpstreamTasksNotCompleted, f)


    def pipeline_test(self) -> BasePipelineTest:
        return pipeline_tests_with_multiple_tasks.PipelineWithVariablePassing()


# tests in containers

class BashTaskLauncherTestInContainer(TaskLaunchTest):

    def pipeline_test(self):
        return pipeline_tests_with_single_tasks.PipelineWithSingleBashTaskInContainer()


class PythonTaskLauncherTestInContainer(TaskLaunchTest):

    def pipeline_test(self):
        return pipeline_tests_with_single_tasks.PipelineWithSinglePythonTaskInContainer()


def all_launch_tests():
    return [
        BashTaskLauncherTest,
        PythonTaskLauncherTest,
        PipelineWithVariablePassingTaskLauncherTest,
        EnsureFailOfLaunchWhenUnsatisfiedUpstreamDependencyTest,
        BashTaskLauncherTestInContainer,
        PythonTaskLauncherTestInContainer
    ]
