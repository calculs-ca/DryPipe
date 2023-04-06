import os
import shutil
import unittest
from pathlib import Path

from base_pipeline_test import BasePipelineTest
from dry_pipe.script_lib import launch_task, UpstreamTasksNotCompleted
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
        self.exceptions_raised = set()

    def setUp(self):

        d = Path(self.pipeline_instance_dir)
        if d.exists():
            shutil.rmtree(d)

    def prepare_env_and_launch_task(self, state_file):

        self.assertFalse(state_file.is_completed())
        env_copy = os.environ.copy()
        try:
            os.environ["__script_location"] = os.path.join(
                state_file.tracker.pipeline_work_dir,
                state_file.task_key
            )
            launch_task(wait_for_completion=True, exit_process_when_done=False)
        finally:
            os.environ.clear()
            for k, v in env_copy.items():
                os.environ[k] = v
            self.assertEqual(os.environ.copy(), env_copy)

    def test_run(self):

        def e(state_file):
            self.prepare_env_and_launch_task(state_file)

        t = self.pipeline_test()
        t.test_run_pipeline(executer_func=e, pipeline_instance_dir=self.pipeline_instance_dir)

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

        def e(state_file):
            raise Exception("should not get called")

        t = self.pipeline_test()

        pi, tasks_by_keys = t.test_run_pipeline(
            executer_func=e,
            pipeline_instance_dir=self.pipeline_instance_dir,
            queue_only_pattern="*"
        )

        consume_and_produce_a_var = pi.lookup_single_task(
            "consume_and_produce_a_var",
            include_incomplete_tasks=True
        )

        self.assertRaises(
            UpstreamTasksNotCompleted,
            lambda: self.prepare_env_and_launch_task(consume_and_produce_a_var.state_file)
        )


    def pipeline_test(self) -> BasePipelineTest:
        class T(pipeline_tests_with_multiple_tasks.PipelineWithVariablePassing):
            def is_fail_test(self):
                return True
            def validate(self, tasks_by_keys):
                pass

        return T()


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
