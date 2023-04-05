import os
import shutil
import unittest
from pathlib import Path

from base_pipeline_test import BasePipelineTest
from dry_pipe.script_lib import launch_task, load_task_conf_dict
import pipeline_tests_with_single_tasks


class TaskLaunchTest(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        all_sandbox_dirs = os.path.join(
            os.path.dirname(__file__),
            "sandboxes"
        )

        self.pipeline_code_dir = os.path.dirname(__file__)
        self.pipeline_instance_dir = os.path.join(all_sandbox_dirs, self.__class__.__name__)
        self.pipeline_instance = None

    def setUp(self):

        d = Path(self.pipeline_instance_dir)
        if d.exists():
            shutil.rmtree(d)

        t = self.single_task_pipeline()
        self.pipeline_instance = t.create_pipeline_instance(self.pipeline_instance_dir)
        self.pipeline_instance.run_sync(queue_only_pattern="*")
        tasks = list(self.pipeline_instance.query("*", include_incomplete_tasks=True))
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        env_copy = os.environ.copy()
        new_vars = set()
        try:
            os.environ["__script_location"] = os.path.join(
                self.pipeline_instance.state_file_tracker.pipeline_work_dir,
                task.key
            )
            load_task_conf_dict()
            for k, v in os.environ.items():
                if k not in env_copy:
                    new_vars.add(k)
            self.launch_task(task.task_conf_json())
        finally:
            os.environ.clear()
            #for k in new_vars:
            #    del os.environ[k]
            for k, v in env_copy.items():
                os.environ[k] = v

            self.assertEqual(os.environ.copy(), env_copy)

    def launch_task(self, task_conf_json):
        launch_task(wait_for_completion=True, task_conf_dict=task_conf_json, exit_process_when_done=False)

    def single_task_pipeline(self) -> BasePipelineTest:
        raise NotImplementedError()

    def test_validate(self):
        t = list(self.pipeline_instance.query("*", include_incomplete_tasks=False))
        self.assertEqual(len(t), 1)
        task = t[0]
        self.assertIsNotNone(task)
        self.assertTrue(task.is_completed())


class BashTaskLauncherTest(TaskLaunchTest):

    def single_task_pipeline(self):
        return pipeline_tests_with_single_tasks.PipelineWithSingleBashTask()


class PythonTaskLauncherTest(TaskLaunchTest):

    def single_task_pipeline(self):
        return pipeline_tests_with_single_tasks.PipelineWithSinglePythonTask()

class BashTaskLauncherTestInContainer(TaskLaunchTest):

    def single_task_pipeline(self):
        return pipeline_tests_with_single_tasks.PipelineWithSingleBashTaskInContainer()


class PythonTaskLauncherTestInContainer(TaskLaunchTest):

    def single_task_pipeline(self):
        return pipeline_tests_with_single_tasks.PipelineWithSinglePythonTaskInContainer()


def all_launch_tests():
    return [
        BashTaskLauncherTest,
        PythonTaskLauncherTest,
        BashTaskLauncherTestInContainer,
        PythonTaskLauncherTestInContainer
    ]
