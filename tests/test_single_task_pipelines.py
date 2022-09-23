import unittest

import pipeline_with_single_bash_task
import pipeline_with_single_python_task
from dry_pipe import TaskConf
from test_utils import TestSandboxDir


class SingleTaskPipelinesTests(unittest.TestCase):

    def _validate(self, pipeline_instance):
        multiply_x_by_y_task = pipeline_instance.tasks["multiply_x_by_y"]

        self.assertEqual(multiply_x_by_y_task.out.result.fetch(), 15)

    def test_single_python_task_pipeline(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_single_python_task.pipeline,
            completed=True
        )

        self._validate(pipeline_instance)

    def test_single_python_task_pipeline_with_container(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_single_python_task.pipeline,
            task_conf=TaskConf(
                executer_type="process",
                container="singularity-test-container.sif"
            ),
            completed=True
        )

        self._validate(pipeline_instance)

    def test_single_bash_task_pipeline(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_single_bash_task.pipeline,
            completed=True
        )

        self._validate(pipeline_instance)

    def test_single_bash_task_pipeline_with_container(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_single_bash_task.pipeline,
            task_conf=TaskConf(
                executer_type="process",
                container="singularity-test-container.sif"
            ),
            completed=True
        )

        self._validate(pipeline_instance)
