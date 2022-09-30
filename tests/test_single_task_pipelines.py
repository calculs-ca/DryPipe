import os
import unittest

import pipeline_with_single_bash_task
import pipeline_with_single_python_task
import pipeline_with_variable_passing
from dry_pipe import TaskConf
from dry_pipe.script_lib import env_from_sourcing
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


class MinimalistPipelinesTests(unittest.TestCase):

    def test_variable_passing(self):
        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_variable_passing.pipeline,
            completed=True
        )

        consume_and_produce_a_var = pipeline_instance.tasks["consume_and_produce_a_var"]

        task_env_script = os.path.join(
            pipeline_instance.work_dir,
            "consume_and_produce_a_var",
            "task-env.sh"
        )

        env = env_from_sourcing(task_env_script)

        v = int(env.get("v"))
        self.assertEqual(v, 1234)

        self.assertEqual(consume_and_produce_a_var.out.result.fetch(), 2468)
