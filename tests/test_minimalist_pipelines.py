import json
import os
import unittest

import pipeline_with_file_and_var_output
import pipeline_with_kwargs_consuming_task
import pipeline_with_single_bash_task
import pipeline_with_single_python_task
import pipeline_with_variable_passing
import pipeline_with_two_python_tasks
import pipeline_with_multistep_tasks_with_shared_vars

from dry_pipe import TaskConf, DryPipe
from dry_pipe.script_lib import iterate_task_env
from pipeline_with_dependency_on_other_pipeline import pipeline_with_external_deps_dag_gen
from test_utils import TestSandboxDir


class SingleTaskPipelinesTests(unittest.TestCase):

    def test_single_python_task_pipeline(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_single_python_task.pipeline,
            completed=True
        )

        pipeline_with_single_python_task.validate_single_task_pipeline(pipeline_instance)

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

        pipeline_with_single_python_task.validate_single_task_pipeline(pipeline_instance)

    def test_single_bash_task_pipeline(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_single_bash_task.pipeline,
            completed=True
        )

        pipeline_with_single_python_task.validate_single_task_pipeline(pipeline_instance)

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

        pipeline_with_single_python_task.validate_single_task_pipeline(pipeline_instance)


class MinimalistPipelinesTests(unittest.TestCase):

    def test_variable_passing(self):
        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_variable_passing.pipeline,
            completed=True
        )

        consume_and_produce_a_var = pipeline_instance.tasks["consume_and_produce_a_var"]

        control_dir = os.path.join(
            pipeline_instance.work_dir,
            "consume_and_produce_a_var",
        )

        with open(os.path.join(control_dir, "task-conf.json")) as tc:
            task_conf = json.loads(tc.read())
            env = dict(iterate_task_env(task_conf, control_dir))

            v = env.get("v")
            if v is None:
                raise Exception(f"variable 'v' was not produced by task 'consume_and_produce_a_var'")
            v = int(v)
            self.assertEqual(v, 1234)

            self.assertEqual(consume_and_produce_a_var.out.result.fetch(), 2468)

    def test_consume_var_local_in_upstream_name(self):
        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_two_python_tasks.pipeline,
            completed=True
        )

        pipeline_with_two_python_tasks.validate_two_task_pipeline(pipeline_instance)

    def test_variable_sharing_between_task_calls(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_multistep_tasks_with_shared_vars.dag_generator,
            completed=True
        )

        pipeline_with_multistep_tasks_with_shared_vars.validate(self, pipeline_instance)

    def test_ultra_minimalist_pipeline(self):
        d = TestSandboxDir(self)
        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_file_and_var_output.dag_gen,
            completed=True
        )
        pipeline_with_file_and_var_output.validate(self, pipeline_instance.tasks["t1"])

    def test_inter_pipeline_dependencies(self):

        d_dep1 = TestSandboxDir(self, "EXT_test_consume_var_local_in_upstream_name")
        d_dep1.delete_and_recreate_sandbox()
        d_dep1.pipeline_instance_from_generator(
            pipeline_with_two_python_tasks.pipeline,
            completed=True
        )

        d_dep2 = TestSandboxDir(self, "EXT_test_pipeline_with_file_and_var_output")
        d_dep2.delete_and_recreate_sandbox()
        d_dep2.pipeline_instance_from_generator(
            pipeline_with_file_and_var_output.dag_gen,
            completed=True
        )

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            lambda dsl: pipeline_with_external_deps_dag_gen(
                dsl,
                d_dep1.sandbox_dir,
                d_dep2.sandbox_dir
            ),
            completed=True
        )

        pipeline_with_two_python_tasks.validate_two_task_pipeline(pipeline_instance)
        pipeline_with_file_and_var_output.validate(self, pipeline_instance.tasks["t2"])

    def test_kwargs_consuming_task(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_kwargs_consuming_task.gen_dag,
            completed=True
        )

        pipeline_with_kwargs_consuming_task.validate(self, pipeline_instance)


    def test_refer_to_task_constant_inputs(self):

        d = TestSandboxDir(self)

        d.pipeline_instance_from_generator(
            pipeline_with_single_bash_task.pipeline,
            completed=True
        )

        pipeline_instance = DryPipe.load_pipeline(d.sandbox_dir)

        multiply_x_by_y = pipeline_instance.query("multiply_x_by_y").single()

        self.assertEqual(3, multiply_x_by_y.inputs.x)

    def test_refer_to_task_upstream_inputs(self):
        d = TestSandboxDir(self)

        pipeline_instance_1 = d.pipeline_instance_from_generator(
            pipeline_with_two_python_tasks.pipeline,
            completed=True
        )

        def validate(task_t2):
            self.assertEqual(task_t2.inputs.x, 12)
            self.assertEqual(task_t2.inputs.z, 34)

        # validate "original" instance
        validate(pipeline_instance_1.tasks["t2"])

        # validate loaded instance
        validate(DryPipe.load_pipeline(d.sandbox_dir).query("t2").single())



