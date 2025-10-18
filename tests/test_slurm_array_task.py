import os
import textwrap
import unittest
from pathlib import Path

from dry_pipe.slurm_arrays import SAcctParser, ArrayTaskManager
from dry_pipe import TaskConf

from tests.base_pipeline_test import TestWithDirectorySandbox, BasePipelineTest
from tests.test_utils import DummyLogger, TestSandboxDir


class TestParser:
    def invoke(self, job_id):
        return


class ArrayTaskScenario(TestWithDirectorySandbox):

    def logger_for_this_test(self):
        return DummyLogger()

    def array_task_key(self):
        return "array-task"

    def auto_restart_condition_regexp_per_log_file(self):
        return None

    def array_task_conf(self):
        return TaskConf(fields_from_json={
            "slurm_account": "zaz",
            "sbatch_options": ["--time=1:00"]
        })

    def sample_child_task_conf(self):
        return TaskConf(fields_from_json={
            "step_invocations": [{}]
        })

    def array_manager_for_this_test(self, pipeline_instance_dir):
        self.pipeline_instance_dir = pipeline_instance_dir
        s = self

        p = Path(self.task_control_dir(self.array_task_key()))
        p.mkdir(parents=True, exist_ok=True)

        with open(p.joinpath("task-keys.tsv"), "w") as f:
            for task_key in self.task_keys():
                f.write(f"{task_key}\n")


        class ArrayTaskManager4Tests(ArrayTaskManager):

            def __init__(self):
                super().__init__(None)

            def logger(self):
                return s.logger_for_this_test()

            def array_task_control_dir(self):
                return Path(pipeline_instance_dir, ".drypipe", s.array_task_key()).__str__()

            def auto_restart_condition_regexp_per_log_file(self):
                return s.auto_restart_condition_regexp_per_log_file()

            def array_task_conf(self):
                return s.array_task_conf()

            def sample_child_task_conf(self):
                return s.sample_child_task_conf()

            def is_debug(self):
                return True

        arm = ArrayTaskManager4Tests()

        for task_key in self.task_keys():
            d = Path(arm.pipeline_work_dir(), task_key)
            d.mkdir(parents=True, exist_ok=True)
            d.joinpath("state.ready").touch()


        return arm


    def task_control_dir(self, task_key):
        return Path(self.pipeline_instance_dir, ".drypipe", task_key).__str__()


    def task_keys(self):
        raise Exception("task_keys not implemented")

    def submits(self):
        raise Exception("submits not implemented")




class ArrayTaskScenario1(ArrayTaskScenario):

    def task_keys(self):
        return ["t_1", "t_2", "t_3"]

    def task_conf_parent(self):
        return TaskConf(fields_from_json={
            "slurm_account": "zaz",
            "sbatch_options": ["--time=1:00"]
        })

    def task_conf_child(self):
        return TaskConf(fields_from_json={
            "step_invocations": [{}]
        })


    def test_1(self):
        d = TestSandboxDir(self)


        atm = self.array_manager_for_this_test(d.sandbox_dir)

        self.assertSetEqual(
            atm.task_keys_for_next_batch(),
            {"t_1", "t_2", "t_3"}
        )

        submit = list(atm.next_submits())[0]

        self.assertEqual(submit.sbatch_command[1],"--array=0-2")

        submit.pre_submit_func()
        self.assertSetEqual(
            atm.children_task_keys(),
            {"t_1", "t_2", "t_3"}
        )
        # un submit...
        os.remove(list(atm.array_files_sequence.list_files())[0][1])

        submit = list(atm.next_submits())[0]

        self.assertSetEqual(
            atm.children_task_keys(),
            submit.task_keys
        )

        submit.invoke("51027286")

        # --format="JobId,State,JobName,ExitCode"

        atm.invoke_sacct(fake_sacct_outputs={
            "51027286": "51027286_[0-2]|PENDING|cli|0:0|"
        })


        def assert_empty_next_submit():
            self.assertEqual(list(atm.next_submits()), [])


        assert_empty_next_submit()

        self.assertEqual(
            list(atm.task_keys_for_next_batch()),
            []
        )

        expected = {'t_1': 'PENDING', 't_2': 'PENDING', 't_3': 'PENDING'}

        self.assertEqual(
            atm.last_sact_state_code_by_task_keys(),
            expected
        )

        atm.invoke_sacct(fake_sacct_outputs={
            "51027286":
                """51027286_0|RUNNING|t_1:step-started.0|0:0|
                   51027286_1|RUNNING|t_2:step-started.1|0:0|
                   51027286_2|RUNNING|t_3:step-started.1|0:0|"""
        })

        expected = {'t_1': 'RUNNING', 't_2': 'RUNNING', 't_3': 'RUNNING'}

        assert_empty_next_submit()

        self.assertEqual(
            atm.last_sact_state_code_by_task_keys(),
            expected
        )

        self.assertEqual(
            atm.last_sact_state_code_by_task_keys(),
            expected
        )

        atm.invoke_sacct(fake_sacct_outputs={
            "51027286":
                """51027286_0|COMPLETED|t_1:completed|0:0|
                   51027286_1|COMPLETED|t_2:completed|0:0|
                   51027286_2|COMPLETED|t_3:completed|0:0|"""
        })

        assert_empty_next_submit()

        expected = {'t_1': 'COMPLETED', 't_2': 'COMPLETED', 't_3': 'COMPLETED'}

        self.assertEqual(
            atm.last_sact_state_code_by_task_keys(),
            expected
        )


    def test_2(self):
        d = TestSandboxDir(self)


        atm = self.array_manager_for_this_test(d.sandbox_dir)

        self.assertSetEqual(
            atm.task_keys_for_next_batch(),
            {"t_1", "t_2", "t_3"},
        )

        submit = list(atm.next_submits())[0]

        self.assertSetEqual(
            atm.children_task_keys(),
            submit.task_keys
        )

        submit.invoke("51027286")

        fake_sacct_outputs = {
            # --format="JobId,State,JobName,ExitCode"
            "51027286": "51027286_[0-2]|CANCELLED|cli|0:0|"
        }

        atm.invoke_sacct(fake_sacct_outputs)

        submit = list(atm.next_submits())[0]

        self.assertSetEqual(
            atm.task_keys_for_next_batch(),
            {"t_1", "t_2", "t_3"},
        )


        submit.invoke("51027287")

        fake_sacct_outputs["51027287"] = \
            """51027287_0|RUNNING|t_1:step-started.0|0:0|
               51027287_1|FAILED|t_2:failed.1|0:0|
               51027287_2|COMPLETED|t_3:completed|0:0|"""

        atm.invoke_sacct(fake_sacct_outputs)

        self.assertSetEqual(atm.task_keys_for_next_batch(), set([]),)
