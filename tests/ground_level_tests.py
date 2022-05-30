import glob
import os
import pathlib
import shutil
import unittest

from dry_pipe.actions import TaskAction
from dry_pipe.task_state import VALID_TRANSITIONS
from test_00_ground_level_tests.pipeline_defs import pipeline_exerciser, single_task_pipeline, \
    single_task_pipeline_with_inline_script
from test_utils import TestSandboxDir


class GroundLevelTests(unittest.TestCase):

    def test_task_action_sanity(self):

        restart_0 = TaskAction.load_from_file("/action.restart")

        self.assertEqual("restart", restart_0.action_name)
        self.assertEqual(0, restart_0.step)

        restart_3 = TaskAction.load_from_file("/action.restart-3")

        self.assertEqual("restart", restart_3.action_name)
        self.assertEqual(3, restart_3.step)
        self.assertTrue(restart_3.is_restart())

        pause = TaskAction.load_from_file("/x/y/action.pause")

        self.assertEqual("pause", pause.action_name)
        self.assertIsNone(pause.step)
        self.assertTrue(pause.is_pause())

        shutil.rmtree("/tmp/.drypipe", ignore_errors=True)

        for control_dir in ["z0", "z2"]:
            pathlib.Path(os.path.join("/tmp", ".drypipe", control_dir)).mkdir(parents=True)

        TaskAction.submit("/tmp", "z0", "restart", 0)

        r0 = TaskAction.load_from_task_control_dir(os.path.join("/tmp", ".drypipe", "z0"))

        self.assertIsNotNone(r0)
        self.assertEqual(0, r0.step)

        self.assertRaises(Exception, lambda: TaskAction.submit("/tmp", "z0", "restart", 0))
        self.assertRaises(Exception, lambda: TaskAction.submit("/tmp", "z0", "restart", 2))

        TaskAction.submit("/tmp", "z2", "restart", 2)

        r2 = TaskAction.load_from_task_control_dir(os.path.join("/tmp", ".drypipe", "z2"))

        self.assertIsNotNone(r2)
        self.assertEqual(2, r2.step)


    def test_janitor_state_machine_sanity(self):
        for state, transitions in VALID_TRANSITIONS.items():
            for next_state in transitions:
                if next_state not in VALID_TRANSITIONS:
                    raise Exception(
                        f"State machine VALID_TRANSITIONS has unknown transition {next_state}, from {state}"
                    )

    def test_transitions_of_tasks_without_failures_00(self):

        d = TestSandboxDir(self)

        pi = d.pipeline_instance_from_generator(
            single_task_pipeline,
            completed=True
        )

        self._validate_transitions_of_tasks_without_failures_01(pi)

    def test_transitions_of_tasks_without_failures_01(self):

        d = TestSandboxDir(self)

        pi = d.pipeline_instance_from_generator(
            single_task_pipeline_with_inline_script,
            completed=True
        )

        self._validate_transitions_of_tasks_without_failures_01(pi)

    def _validate_transitions_of_tasks_without_failures_01(self, pipeline):

        unique_task = list(pipeline.tasks)[0]

        state = unique_task.get_state()
        self.assertTrue(state.is_completed(), msg=f"expected completed, got {state}")

        out_vars = dict(unique_task.iterate_out_vars())

        self.assertEqual({"huge_variable": "123"}, out_vars)

        self.assertTrue(os.path.exists(
            unique_task.abs_path_of_produced_file("inflated-dummy.fasta")
        ))

    def test_transitions_of_tasks_with_failures_02_no_snippet(self):

        pipeline_instance = TestSandboxDir(self).pipeline_instance_from_generator(
            single_task_pipeline,
            env_vars={"PLEASE_CRASH": "single-task"},
            completed=True,
            fail_silently=True
        )

        self.assertTrue(pipeline_instance.tasks["single-task"].get_state().is_failed())

    def test_transitions_of_tasks_with_failures_02_with_snippet(self):

        pipeline_instance = TestSandboxDir(self).pipeline_instance_from_generator(
            single_task_pipeline_with_inline_script,
            env_vars={"PLEASE_CRASH": "single-task"},
            completed=True,
            fail_silently=True
        )

        self.assertTrue(pipeline_instance.tasks["single-task"].get_state().is_failed())


class TaskSignatureTests(unittest.TestCase):

    def _test_03(self):

        pipeline, sig_loader, inputs_are_stale_loader = pipeline_exerciser(1, 2, 3, 4, "p03")

        pipeline.run_sync()

        expected_signatures_1_2_3_4 = [
            '0f1f6df99896b5827f853e4ed25b7cb2a397d222', '222f805f51037a734dd95de732b20cb16fa3c7fc',
            'fddcefdc738f1134eb15a93ecb61b5045efbc04a', '213f3ee459c0b32741f89ee85a1dde1f7d590815'
        ]

        self.assertEqual(expected_signatures_1_2_3_4, sig_loader())

        self.assertEqual(
            [False, False],
            inputs_are_stale_loader()
        )

        pipeline, sig_loader, inputs_are_stale_loader = pipeline_exerciser(1, 12, 3, 4, "p03", skip_reset=True)

        pipeline.recompute_signatures()

        self.assertEqual(expected_signatures_1_2_3_4, sig_loader())

#        self.assertEqual(
#            [True, False],
#            inputs_are_stale_loader()
#        )

        pipeline, sig_loader, inputs_are_stale_loader = pipeline_exerciser(1, 2, 3, 4, "p03", skip_reset=True)

        pipeline.recompute_signatures()

        self.assertEqual(
            [False, False],
            inputs_are_stale_loader()
        )

        self.assertEqual(expected_signatures_1_2_3_4, sig_loader())

        pipeline, sig_loader, inputs_are_stale_loader = pipeline_exerciser(1, 2, 13, 4, "p03", skip_reset=True)

        pipeline.recompute_signatures()

        self.assertEqual(
            [False, True],
            inputs_are_stale_loader()
        )

        self.assertEqual(expected_signatures_1_2_3_4, sig_loader())
