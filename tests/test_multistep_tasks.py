import unittest

import os
import test_helpers


from tests.pipeline_with_multistep_tasks import three_steps_pipeline, hybrid_bash_python_mutlistep_pipeline
from test_utils import TestSandboxDir


class MultipstepTaskTests(unittest.TestCase):

    def test_normal_execution(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            three_steps_pipeline,
            completed=True
        )

        three_phase_task = next(pipeline_instance.tasks_for_key_prefix("three_phase_task"))

        three_step_task_out_file = os.path.join(three_phase_task.v_abs_work_dir(), "out_file.txt")

        def load_output_from_three_phase_task():
            return test_helpers.load_file_as_string(three_step_task_out_file)

        def validate_output_of_three_phase_task():
            out = load_output_from_three_phase_task()
            self.assertEqual("s1\ns2\ns3\n", out)

        validate_output_of_three_phase_task()

        pipeline_instance.clean_all()

        from dry_pipe.internals import Local
        try:
            Local.fail_silently_for_test = True

            pipeline_instance.run_sync(tmp_env_vars={
                "CRASH_STEP_2": "true"
            })

            self.assertEqual("s1\n", load_output_from_three_phase_task())
            self.assertTrue(three_phase_task.get_state().is_failed())

            test_helpers.rewrite_file(three_step_task_out_file, "")

            three_phase_task.re_queue()

            self.assertFalse(three_phase_task.get_state().is_failed())

            pipeline_instance.run_sync(tmp_env_vars={
                "CRASH_STEP_3": "true"
            })

            # ensure s1 is not there, i.e. step1 has NOT executed
            self.assertEqual("s2\n", load_output_from_three_phase_task())

            three_phase_task.re_queue()

            pipeline_instance.run_sync()

            self.assertEqual("s2\ns3\n", load_output_from_three_phase_task())

            """
            event_lines = list(three_phase_task.parse_tracking_file())
    
            step_lines = [
                int(l.split("_")[1])
                for l in event_lines if l.startswith("step_")
            ]
    
            self.assertEqual(step_lines, [0, 0, 1, 1, 2, 2])
    
            event_lines = [
                l.split("=")[0]
                for l in event_lines
            ]
    
            self.assertTrue("launched_at" in event_lines)
            self.assertTrue("completed_at" in event_lines)
            """
        finally:
            Local.fail_silently_for_test = False

    def test_pipeline_with_mixed_python_bash(self):

        pipeline = TestSandboxDir(self).pipeline_instance_from_generator(
            hybrid_bash_python_mutlistep_pipeline,
            completed=True
        )

        three_phase_task = next(pipeline.tasks_for_key_prefix("three_phase_task"))

        three_phase_task_out_file = os.path.join(three_phase_task.v_abs_work_dir(), "out_file.txt")

        self.assertEqual("s1\ns2\ns3\ns4\n", test_helpers.load_file_as_string(three_phase_task_out_file))
