import unittest

from dry_pipe import TaskConf
from pipeline_with_never_ending_task import never_ending_pipeline
from pipelines_with_bash_corner_cases import dag_gen_for_set_e_test
from test_utils import TestSandboxDir


class CornerCasesFailureTests(unittest.TestCase):

    def test_slurm_timeout_signal_handling(self):

        pipeline = TestSandboxDir(self).pipeline_instance_from_generator(
            never_ending_pipeline(TaskConf(
                executer_type="slurm",
                slurm_account="def-rodrigu1",
                sbatch_options=[
                    "--time=0:1:00"
                ]
            ))
        )

        single_task, = pipeline.tasks

        pipeline.clean()

        pipeline.run_sync()

        self.assertTrue(single_task.get_state().is_timed_out())

    def test_bash_set_e(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            dag_gen_for_set_e_test,
            completed=True
        )

        self.assertIsNone(pipeline_instance.tasks["t1"].out.r.fetch())



