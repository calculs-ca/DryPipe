import unittest

from dry_pipe.core_lib import Cli
from pipeline_tests_with_single_tasks import PipelineWithSinglePythonTask
from pipeline_tests_with_slurm import PipelineWithSlurmArrayForRealSlurmTest
from test_utils import TestSandboxDir


class CliArrayTests1(PipelineWithSlurmArrayForRealSlurmTest):

    def setUp(self):
        pass

    def create_and_prepare_pipeline(self, d):
        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)
        pipeline_instance.run_sync(queue_only_pattern="*")
        return pipeline_instance

    def do_validate(self, pipeline_instance):
        self.validate({
            task.key: task
            for task in pipeline_instance.query("*")
        })

    def test_array_launch_one_chunk(self):

        pipeline_instance = self.create_and_prepare_pipeline(TestSandboxDir(self))

        Cli([
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'submit-array',
            '--task-key', 'array-parent'
        ]).invoke(test_mode=True)

        self.do_validate(pipeline_instance)

    def test_array_launch_2_chunks(self):
        pipeline_instance = self.create_and_prepare_pipeline(TestSandboxDir(self))

        Cli([
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'submit-array',
            '--task-key', 'array-parent',
            '--limit', '1'
        ]).invoke(test_mode=True)

        Cli([
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'submit-array',
            '--task-key', 'array-parent'
        ]).invoke(test_mode=True)

        self.do_validate(pipeline_instance)

    def test_array_launch_3_chunks(self):
        pipeline_instance = self.create_and_prepare_pipeline(TestSandboxDir(self))

        for _ in [1, 2, 3]:
            Cli([
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                'submit-array',
                '--task-key', 'array-parent',
                '--limit', '1'
            ]).invoke(test_mode=True)

        self.do_validate(pipeline_instance)
