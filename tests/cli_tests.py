import glob
import os.path
import unittest

from dry_pipe import DryPipe
from dry_pipe.core_lib import Cli, UpstreamTasksNotCompleted
from pipeline_tests_with_single_tasks import PipelineWithSinglePythonTask
from pipeline_tests_with_slurm_mockup import PipelineWithSlurmArrayForRealSlurmTest, PipelineWithSlurmArray
from test_utils import TestSandboxDir

def pipeline_with_slurm_array_1():
    t = PipelineWithSlurmArrayForRealSlurmTest()
    return DryPipe.create_pipeline(
        lambda dsl: t.dag_gen(dsl),
        pipeline_code_dir=t.pipeline_code_dir
    )

def pipeline_with_slurm_array_2():
    t = PipelineWithSlurmArray()
    return DryPipe.create_pipeline(
        lambda dsl: t.dag_gen(dsl),
        pipeline_code_dir=t.pipeline_code_dir
    )


class CliArrayTests1(PipelineWithSlurmArrayForRealSlurmTest):

    def setUp(self):
        pass

    def create_prepare_and_run_pipeline(self, d, until_patterns=["*"]):
        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)
        pipeline_instance.run_sync(until_patterns)
        return pipeline_instance


    def do_validate(self, pipeline_instance):
        self.validate({
            task.key: task
            for task in pipeline_instance.query("*")
        })

    def test_complete_run(self):
        d = TestSandboxDir(self)

        Cli([
            f'--pipeline-instance-dir={d.sandbox_dir}',
            'run',
            f'--generator=cli_tests:pipeline_with_slurm_array_1'
        ]).invoke(test_mode=True)

        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)

        self.do_validate(pipeline_instance)


    def test_array_launch_one_complete_array(self):

        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        Cli([
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            'submit-array',
            '--task-key=array-parent'
        ]).invoke(test_mode=True)

        self.do_validate(pipeline_instance)

    def _get_job_files(self, pipeline_instance, array_task_key):
        p = os.path.join(
            pipeline_instance.state_file_tracker.pipeline_instance_dir, ".drypipe", array_task_key, "array.*.job.*")
        return list(glob.glob(p))

    def test_array_launch_one_task_in_array(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        Cli([
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'submit-array',
            '--task-key', 'array-parent',
            '--limit', '1'
        ]).invoke(test_mode=True)

        self.assertEqual(
            len(self._get_job_files(pipeline_instance,"array-parent")),
            1
        )

        Cli([
            'submit-array',
            '--task-key', 'array-parent'
        ], env={
            "DRYPIPE_PIPELINE_INSTANCE_DIR": pipeline_instance.state_file_tracker.pipeline_instance_dir
        }).invoke(test_mode=True)

        self.assertEqual(
            len(self._get_job_files(pipeline_instance,"array-parent")),
            2
        )

        self.do_validate(pipeline_instance)

    def test_array_launch_3_chunks(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        for _ in [1, 1, 1]:
            Cli([
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                'submit-array',
                '--task-key', 'array-parent',
                '--limit=1'
            ]).invoke(test_mode=True)

        self.assertEqual(
            len(self._get_job_files(pipeline_instance,"array-parent")),
            3
        )

        self.do_validate(pipeline_instance)


class CliTestsPipelineWithSlurmArray(PipelineWithSlurmArray):

    def create_prepare_and_run_pipeline(self, d, until_patterns=["*"]):
        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)
        pipeline_instance.run_sync(until_patterns)
        return pipeline_instance


    def do_validate(self, pipeline_instance):
        self.validate({
            task.key: task
            for task in pipeline_instance.query("*")
        })

    def test_custom_array_parents(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        def go():
            Cli([
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                'create-array-parent',
                'p1', 't_a_*'
            ]).invoke(test_mode=True)

        self.assertRaises(UpstreamTasksNotCompleted, go)


        Cli([
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'run',
            '--generator', 'cli_tests:pipeline_with_slurm_array_2',
            '--task-key=z'
        ]).invoke(test_mode=True)

        go()

        def keys_p1():
            with open(os.path.join(pipeline_instance.state_file_tracker.pipeline_work_dir, "p1", "task-keys.tsv")) as f:
                for l in f.readlines():
                    l = l.strip()
                    if l != "":
                        yield l.strip()

        self.assertEqual({k for k in keys_p1()}, {'t_a_2', 't_a_1'})


        Cli([
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            'submit-array',
            '--task-key=p1'
        ]).invoke(test_mode=True)



class CliTestScenario2(PipelineWithSlurmArray):

    def test_run_until(self):
        d = TestSandboxDir(self)

        Cli([
            '--pipeline-instance-dir', d.sandbox_dir,
            'run',
            '--generator', 'cli_tests:pipeline_with_slurm_array_2',
            '--until', 't_a*'
        ]).invoke(test_mode=True)
