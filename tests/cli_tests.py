import glob
import os.path
import unittest

from dry_pipe import DryPipe, TaskConf
from dry_pipe.cli import Cli
from dry_pipe.core_lib import UpstreamTasksNotCompleted, PortablePopen
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

    def test_run_pipeline(self):
        pass

    def test_custom_array_parents(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        def create_parent_task(parent_task_key, match):
            Cli([
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                'create-array-parent',
                parent_task_key, match,
                '--slurm-account=dummy', '--force'
            ]).invoke(test_mode=True)

        self.assertRaises(UpstreamTasksNotCompleted, lambda: create_parent_task('p1', 't_a_*'))

        Cli([
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'task',
            f'{pipeline_instance.state_file_tracker.pipeline_instance_dir}/.drypipe/z',
            '--wait'
        ]).invoke(test_mode=True)

        create_parent_task('p1', 't_a_*')

        def keys_p1(parent_task_key):
            kf = os.path.join(pipeline_instance.state_file_tracker.pipeline_work_dir, parent_task_key, "task-keys.tsv")
            with open(kf) as f:
                for l in f.readlines():
                    l = l.strip()
                    if l != "":
                        yield l.strip()

        self.assertEqual({k for k in keys_p1('p1')}, {'t_a_2', 't_a_1'})

        def run_parent_task(parent_task_key):
            Cli([
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                'task',
                f'{pipeline_instance.state_file_tracker.pipeline_instance_dir}/.drypipe/{parent_task_key}',
                '--wait'
            ]).invoke(test_mode=True)

        run_parent_task('p1')

        create_parent_task('p2', 't_*')

        self.assertEqual({k for k in keys_p1('p2')}, {'t_b_2', 't_b_1'})

        Cli([
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            'submit-array',
            '--task-key=p2',
            '--limit=1'
        ]).invoke(test_mode=True)

        Cli([
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            'submit-array',
            '--task-key=p2',
            '--limit=1'
        ]).invoke(test_mode=True)

        self.do_validate(pipeline_instance)

    def task_conf(self):
        return TaskConf(executer_type="slurm", slurm_account="def-xroucou", extra_env={"DRYPIPE_TASK_DEBUG": "True"})


    def exec_remote(self, user_at_host, cmd):

        with PortablePopen(["ssh", user_at_host, " ".join(cmd)]) as p:
            p.wait_and_raise_if_non_zero()

    def test_array_upload(self):
        d = TestSandboxDir(self)

        pipeline_instance = self.create_prepare_and_run_pipeline(d)

        remote_base_dir = "/home/maxl/tests-drypipe"
        ssh_dest = f"maxl@mp2.ccs.usherbrooke.ca:{remote_base_dir}"

        user_at_host, root_dir = ssh_dest.split(":")

        self.exec_remote(user_at_host, ["rm", "-Rf", root_dir])
        self.exec_remote(user_at_host, ["mkdir", "-p", root_dir])

        pid = pipeline_instance.state_file_tracker.pipeline_instance_dir

        Cli([
            '--pipeline-instance-dir', pid,
            'task',
            f'{pid}/.drypipe/z',
            '--wait'
        ]).invoke(test_mode=True)

        Cli([
            '--pipeline-instance-dir', pid,
            'upload-array',
            '--task-key=array_parent',
            f'--ssh-remote-dest={ssh_dest}'
        ]).invoke(test_mode=True)

        #remote_pid = f"{remote_base_dir}/{os.path.basename(pid)}"
        
        Cli([
            '--pipeline-instance-dir', pid,
            'task',
            f'{pid}/.drypipe/array_parent',
            '--wait',
            f'--ssh-remote-dest={ssh_dest}'
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
