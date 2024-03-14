import glob
import os.path
import unittest
from pathlib import Path

from dry_pipe import DryPipe, TaskConf
from dry_pipe.cli import Cli
from dry_pipe.core_lib import UpstreamTasksNotCompleted, PortablePopen
from dry_pipe.pipeline import Pipeline
from dry_pipe.task_process import TaskProcess
from dry_pipe.slurm_array_task import SlurmArrayParentTask

from pipeline_tests_with_slurm_mockup import PipelineWithSlurmArrayForRealSlurmTest, PipelineWithSlurmArray, \
    PipelineWithSlurmArrayForRestarts
from test_utils import TestSandboxDir
from tests.base_pipeline_test import BasePipelineTest


def test_cli(*args):
    Cli(args).invoke(test_mode=True)

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

        test_cli(
            f'--pipeline-instance-dir={d.sandbox_dir}',
            'run',
            f'--generator=cli_tests:pipeline_with_slurm_array_1'
        )

        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)

        self.do_validate(pipeline_instance)


    def test_array_launch_one_complete_array(self):

        d = TestSandboxDir(self)

        pipeline_with_slurm_array_1_modfunc = 'cli_tests:pipeline_with_slurm_array_1'
        test_cli(
            f'--pipeline-instance-dir={d.sandbox_dir}',
            'prepare',
            f'--generator={pipeline_with_slurm_array_1_modfunc}'
        )

        pipeline_instance = Pipeline.load_from_module_func(
            pipeline_with_slurm_array_1_modfunc
        ).create_pipeline_instance(d.sandbox_dir)

        # ensure no task has been executed
        for task in pipeline_instance.query("*", include_incomplete_tasks=True):
            self.assertEqual(task.state_name(), 'state.ready')

        test_cli(
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            'submit-array',
            '--task-key=array-parent'
        )

        self.do_validate(pipeline_instance)

    def _get_job_files(self, pipeline_instance, array_task_key):
        p = os.path.join(
            pipeline_instance.state_file_tracker.pipeline_instance_dir, ".drypipe", array_task_key, "array.*.job.*")
        return list(glob.glob(p))

    def test_array_launch_one_task_in_array(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        test_cli(
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'submit-array',
            '--task-key', 'array-parent',
            '--limit', '1'
        )

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
            len(self._get_job_files(pipeline_instance, "array-parent")),
            2
        )

        self.do_validate(pipeline_instance)

    def test_array_launch_3_chunks(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        for _ in [1, 1, 1]:
            test_cli(
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                'submit-array',
                '--task-key', 'array-parent',
                '--limit=1'
            )

        self.assertEqual(
            len(self._get_job_files(pipeline_instance,"array-parent")),
            3
        )

        self.do_validate(pipeline_instance)


class CliTestsPipelineWithSlurmArrayForRestarts(PipelineWithSlurmArrayForRestarts):

    def create_prepare_and_run_pipeline(self, d, until_patterns=["*"]):
        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)
        pipeline_instance.run_sync(until_patterns)
        return pipeline_instance

    def test_array_restart(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        pid = pipeline_instance.state_file_tracker.pipeline_instance_dir
        test_cli(
            f'--pipeline-instance-dir={pid}',
            'submit-array',
            '--task-key=array_parent'
        )

        for task in pipeline_instance.query("t_*", include_incomplete_tasks=True):
            self.assertEqual("state.failed.1", task.state_name())

        Path(f"{pid}/output/t_1/ok").touch()

        test_cli(
            f'--pipeline-instance-dir={pid}',
            'restart-failed-array-tasks',
            '--task-key=array_parent',
            '--wait'
        )

        for task in pipeline_instance.query("t_1", include_incomplete_tasks=True):
            self.assertEqual("state.completed", task.state_name())
            self.assertEqual(1, int(task.outputs.r))
            self.assertEqual(1, int(task.outputs.r2))


        for task in pipeline_instance.query("t_2", include_incomplete_tasks=True):
            self.assertEqual("state.failed.1", task.state_name())

        Path(f"{pid}/output/t_2/ok").touch()

        test_cli(
            f'--pipeline-instance-dir={pid}',
            'restart-failed-array-tasks',
            '--task-key=array_parent',
            '--wait'
        )

        for task in pipeline_instance.query("t_2", include_incomplete_tasks=True):
            self.assertEqual("state.completed", task.state_name())
            self.assertEqual(4, int(task.outputs.r))
            self.assertEqual(2, int(task.outputs.r2))


class CliTestsPipelineWithSlurmArrayRemote(PipelineWithSlurmArray):

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

    def task_conf(self):

        repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

        tc = TaskConf(
            executer_type="slurm",
            slurm_account="def-xroucou",
            extra_env={
                "DRYPIPE_TASK_DEBUG": "True",
                "PYTHONPATH": ":".join([
                    f"{self.remote_base_dir()}/CliTestsPipelineWithSlurmArrayRemote.test_array_upload/.drypipe",
                    f"{self.remote_base_dir()}/CliTestsPipelineWithSlurmArrayRemote.test_array_upload/external-file-deps/{repo_dir}"
                ])
            }
        )
        tc.python_bin = None
        return tc

    def exec_remote(self, cmd):
        with PortablePopen(["ssh", self.user_at_host(), " ".join(cmd)]) as p:
            p.wait_and_raise_if_non_zero()

    def remote_base_dir(self):
        return "/home/maxl/tests-drypipe"

    def user_at_host(self):
        return "maxl@mp2.ccs.usherbrooke.ca"

    def test_array_upload(self):
        d = TestSandboxDir(self)

        pipeline_instance = self.create_prepare_and_run_pipeline(d)

        pid = pipeline_instance.state_file_tracker.pipeline_instance_dir

        self.exec_remote(["rm", "-Rf", self.remote_base_dir()])
        self.exec_remote(["mkdir", "-p", self.remote_base_dir()])

        ssh_dest = f"{self.user_at_host()}:{self.remote_base_dir()}"

        test_cli(
            '--pipeline-instance-dir', pid,
            'task',
            f'{pid}/.drypipe/z',
            '--wait'
        )

        test_cli(
            '--pipeline-instance-dir', pid,
            'upload-array',
            '--task-key=array_parent',
            f'--ssh-remote-dest={ssh_dest}'
        )

        test_cli(
            '--pipeline-instance-dir', pid,
            'task',
            f'{pid}/.drypipe/array_parent',
            '--wait',
            f'--ssh-remote-dest={ssh_dest}'
        )

        test_cli(
            '--pipeline-instance-dir', pid,
            'download-array',
            '--task-key=array_parent',
            f'--ssh-remote-dest={ssh_dest}'
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
            test_cli(
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                'create-array-parent',
                parent_task_key, match,
                '--slurm-account=dummy', '--force'
            )

        self.assertRaises(UpstreamTasksNotCompleted, lambda: create_parent_task('p1', 't_a_*'))

        test_cli(
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'task',
            f'{pipeline_instance.state_file_tracker.pipeline_instance_dir}/.drypipe/z',
            '--wait'
        )

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
            test_cli(
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                'task',
                f'{pipeline_instance.state_file_tracker.pipeline_instance_dir}/.drypipe/{parent_task_key}',
                '--wait'
            )

        run_parent_task('p1')

        for task in pipeline_instance.query("t_a_*", include_incomplete_tasks=True):
            self.assertEqual("state.completed", task.state_name())

        create_parent_task('p2', 't_*')

        self.assertEqual({k for k in keys_p1('p2')}, {'t_b_2', 't_b_1'})

        test_cli(
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            'submit-array',
            '--task-key=p2',
            '--limit=1'
        )

        test_cli(
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            'submit-array',
            '--task-key=p2',
            '--limit=1'
        )

        array_parent_task = SlurmArrayParentTask(TaskProcess(
            os.path.join(pipeline_instance.state_file_tracker.pipeline_work_dir, "p2")
        ))

        self.assertEqual(
            {task_key: state for task_key, state in array_parent_task.list_array_states()},
            {"t_b_1": "state.completed", "t_b_2": "state.completed"}
        )

        self.do_validate(pipeline_instance)


class CliTestScenario2(PipelineWithSlurmArray):

    def test_run_until(self):
        d = TestSandboxDir(self)

        test_cli(
            '--pipeline-instance-dir', d.sandbox_dir,
            'run',
            '--generator', 'cli_tests:pipeline_with_slurm_array_2',
            '--until', 't_a*'
        )
