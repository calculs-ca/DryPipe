import glob
import os.path
from pathlib import Path

from dry_pipe import DryPipe
from dry_pipe.cli import Cli
from dry_pipe.core_lib import UpstreamTasksNotCompleted
from dry_pipe.pipeline import Pipeline
from dry_pipe.task_process import TaskProcess

from dry_pipe_tests.pipeline_tests_with_slurm_mockup import PipelineWithSlurmArray
from dry_pipe.slurm_arrays import ArrayTaskManager
from dry_pipe_tests.test_utils import TestSandboxDir
from dry_pipe_tests.pipeline_tests_with_slurm_arrays import PipelineWithSlurmArrayForRealSlurmTest, \
    PipelineWithSlurmArrayForRestarts


def test_cli(*args, **kwargs):
    test = args[0]

    env = {
        "DRYPIPE_TASK_DEBUG": test.is_log_level_debug().__str__(),
        "DRYPIPE_INSTANCE_DEBUG": test.is_log_level_debug().__str__(),
        "DRYPIPE_SERVICE_SLEEP_SCHEDULE": test.custom_sleep_schedule()
    }

    if "env" in kwargs:
        env.update(kwargs["env"])

    args = args[1:]

    Cli(args, env=env, test_mode=True).invoke()

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
        pipeline_instance.run_sync(until_patterns, sleep_schedule=self.custom_sleep_schedule_parsed())
        return pipeline_instance


    def do_validate(self, pipeline_instance):
        self.validate({
            task.key: task
            for task in pipeline_instance.query("*")
        })

    def test_complete_run(self):
        d = TestSandboxDir(self)

        test_cli(
            self,
            'run',
            f'--pipeline-instance-dir={d.sandbox_dir}',
            f'--generator=dry_pipe_tests.cli_tests:pipeline_with_slurm_array_1'
        )

        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)

        self.do_validate(pipeline_instance)


    def test_array_launch_one_complete_array(self):

        d = TestSandboxDir(self)

        pipeline_with_slurm_array_1_modfunc = 'dry_pipe_tests.cli_tests:pipeline_with_slurm_array_1'
        test_cli(
            self,
            'prepare',
            f'--pipeline-instance-dir={d.sandbox_dir}',
            f'--generator={pipeline_with_slurm_array_1_modfunc}'
        )

        pipeline_instance = Pipeline.load_from_module_func(
            pipeline_with_slurm_array_1_modfunc
        ).create_pipeline_instance(d.sandbox_dir, instance_log_level=self.instance_log_level())

        # ensure no task has been executed
        for k, task in pipeline_instance.query_all_tasks_by_key().items():
            self.assertEqual(task.state_name(), 'state.ready')

        test_cli(
            self,
            'array-submit',
            f'-pid={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            '-k=array-parent'
        )

        self.do_validate(pipeline_instance)

    def _get_job_files(self, pipeline_instance, array_task_key):
        p = os.path.join(
            pipeline_instance.state_file_tracker.pipeline_instance_dir, ".drypipe", array_task_key, "array.*.job.*")
        return list(glob.glob(p))

    def _test_array_launch_one_task_in_array(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        test_cli(
            self,
            'array-submit',
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            '--task-key', 'array-parent',
            '--limit', '1'
        )

        self.assertEqual(
            len(self._get_job_files(pipeline_instance,"array-parent")),
            1
        )

        Cli([
            'array-submit',
            '--task-key', 'array-parent'
        ], env={
            "DRYPIPE_PIPELINE_INSTANCE_DIR": pipeline_instance.state_file_tracker.pipeline_instance_dir
        }).invoke(test_mode=True)

        self.assertEqual(
            len(self._get_job_files(pipeline_instance, "array-parent")),
            2
        )

        self.do_validate(pipeline_instance)

    def _test_array_launch_3_chunks(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        for _ in [1, 1, 1]:
            test_cli(
                self,
                'array-submit',
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
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
        pipeline_instance.run_sync(until_patterns, sleep_schedule=self.custom_sleep_schedule_parsed())
        return pipeline_instance

    def test_array_restart(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        pid = pipeline_instance.state_file_tracker.pipeline_instance_dir
        test_cli(
            self,
            'array-submit',
            f'--pipeline-instance-dir={pid}',
            '--task-key=array_parent'
        )

        for task in pipeline_instance.query("t_*", include_incomplete_tasks=True):
            self.assertEqual("state.failed.1", task.state_name())

        Path(f"{pid}/output/t_1/ok").touch()

        test_cli(
            self,
            'restart-failed-array-tasks',
            f'--pipeline-instance-dir={pid}',
            '--task-key=array_parent',
            '--include-pre-launch',
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
            self,
            'restart-failed-array-tasks',
            f'--pipeline-instance-dir={pid}',
            '--task-key=array_parent',
            '--wait'
        )

        for task in pipeline_instance.query("t_2", include_incomplete_tasks=True):
            self.assertEqual("state.completed", task.state_name())
            self.assertEqual(4, int(task.outputs.r))
            self.assertEqual(2, int(task.outputs.r2))




class CliTestsPipelineWithSlurmArray(PipelineWithSlurmArray):

    def create_prepare_and_run_pipeline(self, d, until_patterns=["*"]):
        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)
        pipeline_instance.run_sync(until_patterns, sleep_schedule=self.custom_sleep_schedule_parsed())
        return pipeline_instance

    def do_validate(self, pipeline_instance):
        self.validate({
            task.key: task
            for task in pipeline_instance.query("*")
        })

    def test_run_pipeline(self):
        pass

    def _test_cli_generated_array_parents(self):
        pipeline_instance = self.create_prepare_and_run_pipeline(TestSandboxDir(self))

        def create_parent_task(parent_task_key, match):
            test_cli(
                self,
                'create-array-parent',
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                parent_task_key, match,
                '--slurm-account=dummy', '--force'
            )

        self.assertRaises(UpstreamTasksNotCompleted, lambda: create_parent_task('p1', 't_a_*'))

        test_cli(
            self,
            'task',
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
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
                self,
                'task',
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                f'{pipeline_instance.state_file_tracker.pipeline_instance_dir}/.drypipe/{parent_task_key}',
                '--wait'
            )

        run_parent_task('p1')

        for task in pipeline_instance.query("t_a_*", include_incomplete_tasks=True):
            self.assertEqual("state.completed", task.state_name())

        create_parent_task('p2', 't_*')

        self.assertEqual({k for k in keys_p1('p2')}, {'t_b_2', 't_b_1'})

        test_cli(
            self,
            'array-submit',
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            '--task-key=p2',
            '--limit=1'
        )

        test_cli(
            self,
            'array-submit',
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            '--task-key=p2',
            '--limit=1'
        )

        atm = ArrayTaskManager(TaskProcess(
            os.path.join(pipeline_instance.state_file_tracker.pipeline_work_dir, "p2")
        ))

        self.assertEqual(
            {task_key: state for task_key, state in atm.list_array_states()},
            {"t_b_1": "state.completed", "t_b_2": "state.completed"}
        )

        self.do_validate(pipeline_instance)


class CliTestScenario2(PipelineWithSlurmArray):

    def test_run_until(self):
        d = TestSandboxDir(self)

        test_cli(
            self,
            'run',
            '--pipeline-instance-dir', d.sandbox_dir,
            '--generator', 'dry_pipe_tests.cli_tests:pipeline_with_slurm_array_2',
            '--until', 't_a*'
        )
