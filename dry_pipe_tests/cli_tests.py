import glob
import math
import os.path
import re
import time
from pathlib import Path

from dry_pipe import DryPipe
from dry_pipe.cli import Cli
from dry_pipe.core_lib import UpstreamTasksNotCompleted
from dry_pipe.pipeline import Pipeline
from dry_pipe.task_process import TaskProcess

from dry_pipe_tests.base_pipeline_test import BasePipelineTest
from dry_pipe_tests.pipeline_tests_with_slurm_mockup import PipelineWithSlurmArray
from dry_pipe.slurm_arrays import ArrayTaskManager
from dry_pipe_tests.test_utils import TestSandboxDir
from dry_pipe_tests.pipeline_tests_with_slurm_arrays import PipelineWithSlurmArrayForRealSlurmTest, \
    PipelineWithSlurmArrayForRestarts, dag_simple_array


def simple_array_pipeline():
    return DryPipe.create_pipeline(dag_simple_array)


def create_cli(*args, **kwargs):
    test = args[0]

    env = {
        "DRYPIPE_TASK_DEBUG": test.is_log_level_debug().__str__(),
        "DRYPIPE_INSTANCE_DEBUG": test.is_log_level_debug().__str__(),
        "DRYPIPE_SERVICE_SLEEP_SCHEDULE": test.custom_sleep_schedule()
    }

    if "env" in kwargs:
        env.update(kwargs["env"])

    args = args[1:]

    return Cli(args, env=env, test_mode=True)

def test_cli(*args, **kwargs):    
    create_cli(*args, **kwargs).invoke()

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
            self.assertIn(task.state_name(), {'state.ready', 'state.waiting'})

        test_cli(
            self,
            'array-submit',
            f'-pid={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            f'--generator={pipeline_with_slurm_array_1_modfunc}',
            '-k=array-parent'
        )

        time.sleep(5)    

            
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


    def test_array_submit_with_filter(self):
        d = TestSandboxDir(self)

        test_cli(
            self,
            'prepare',
            '--pipeline-instance-dir', d.sandbox_dir,
            '--generator', 'dry_pipe_tests.cli_tests:pipeline_with_slurm_array_2'
        )
        
        test_cli(
            self,
            'array-submit',
            '-k=array_parent',
            '--pipeline-instance-dir', d.sandbox_dir,
            '--generator', 'dry_pipe_tests.cli_tests:pipeline_with_slurm_array_2',
            '--filter=t_a*'
        )

        c = create_cli(
            self,
            'array-submit',
            '-k=array_parent',
            '--pipeline-instance-dir', d.sandbox_dir,
            '--generator', 'dry_pipe_tests.cli_tests:pipeline_with_slurm_array_2',
            '--filter=t_a*',
            '--sbatch-options=--mem=20 --account=x'
        )   

        c.parse_args()
        

        original_options = ["--mem=10"]

        overriden_options = c.sbatch_options_overrider_func(original_options)

        self.assertSetEqual(
            set(overriden_options),
            {'--mem=20', '--account=x'}
        )


    def _test_run_until(self):
        d = TestSandboxDir(self)

        test_cli(
            self,
            'run',
            '--pipeline-instance-dir', d.sandbox_dir,
            '--generator', 'dry_pipe_tests.cli_tests:pipeline_with_slurm_array_2',
            '--until', 't_a*'
        )


class CliTestArraySubmitTasksPerJob(BasePipelineTest):
    """
    Tests --tasks-per-job, using dag_simple_array (11 children, t01..t11) as the
    pipeline definition: with --tasks-per-job=3, the 11 children should be bundled
    into ceil(11/3)=4 slurm array slots, instead of one slot per child.
    """

    generator = 'dry_pipe_tests.cli_tests:simple_array_pipeline'
    tasks_per_job = 3
    number_of_children = 11

    def test_run_pipeline(self):
        pass

    def test_array_submit_with_tasks_per_job(self):

        d = TestSandboxDir(self)

        test_cli(
            self,
            'prepare',
            f'--pipeline-instance-dir={d.sandbox_dir}',
            f'--generator={self.generator}'
        )

        test_cli(
            self,
            'array-submit',
            f'-pid={d.sandbox_dir}',
            f'--generator={self.generator}',
            '-k=ap',
            f'--tasks-per-job={self.tasks_per_job}',
            '--wait'
        )

        # all children are now complete, running the array-parent task itself
        # makes it observe that and transition to state.completed
        test_cli(
            self,
            'task',
            f'--pipeline-instance-dir={d.sandbox_dir}',
            '--task-key=ap',
            '--wait'
        )

        pipeline_instance = Pipeline.load_from_module_func(self.generator).create_pipeline_instance(
            d.sandbox_dir, instance_log_level=self.instance_log_level()
        )

        tasks_by_keys = pipeline_instance.query_all_tasks_by_key()

        array_task_id_of = {}

        for i in range(1, self.number_of_children + 1):
            task_key = f"t{i:02d}"
            task = tasks_by_keys[task_key]

            self.assertTrue(task.is_completed(), f"{task_key}: {task.state_name()}")
            self.assertEqual(int(task.outputs.r), i * 2)

            drypipe_log = Path(d.sandbox_dir, ".drypipe", task_key, "drypipe.log")
            with open(drypipe_log) as f:
                log_content = f.read()

            m = re.search(r"SLURM_ARRAY_TASK_ID: (\d+)", log_content)
            self.assertIsNotNone(m, f"no SLURM_ARRAY_TASK_ID logged for {task_key}")
            array_task_id_of[task_key] = m.group(1)

        self.assertTrue(tasks_by_keys["ap"].is_completed())

        children_by_array_task_id = {}
        for task_key, array_task_id in array_task_id_of.items():
            children_by_array_task_id.setdefault(array_task_id, []).append(task_key)

        expected_array_size = math.ceil(self.number_of_children / self.tasks_per_job)

        self.assertEqual(
            len(children_by_array_task_id),
            expected_array_size,
            f"expected {expected_array_size} packed slurm array slots for "
            f"{self.number_of_children} tasks with tasks-per-job={self.tasks_per_job}, "
            f"got {len(children_by_array_task_id)}: {children_by_array_task_id}"
        )

        for array_task_id, children in children_by_array_task_id.items():
            self.assertLessEqual(
                len(children), self.tasks_per_job,
                f"slurm array slot {array_task_id} got {len(children)} packed tasks, "
                f"more than tasks-per-job={self.tasks_per_job}: {children}"
            )
