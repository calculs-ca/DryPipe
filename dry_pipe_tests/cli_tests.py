import glob
import math
import os.path
import re
import time
from pathlib import Path

from dry_pipe import DryPipe, TaskConf
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


def _child_number(key):
    m = re.match(r"t(\d+)$", key)
    return None if m is None else int(m.group(1))


def func_filter_even_children():
    """factory: selects the even numbered children of dag_simple_array (t02, t04, ...), excludes parent 'ap'"""
    def f(key, state_name, step):
        n = _child_number(key)
        return n is not None and n % 2 == 0
    return f


def func_filter_children_above(threshold):
    """factory with an argument: selects children whose number is strictly above threshold"""
    def f(key, state_name, step):
        n = _child_number(key)
        return n is not None and n > threshold
    return f


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


class CliFuncFilterTests(BasePipelineTest):
    """
    Tests --func-filter, validated with list-keys. The pipeline is dag_simple_array
    (children t01..t11 + array parent 'ap'). func_filter_even_children selects the
    even numbered children, i.e. t02, t04, t06, t08, t10.
    """

    generator = 'dry_pipe_tests.cli_tests:simple_array_pipeline'

    expected_even_children = ['t02', 't04', 't06', 't08', 't10']

    def test_run_pipeline(self):
        pass

    def _prepare(self, d):
        test_cli(
            self,
            'prepare',
            f'--pipeline-instance-dir={d.sandbox_dir}',
            f'--generator={self.generator}'
        )

    def _list_keys(self, d, *extra_args):
        return sorted(Cli.invoke_and_iterate_lines(
            'list-keys',
            f'--pipeline-instance-dir={d.sandbox_dir}',
            f'--generator={self.generator}',
            *extra_args,
            env={"DRYPIPE_SERVICE_SLEEP_SCHEDULE": self.custom_sleep_schedule()},
            test_mode=True
        ))

    def test_no_filter_lists_all_keys(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        expected_all = sorted([f"t{i:02d}" for i in range(1, 12)] + ["ap"])
        self.assertEqual(self._list_keys(d), expected_all)

    def test_func_filter_with_full_module_func(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        self.assertEqual(
            self._list_keys(d, '--func-filter=dry_pipe_tests.cli_tests:func_filter_even_children()'),
            self.expected_even_children
        )

    def test_func_filter_with_bare_name_resolved_against_generator_module(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        # bare name (no ":") is looked up in the --generator module; a bare name with no
        # parens is a zero-arg factory call (equivalent to "func_filter_even_children()")
        self.assertEqual(
            self._list_keys(d, '--func-filter=func_filter_even_children'),
            self.expected_even_children
        )

    def test_func_filter_combines_with_glob_filter(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        # glob keeps t01..t09, func-filter keeps even children -> intersection
        self.assertEqual(
            self._list_keys(
                d,
                '--filter=t0*',
                '--func-filter=func_filter_even_children()'
            ),
            ['t02', 't04', 't06', 't08']
        )

    def _set_state(self, d, state, key_glob):
        test_cli(
            self,
            'set-state',
            f'--pipeline-instance-dir={d.sandbox_dir}',
            f'--generator={self.generator}',
            f'--state={state}',
            f'--filter={key_glob}'
        )

    def test_filter_not_completed_then_func_filter(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        # mark two of the even children completed, so --filter-not-completed excludes them first
        self._set_state(d, 'completed', 't02')
        self._set_state(d, 'completed', 't04')

        # --filter-not-completed first selects everything except t02, t04 (the completed ones),
        # then --func-filter keeps only the even children among those survivors.
        # t02 and t04 are even, but were already excluded by the not-completed filter.
        self.assertEqual(
            self._list_keys(
                d,
                '--filter-not-completed',
                '--func-filter=func_filter_even_children()'
            ),
            ['t06', 't08', 't10']
        )

    def test_func_filter_with_args(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        # the factory is called once with the parsed args and returns the (key, state_name, step) filter
        self.assertEqual(
            self._list_keys(d, '--func-filter=func_filter_children_above(threshold=8)'),
            ['t09', 't10', 't11']
        )


# ---------------------------------------------------------------------------
# --sbatch-options override tests
#
# The original sbatch options are defined in the pipeline definition (via
# task_conf.sbatch_options). The --sbatch-options cli option produces an
# "overrider" function (Cli.sbatch_options_overrider_func_if_option_exists),
# and this function is threaded into:
#   - TaskProcess.sbatch_cmd_lines(overrider)          (plain slurm task)
#   - ArrayTaskManager.next_submits(sbatch_option_overrider=overrider)  (slurm array)
#
# Overriding semantics: options with the same name are replaced, new options
# are added, and options only present in the original are kept untouched.
# ---------------------------------------------------------------------------

# original sbatch options, defined in the pipeline definition below
SBATCH_OVERRIDE_ORIGINAL_OPTIONS = ["--mem=10G", "--cpus-per-task=4", "--time=1:00:00"]


def _sbatch_override_task_conf():
    return TaskConf(
        executer_type="slurm",
        slurm_account="acct",
        sbatch_options=list(SBATCH_OVERRIDE_ORIGINAL_OPTIONS)
    )


def dag_sbatch_override(dsl):
    # a plain (non array) slurm task, exercised by TaskProcess.sbatch_cmd_lines()
    yield dsl.task(
        key="solo",
        task_conf=_sbatch_override_task_conf()
    ).calls("""
        #!/usr/bin/env bash
        echo solo
    """)()

    # slurm array children, exercised by ArrayTaskManager.next_submits()
    for i in [1, 2, 3]:
        yield dsl.task(
            key=f"c{i}",
            is_slurm_array_child=True,
            task_conf=TaskConf(executer_type="slurm")
        ).calls("""
            #!/usr/bin/env bash
            echo child
        """)()

    for match in dsl.query_all_or_nothing("c*", state="ready"):
        yield dsl.task(
            key="ap",
            task_conf=_sbatch_override_task_conf()
        ).slurm_array_parent(
            children_tasks=match.tasks
        )()


def sbatch_override_pipeline():
    return DryPipe.create_pipeline(dag_sbatch_override)


class CliSbatchOptionsOverrideTests(BasePipelineTest):
    """
    Validates that a --sbatch-options cli option correctly overrides the sbatch
    options declared in the pipeline definition (task_conf), both for a plain
    slurm task (TaskProcess.sbatch_cmd_lines) and for a slurm array
    (ArrayTaskManager.next_submits).
    """

    generator = 'dry_pipe_tests.cli_tests:sbatch_override_pipeline'

    # this class does not run a pipeline, it only inspects generated commands
    def test_run_pipeline(self):
        pass

    def _prepare(self, d):
        test_cli(
            self,
            'prepare',
            f'--pipeline-instance-dir={d.sandbox_dir}',
            f'--generator={self.generator}'
        )

    def _cli_with_sbatch_options(self, d, command, task_key, sbatch_options=None):
        args = [
            self,
            command,
            f'--pipeline-instance-dir={d.sandbox_dir}',
            f'--generator={self.generator}',
            f'--task-key={task_key}',
        ]
        if sbatch_options is not None:
            args.append(f'--sbatch-options={sbatch_options}')

        c = create_cli(*args)
        c.parse_args()
        return c

    def _solo_sbatch_options(self, d, sbatch_options=None):
        """the sbatch options portion of the sbatch command for the plain 'solo' task"""
        c = self._cli_with_sbatch_options(d, 'sbatch-gen', 'solo', sbatch_options)

        task_process = TaskProcess(
            Path(d.sandbox_dir, ".drypipe", "solo").__str__(), no_logger=True
        )

        overrider = c.sbatch_options_overrider_func_if_option_exists()
        return list(task_process.sbatch_cmd_lines(overrider))

    def _array_sbatch_options(self, d, sbatch_options=None):
        """the sbatch command of the single submit produced for the 'ap' slurm array"""
        c = self._cli_with_sbatch_options(d, 'array-submit', 'ap', sbatch_options)

        task_process = TaskProcess(
            Path(d.sandbox_dir, ".drypipe", "ap").__str__(), no_logger=True
        )
        self.assertTrue(task_process.is_slurm_array_parent())

        atm = task_process.create_array_task_manager()

        overrider = c.sbatch_options_overrider_func_if_option_exists()
        submits = list(atm.next_submits(sbatch_option_overrider=overrider))
        self.assertEqual(len(submits), 1)
        return submits[0].sbatch_command

    # ---- plain slurm task : TaskProcess.sbatch_cmd_lines ------------------

    def test_solo_no_override_keeps_original_options(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        cmd = self._solo_sbatch_options(d)

        # with no --sbatch-options, the original options are emitted verbatim
        for o in SBATCH_OVERRIDE_ORIGINAL_OPTIONS:
            self.assertIn(o, cmd)

    def test_solo_override_replaces_same_name_and_adds_new(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        cmd = self._solo_sbatch_options(d, sbatch_options="--mem=20G --partition=gpu")

        # --mem is replaced (same name)
        self.assertIn("--mem=20G", cmd)
        self.assertNotIn("--mem=10G", cmd)
        # --partition is added (new name)
        self.assertIn("--partition=gpu", cmd)
        # untouched original options are kept
        self.assertIn("--cpus-per-task=4", cmd)
        self.assertIn("--time=1:00:00", cmd)

    def test_solo_override_only_adds_new_options(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        cmd = self._solo_sbatch_options(d, sbatch_options="--gpus-per-node=2")

        # no name collision : every original option is kept, the new one is added
        for o in SBATCH_OVERRIDE_ORIGINAL_OPTIONS:
            self.assertIn(o, cmd)
        self.assertIn("--gpus-per-node=2", cmd)

    def test_solo_override_replaces_all_original_options(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        cmd = self._solo_sbatch_options(
            d, sbatch_options="--mem=99G --cpus-per-task=8 --time=2:00:00"
        )

        # every option has the same name as an original : all are replaced,
        # none of the original values survive
        self.assertIn("--mem=99G", cmd)
        self.assertIn("--cpus-per-task=8", cmd)
        self.assertIn("--time=2:00:00", cmd)
        for o in SBATCH_OVERRIDE_ORIGINAL_OPTIONS:
            self.assertNotIn(o, cmd)

    def test_solo_override_with_surrounding_whitespace(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        # leading/trailing spaces around the option string must not create empty
        # tokens (the overrider strips before splitting on spaces)
        cmd = self._solo_sbatch_options(d, sbatch_options="  --mem=20G  ")

        self.assertIn("--mem=20G", cmd)
        self.assertNotIn("--mem=10G", cmd)
        self.assertIn("--cpus-per-task=4", cmd)
        self.assertIn("--time=1:00:00", cmd)

    # ---- slurm array : ArrayTaskManager.next_submits ----------------------

    def test_array_no_override_keeps_original_options(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        cmd = self._array_sbatch_options(d)

        for o in SBATCH_OVERRIDE_ORIGINAL_OPTIONS:
            self.assertIn(o, cmd)

    def test_array_override_replaces_same_name_and_adds_new(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        cmd = self._array_sbatch_options(d, sbatch_options="--mem=20G --partition=gpu")

        # --mem replaced
        self.assertIn("--mem=20G", cmd)
        self.assertNotIn("--mem=10G", cmd)
        # --partition added
        self.assertIn("--partition=gpu", cmd)
        # untouched originals kept
        self.assertIn("--cpus-per-task=4", cmd)
        self.assertIn("--time=1:00:00", cmd)

    def test_array_override_replaces_all_original_options(self):
        d = TestSandboxDir(self)
        self._prepare(d)

        cmd = self._array_sbatch_options(
            d, sbatch_options="--mem=99G --cpus-per-task=8 --time=2:00:00"
        )

        self.assertIn("--mem=99G", cmd)
        self.assertIn("--cpus-per-task=8", cmd)
        self.assertIn("--time=2:00:00", cmd)
        for o in SBATCH_OVERRIDE_ORIGINAL_OPTIONS:
            self.assertNotIn(o, cmd)
