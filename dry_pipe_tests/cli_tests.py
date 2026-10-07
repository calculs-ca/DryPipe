import glob
import inspect
import io
import itertools
import math
import os.path
import re
import shutil
import sqlite3
import subprocess
import time
import unittest
import unittest.mock
from pathlib import Path

from dry_pipe import DryPipe, TaskConf
from dry_pipe.cli import Cli, _load_filter_from_keys, _signature_numbers
from dry_pipe.core_lib import UpstreamTasksNotCompleted
from dry_pipe.pipeline import Pipeline
from dry_pipe.task_classifier import lazy_task_inputs_outputs
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
            'array-submit', '--yes',
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
            'array-submit', '--yes',
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            '--task-key', 'array-parent',
            '--limit', '1'
        )

        self.assertEqual(
            len(self._get_job_files(pipeline_instance,"array-parent")),
            1
        )

        Cli([
            'array-submit', '--yes',
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
                'array-submit', '--yes',
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
            'array-submit', '--yes',
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
            'array-submit', '--yes',
            f'--pipeline-instance-dir={pipeline_instance.state_file_tracker.pipeline_instance_dir}',
            '--task-key=p2',
            '--limit=1'
        )

        test_cli(
            self,
            'array-submit', '--yes',
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
            'array-submit', '--yes',
            '-k=array_parent',
            '--pipeline-instance-dir', d.sandbox_dir,
            '--generator', 'dry_pipe_tests.cli_tests:pipeline_with_slurm_array_2',
            '--filter=t_a*'
        )

        c = create_cli(
            self,
            'array-submit', '--yes',
            '-k=array_parent',
            '--pipeline-instance-dir', d.sandbox_dir,
            '--generator', 'dry_pipe_tests.cli_tests:pipeline_with_slurm_array_2',
            '--filter=t_a*',
            '--sbatch-options=--mem=20 --account=x'
        )   

        c.parse_args()
        

        original_options = ["--mem=10"]

        overriden_options = c.sbatch_options_overrider_func_if_option_exists()(original_options)

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
            'array-submit', '--yes',
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


class CliArraySubmitFilterFromTests(BasePipelineTest):

    generator = 'dry_pipe_tests.cli_tests:simple_array_pipeline'

    def test_run_pipeline(self):
        pass

    def test_array_submit_only_the_keys_of_filter_from(self):
        d = TestSandboxDir(self)
        test_cli(self, 'prepare', f'--pipeline-instance-dir={d.sandbox_dir}', f'--generator={self.generator}')

        keys_file = Path(d.sandbox_dir, "keys.txt")
        keys_file.write_text("t02\nt05\n")

        test_cli(
            self, 'array-submit', '--yes', f'-pid={d.sandbox_dir}', f'--generator={self.generator}', '-k=ap',
            f'--filter-from={keys_file}', '--wait'
        )

        tasks_by_keys = Pipeline.load_from_module_func(self.generator).create_pipeline_instance(
            d.sandbox_dir, instance_log_level=self.instance_log_level()
        ).query_all_tasks_by_key()

        completed_children = sorted(
            key for key, task in tasks_by_keys.items() if key != "ap" and task.is_completed()
        )
        self.assertEqual(completed_children, ['t02', 't05'])


class CliArraySubmitConfirmationTests(BasePipelineTest):

    generator = 'dry_pipe_tests.cli_tests:simple_array_pipeline'

    def test_run_pipeline(self):
        pass

    def _array_submit_answering(self, answer):
        """array-submit of t02 and t05, answering the confirmation, returns (prompt, completed children, array files)"""
        d = TestSandboxDir(self, other_func=inspect.stack()[1].function)
        d.delete_sandbox()
        test_cli(self, 'prepare', f'--pipeline-instance-dir={d.sandbox_dir}', f'--generator={self.generator}')

        keys_file = Path(d.sandbox_dir, "keys.txt")
        keys_file.write_text("t02\nt05\n")

        stderr = io.StringIO()
        with unittest.mock.patch('sys.stdin', io.StringIO(answer)), unittest.mock.patch('sys.stderr', stderr):
            test_cli(
                self, 'array-submit', f'-pid={d.sandbox_dir}', f'--generator={self.generator}', '-k=ap',
                f'--filter-from={keys_file}', '--wait'
            )

        tasks_by_keys = Pipeline.load_from_module_func(self.generator).create_pipeline_instance(
            d.sandbox_dir, instance_log_level=self.instance_log_level()
        ).query_all_tasks_by_key()
        completed_children = sorted(key for key, task in tasks_by_keys.items() if key != "ap" and task.is_completed())
        array_files = sorted(Path(d.sandbox_dir, ".drypipe", "ap").glob("array.*.tsv"))

        return stderr.getvalue(), completed_children, array_files

    def test_submits_when_confirmed(self):
        prompt, completed_children, array_files = self._array_submit_answering("y\n")

        self.assertIn("Will submit slurm array with 2 tasks", prompt)
        self.assertEqual(completed_children, ['t02', 't05'])
        self.assertEqual([f.name for f in array_files], ['array.0.tsv'])
        # N in the prompt is the key count of the array file
        self.assertEqual(array_files[0].read_text().split(), ['t02', 't05'])

    def test_nothing_submitted_when_not_confirmed(self):
        for answer in ["n\n", "\n", ""]:
            with self.subTest(repr(answer)):
                prompt, completed_children, array_files = self._array_submit_answering(answer)

                self.assertIn("Will submit slurm array with 2 tasks", prompt)
                self.assertIn("nothing submitted", prompt)
                self.assertEqual(completed_children, [])
                self.assertEqual(array_files, [])


class FilterFromTests(unittest.TestCase):

    analysis_file = """## Error signatures

3 tasks, 2 signatures

| # | tasks | signature |
|---:|---:|---|
| 1 | 2 | `ValueError: bad \\| value <n>` |
| 2 | 1 | `KeyError: <str>` |

## Tails

### 1. t1 (signature 1)

tail -50 .drypipe/t1/out.log

````text
### 9. t99 (signature 1)
### not a heading, in a fenced tail
```
ValueError: bad | value 1
````

### 2. t2 (signature 2)

no out.log

### 3. t3 (signature 1)

tail -50 .drypipe/t3/out.log

```text
ValueError: bad | value 2
```
"""

    def write(self, name, text):
        f = Path(TestSandboxDir(self, other_func=name).sandbox_dir, name)
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(text)
        return f

    def test_key_file(self):
        f = self.write("keys.tsv", "t1\tignored column\n\n  t2  \nt3\n")
        self.assertEqual(_load_filter_from_keys(str(f)), {"t1", "t2", "t3"})

    def test_whole_analysis_file(self):
        f = self.write("failed.0-3.md", self.analysis_file)
        # t99 is in a fenced tail, it's not a task of the file
        self.assertEqual(_load_filter_from_keys(str(f)), {"t1", "t2", "t3"})

    def test_signatures_of_analysis_file(self):
        f = self.write("failed.0-3.md", self.analysis_file)
        self.assertEqual(_load_filter_from_keys(f"{f}:1"), {"t1", "t3"})
        self.assertEqual(_load_filter_from_keys(f"{f}:2"), {"t2"})
        self.assertEqual(_load_filter_from_keys(f"{f}:2, 1"), {"t1", "t2", "t3"})

    def test_invalid_signature_numbers(self):
        f = self.write("failed.0-3.md", self.analysis_file)
        for numbers in ["3", "0", "1,x", ""]:
            with self.subTest(numbers):
                with self.assertRaises(Exception):
                    _load_filter_from_keys(f"{f}:{numbers}")

    def test_signature_numbers(self):
        self.assertEqual(_signature_numbers("1, 3,3\n", 3), {1, 3})
        with self.assertRaisesRegex(Exception, "valid numbers are 1 to 2"):
            _signature_numbers("1,3", 2)

    def test_key_with_spaces(self):
        f = self.write("keys.tsv", "t1\nt 2\tignored column\n")
        with self.assertRaisesRegex(Exception, r"keys.tsv:2: task keys can't contain spaces, got 't 2'"):
            _load_filter_from_keys(str(f))

    def test_analysis_file_with_keys_column(self):
        f = self.write("failed.0-3.md", self.analysis_file.replace(
            "| # | tasks | signature |\n|---:|---:|---|\n"
            "| 1 | 2 | `ValueError: bad \\| value <n>` |\n| 2 | 1 | `KeyError: <str>` |",
            "| # | tasks | signature | keys |\n|---:|---:|---|---|\n"
            "| 1 | 2 | `ValueError: bad \\| value <n>` | t1 t3 |\n| 2 | 1 | `KeyError: <str>` | t2 |"
        ))
        self.assertEqual(_load_filter_from_keys(f"{f}:2"), {"t2"})

    def test_malformed_analysis_files(self):
        table = "| # | tasks | signature |\n|---:|---:|---|\n"

        for name, (old, new), error in [
            ("no table", (table, ""), "no error signatures table"),
            ("bad separator", ("|---:|---:|---|", "|---|---|---|"), r":6: .*expected \|---:\|---:\|---\|"),
            ("rows not numbered in order", ("| 2 | 1 |", "| 3 | 1 |"), r":8: .*expected \| 2 \| <task count>"),
            ("task count not a number", ("| 1 | 2 |", "| 1 | two |"), r":7: .*expected \| 1 \| <task count>"),
            ("signature not code", ("`KeyError: <str>`", "KeyError: <str>"), r":8: "),
            ("heading of the old format", ("### 2. t2 (signature 2)", "### 2. t2"), r":23: .*### <i>. <task-key>"),
            ("key with a space", ("### 2. t2 (signature 2)", "### 2. t 2 (signature 2)"), r":23: "),
        ]:
            with self.subTest(name):
                self.assertIn(old, self.analysis_file)
                f = self.write("failed.0-3.md", self.analysis_file.replace(old, new))
                for spec in [str(f), f"{f}:1"]:
                    with self.assertRaisesRegex(Exception, error):
                        _load_filter_from_keys(spec)

    def test_missing_file(self):
        for spec in ["no-such-keys.txt", "no-such-analysis.md", "no-such-analysis.md:1"]:
            with self.subTest(spec):
                with self.assertRaises(FileNotFoundError):
                    _load_filter_from_keys(spec)


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


def dag_that_fails(dsl):
    raise Exception("dag_that_fails always fails")
    yield


def pipeline_that_fails():
    return DryPipe.create_pipeline(dag_that_fails)


def dag_that_fails_after_first_task(dsl):
    yield dsl.task(key="t1").calls("""
        #!/usr/bin/env bash
        echo t1
    """)()
    raise Exception("dag_that_fails_after_first_task fails after t1")


def pipeline_that_fails_after_first_task():
    return DryPipe.create_pipeline(dag_that_fails_after_first_task)


class CliStatusDbTests(BasePipelineTest):

    generator = 'dry_pipe_tests.cli_tests:simple_array_pipeline'

    def test_run_pipeline(self):
        pass

    def _status_db(self, d, *extra_args, generator=None):
        test_cli(
            self,
            'status-db',
            f'--pipeline-instance-dir={d.sandbox_dir}',
            f'--generator={generator or self.generator}',
            *extra_args
        )

    def _query(self, d, sql):
        with sqlite3.connect(Path(d.sandbox_dir, ".drypipe", "status.db")) as conn:
            rows = conn.execute(sql).fetchall()
        conn.close()
        return rows

    def test_status_db_with_filter(self):
        d = TestSandboxDir(self)
        test_cli(self, 'prepare', f'--pipeline-instance-dir={d.sandbox_dir}', f'--generator={self.generator}')

        Path(d.sandbox_dir, ".drypipe", "t01", "out.log").write_text("t01 says hi\n")

        self._status_db(d, '--filter=t0*', '--instance-name=z')
        # a second invocation overwrites the db, it doesn't append
        self._status_db(d, '--filter=t0*', '--instance-name=z')

        self.assertEqual(self._query(d, "select * from instance_status"), [("z", "ok", None)])

        task_rows = self._query(d, "select * from task_status order by key")
        self.assertEqual([row[1] for row in task_rows], [f"t0{i}" for i in range(1, 10)])
        self.assertEqual(task_rows[0], ("z", "t01", "waiting", None, None, "t01 says hi\n", "<no error line> last: <key> says hi"))
        # t02 never ran, it has no logs
        self.assertEqual(task_rows[1], ("z", "t02", "waiting", None, None, None, "<no out.log>"))

    def test_status_db_log_signature(self):
        d = TestSandboxDir(self)
        test_cli(self, 'prepare', f'--pipeline-instance-dir={d.sandbox_dir}', f'--generator={self.generator}')

        Path(d.sandbox_dir, ".drypipe", "t01", "out.log").write_text("t01 starts\nKeyError: 'x' at row 3\n")

        def signatures():
            return self._query(d, "select key, log_signature from task_status order by key")

        self._status_db(d, '--filter=t0[12]')
        self.assertEqual(signatures(), [("t01", "KeyError: <str> at row <n>"), ("t02", "<no out.log>")])

        self._status_db(d, '--filter=t01', '--log-classifier=log_classifier_merging_lookup_errors')
        self.assertEqual(signatures(), [("t01", "lookup error")])

        test_cli(
            self, 'status-db', f'--pipeline-instance-dir={d.sandbox_dir}', f'--generator={self.generator}', '--filter=t01',
            env={"DRYPIPE_LOG_CLASSIFIER": "log_classifier_merging_lookup_errors"}
        )
        self.assertEqual(signatures(), [("t01", "lookup error")])

        self._status_db(d, '--tsv', '--filter=t01')
        [row] = self._import_tsv(
            d, "task_status",
            "create table task_status (instance_name text, key text, state text, step int, drypipe_log text, out_log text, log_signature text)"
        )
        self.assertEqual(row[-1], "KeyError: <str> at row <n>")

    def test_status_db_lean(self):
        d = TestSandboxDir(self)
        test_cli(self, 'prepare', f'--pipeline-instance-dir={d.sandbox_dir}', f'--generator={self.generator}')
        test_cli(
            self, 'set-state', f'--pipeline-instance-dir={d.sandbox_dir}', f'--generator={self.generator}',
            '--state=completed', '--filter=t02'
        )

        # 60 lines of 200 chars span more than one of the 8192 bytes blocks read from the end
        out_lines = [f"{i:03d} {'x' * 195}\r\n" for i in range(60)]
        Path(d.sandbox_dir, ".drypipe", "t01", "out.log").write_text("".join(out_lines), newline="")
        # fewer than 50 lines, and no newline at the end
        Path(d.sandbox_dir, ".drypipe", "t01", "drypipe.log").write_text("a\nb\nc")

        Path(d.sandbox_dir, ".drypipe", "t02", "out.log").write_text("t02 log")

        self._status_db(d, '--lean', '--filter=t0[123]')

        task_rows = self._query(d, "select key, state, drypipe_log, out_log from task_status order by key")
        self.assertEqual([(key, state) for key, state, _, _ in task_rows], [("t01", "waiting"), ("t02", "completed"), ("t03", "waiting")])
        _, _, drypipe_log, out_log = task_rows[0]
        self.assertEqual(out_log, "".join(out_lines[-50:]))
        self.assertEqual(drypipe_log, "a\nb\nc")
        # --lean drops the logs of completed tasks, they are not read for the signature either
        self.assertEqual(task_rows[1][2:], (None, None))
        self.assertEqual(
            self._query(d, "select log_signature from task_status where key = 't02'"), [("<no out.log>",)]
        )

        # without --lean, completed tasks keep their logs
        self._status_db(d, '--filter=t02')
        self.assertEqual(self._query(d, "select out_log from task_status"), [("t02 log",)])

    def test_status_db_missing_drypipe(self):
        d = TestSandboxDir(self)
        Path(d.sandbox_dir).mkdir(parents=True, exist_ok=True)

        self._status_db(d)

        self.assertEqual(
            self._query(d, "select * from instance_status"),
            [(os.path.basename(d.sandbox_dir), "missing .drypipe", None)]
        )
        self.assertEqual(self._query(d, "select * from task_status"), [])

    def test_status_db_digest_failed(self):
        d = TestSandboxDir(self)
        Path(d.sandbox_dir, ".drypipe").mkdir(parents=True, exist_ok=True)

        self._status_db(d, generator='dry_pipe_tests.cli_tests:pipeline_that_fails')

        [(_, state, error)] = self._query(d, "select * from instance_status")
        self.assertEqual(state, "digest failed")
        self.assertIn("dag_that_fails always fails", error)
        self.assertEqual(self._query(d, "select * from task_status"), [])

    def _import_tsv(self, d, table, create_table_sql):
        db_file = Path(d.sandbox_dir, "aggregate.db")
        db_file.unlink(missing_ok=True)
        with sqlite3.connect(db_file) as conn:
            conn.execute(create_table_sql)
        conn.close()

        subprocess.run(
            ["sqlite3", db_file, ".mode tabs", f".import {Path(d.sandbox_dir, '.drypipe', f'{table}.tsv')} {table}"],
            check=True
        )

        with sqlite3.connect(db_file) as conn:
            rows = conn.execute(f"select * from {table}").fetchall()
        conn.close()
        return rows

    def test_status_db_digest_failed_after_first_task_has_no_task_rows(self):
        d = TestSandboxDir(self)
        Path(d.sandbox_dir, ".drypipe").mkdir(parents=True, exist_ok=True)
        generator = 'dry_pipe_tests.cli_tests:pipeline_that_fails_after_first_task'

        self._status_db(d, generator=generator)

        [(_, state, error)] = self._query(d, "select * from instance_status")
        self.assertEqual(state, "digest failed")
        self.assertIn("fails after t1", error)
        self.assertEqual(self._query(d, "select * from task_status"), [])

        self._status_db(d, '--tsv', generator=generator)

        self.assertEqual(Path(d.sandbox_dir, ".drypipe", "task_status.tsv").read_text(), "")
        self.assertIn("digest failed", Path(d.sandbox_dir, ".drypipe", "instance_status.tsv").read_text())

    def test_status_db_tsv_stack_dump_imports_in_sqlite(self):
        d = TestSandboxDir(self)
        Path(d.sandbox_dir, ".drypipe").mkdir(parents=True, exist_ok=True)

        self._status_db(d, '--tsv', generator='dry_pipe_tests.cli_tests:pipeline_that_fails')

        # the stack dump in the error column has newlines, it must survive the bulk import as a single row
        [(_, state, error)] = self._import_tsv(
            d, "instance_status", "create table instance_status (instance_name text, state text, error text)"
        )

        self.assertEqual(state, "digest failed")
        self.assertIn("dag_that_fails always fails", error)

    def test_status_db_tsv_logs_import_in_sqlite(self):
        d = TestSandboxDir(self)
        test_cli(self, 'prepare', f'--pipeline-instance-dir={d.sandbox_dir}', f'--generator={self.generator}')

        nasty_log = 'tab\there\nnew line\r\ncrlf "quoted" "starts quoted\\back slash\n'
        Path(d.sandbox_dir, ".drypipe", "t01", "out.log").write_text(nasty_log, newline="")
        Path(d.sandbox_dir, ".drypipe", "t01", "drypipe.log").write_bytes(b"before nul\x00after nul")

        self._status_db(d, '--tsv', '--filter=t01')

        [(_, key, state, _, drypipe_log, out_log, _)] = self._import_tsv(
            d, "task_status",
            "create table task_status (instance_name text, key text, state text, step int, drypipe_log text, out_log text, log_signature text)"
        )

        self.assertEqual((key, state), ("t01", "waiting"))
        self.assertEqual(out_log, nasty_log)
        # the sqlite3 shell's .import would truncate at NUL, it is replaced
        self.assertEqual(drypipe_log, "before nul\ufffdafter nul")


    def test_status_db_empty_aggregates_tsvs(self):
        d = TestSandboxDir(self)
        Path(d.sandbox_dir).mkdir(parents=True, exist_ok=True)

        db_file = Path(d.sandbox_dir, "aggregate.db")

        # no --pipeline-instance-dir nor --generator: --empty-db only creates the schema
        test_cli(self, 'status-db', f'--empty-db={db_file}')

        def query(sql):
            with sqlite3.connect(db_file) as conn:
                rows = conn.execute(sql).fetchall()
            conn.close()
            return rows

        self.assertEqual(query("select * from task_status"), [])
        self.assertEqual(query("select * from instance_status"), [])

        for name in ["i1", "i2"]:
            pid = Path(d.sandbox_dir, name)
            test_cli(self, 'prepare', f'--pipeline-instance-dir={pid}', f'--generator={self.generator}')
            test_cli(
                self, 'status-db', f'--pipeline-instance-dir={pid}', f'--generator={self.generator}',
                '--tsv', '--filter=t01'
            )
            for table in ["task_status", "instance_status"]:
                subprocess.run(
                    ["sqlite3", db_file, ".mode tabs", f".import {pid.joinpath('.drypipe', f'{table}.tsv')} {table}"],
                    check=True
                )

        self.assertEqual(
            query("select instance_name, key from task_status order by instance_name"),
            [("i1", "t01"), ("i2", "t01")]
        )
        self.assertEqual(
            query("select instance_name, state from instance_status order by instance_name"),
            [("i1", "ok"), ("i2", "ok")]
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


class HasFiltersTests(unittest.TestCase):

    def has_filters(self, *filter_args):
        pid = TestSandboxDir(self).sandbox_dir
        return Cli(['list-keys', f'--pipeline-instance-dir={pid}', '--generator=not:used', *filter_args]).has_filters()

    def test_no_filter(self):
        self.assertFalse(self.has_filters())

    def test_every_filter_counts(self):
        for filter_arg in [
            '--filter=t*', '--py-filter={step} > 0', '--func-filter=m:f()', '--filter-completed',
            '--filter-not-completed', '--filter-failed', '--filter-timed-out', '--filter-ready',
            '--filter-unhealthy', '--filter-from=keys.txt'
        ]:
            with self.subTest(filter_arg):
                self.assertTrue(self.has_filters(filter_arg))


class ArraySummarySubmitFiltersTests(unittest.TestCase):

    def filters(self, *job_file_lines):
        job_file = Path(TestSandboxDir(self).sandbox_dir, "array.0.job.123")
        job_file.parent.mkdir(parents=True, exist_ok=True)
        job_file.write_text("".join(f"{line}\n" for line in job_file_lines))
        return Cli([], parse_args=False)._filters_of_submit(job_file)

    def test_filters_of_submit(self):
        self.assertEqual(
            self.filters(
                "sbatch --array=0-2",
                "/bin/drypipe array-submit -pid=/p -k=ap --filter=t0* --filter-failed '--py-filter=lambda k, s, st: True' '--sbatch-options=--mem=2G'"
            ),
            "'--filter=t0*' '--py-filter=lambda k, s, st: True' --filter-failed"
        )

    def test_job_file_without_submit_command(self):
        self.assertEqual(self.filters("sbatch --array=0-2", "", "BATCH_ENDED"), "")

    def test_unparsable_submit_command(self):
        self.assertEqual(self.filters("sbatch --array=0-2", "/bin/pytest -q"), "unparsable submit command")


def log_classifier_merging_lookup_errors(default):
    """resolved by bare name from the --generator module (this module) in CliAnalyzeLogsTests"""
    default.masks.insert(0, (r"^(KeyError|IndexError):.*", "lookup error"))
    return default


def dag_with_outputs(dsl):
    t1 = dsl.task(key="t1").outputs(
        f=dsl.file("f.tsv"), n=int, csvs=dsl.file_set("*.csv")
    ).calls("""
    #!/usr/bin/env bash
    echo "..."
    """)()
    yield t1
    yield dsl.task(key="t2").inputs(
        t1.outputs.f, m=t1.outputs.n, csvs=t1.outputs.csvs, c=7
    ).calls("""
    #!/usr/bin/env bash
    echo "..."
    """)()


def pipeline_with_outputs():
    return DryPipe.create_pipeline(dag_with_outputs)


def classifier_of_missing_outputs(default):
    """resolved by bare name from the --generator module (this module) in CliTaskClassifierTests"""

    class MissingOutputs(type(default)):
        def signature(self, key, out_log, drypipe_log, state, step, task_inputs, task_outputs, runtime_metrics):
            if key == "t1" and not task_outputs.f.exists():
                return "FATAL_DID_NOT_PRODUCE_RESULTS"
            return None

    return MissingOutputs()


class CliTaskClassifierTests(BasePipelineTest):

    generator = 'dry_pipe_tests.cli_tests:pipeline_with_outputs'

    def test_run_pipeline(self):
        pass

    def _prepare(self):
        d = TestSandboxDir(self, other_func=inspect.stack()[1].function)
        d.delete_sandbox()
        test_cli(self, 'prepare', f'--pipeline-instance-dir={d.sandbox_dir}', f'--generator={self.generator}')
        return Path(d.sandbox_dir)

    def test_lazy_inputs_and_outputs(self):
        pid = self._prepare()
        t1_output_dir = Path(pid, "output", "t1")
        t1_output_dir.mkdir(parents=True)
        Path(t1_output_dir, "a.csv").write_text("")
        Path(pid, ".drypipe", "t1", "output_vars").write_text("n=3\n")

        t1_inputs, t1_outputs = lazy_task_inputs_outputs(str(Path(pid, ".drypipe", "t1")))
        t2_inputs, _ = lazy_task_inputs_outputs(str(Path(pid, ".drypipe", "t2")))

        self.assertEqual(t1_outputs.f, Path(t1_output_dir, "f.tsv"))
        self.assertEqual(t1_outputs.n, 3)
        self.assertEqual(t1_outputs.csvs, [Path(t1_output_dir, "a.csv")])

        self.assertEqual(t2_inputs.f, Path(t1_output_dir, "f.tsv"))
        self.assertEqual(t2_inputs.m, 3)
        self.assertEqual(t2_inputs.csvs, [Path(t1_output_dir, "a.csv")])
        self.assertEqual(t2_inputs.c, 7)

        with self.assertRaisesRegex(Exception, "task t1 has no output 'nope'"):
            t1_outputs.nope

    def test_lazy_inputs_and_outputs_of_a_task_that_did_not_run(self):
        pid = self._prepare()

        _, t1_outputs = lazy_task_inputs_outputs(str(Path(pid, ".drypipe", "t1")))
        t2_inputs, _ = lazy_task_inputs_outputs(str(Path(pid, ".drypipe", "t2")))

        self.assertIsNone(t1_outputs.n)
        self.assertEqual(t1_outputs.csvs, [])
        self.assertIsNone(t2_inputs.m)

    def test_analyze_logs_leaves_out_none_signatures(self):
        pid = self._prepare()
        test_cli(self, 'set-state', f'--pipeline-instance-dir={pid}', f'--generator={self.generator}', '--state=completed')
        analysis_dir = Path(pid, "analysis")

        def analyze_logs():
            test_cli(
                self, 'analyze-logs', f'--pipeline-instance-dir={pid}', f'--generator={self.generator}',
                '--all-tasks', f'--dir={analysis_dir}', '--log-classifier=classifier_of_missing_outputs'
            )
            return sorted(f.name for f in analysis_dir.iterdir())

        self.assertEqual(analyze_logs(), ["completed-1.md"])
        md = Path(analysis_dir, "completed-1.md").read_text()
        self.assertIn("`FATAL_DID_NOT_PRODUCE_RESULTS`", md)
        self.assertNotIn("t2", md)

        shutil.rmtree(analysis_dir)
        Path(pid, "output", "t1").mkdir(parents=True)
        Path(pid, "output", "t1", "f.tsv").write_text("")
        self.assertEqual(analyze_logs(), [])


class CliAnalyzeLogsTests(BasePipelineTest):

    generator = 'dry_pipe_tests.cli_tests:simple_array_pipeline'

    def test_run_pipeline(self):
        pass

    def _analyze_logs(self, *extra_args, out_log_of_t03="50% done\n", dag_args=None):
        d = TestSandboxDir(self, other_func=inspect.stack()[1].function)
        d.delete_sandbox()
        pid = f'--pipeline-instance-dir={d.sandbox_dir}'
        test_cli(self, 'prepare', pid, f'--generator={self.generator}')

        def set_state(state, key_filter):
            test_cli(self, 'set-state', pid, f'--generator={self.generator}', f'--state={state}', f'--filter={key_filter}')

        set_state('completed', 't0[1-2]')
        set_state('timed-out.1', 't03')
        set_state('failed.2', 't0[4-8]')

        def write_out_log(key, text):
            Path(d.sandbox_dir, ".drypipe", key, "out.log").write_text(f"{key} says hi\n{text}")

        for key in ['t01', 't02']:
            write_out_log(key, "done\n")
        write_out_log('t03', out_log_of_t03)

        write_out_log('t04', "ValueError: bad mass 12.5 in /data/t04/a.mgf\n")
        write_out_log('t05', "ValueError: bad mass 7.1 in /data/t05/b.mgf\n")
        write_out_log('t06', "ValueError: bad mass 3.0 in /data/t06/c.mgf\n")
        write_out_log('t07', "### 9. t99 (signature 1)\nKeyError: 'BRCA1'\n")
        write_out_log('t08', "IndexError: list index out of range | row 3\n```\n")

        analysis_dir = Path(d.sandbox_dir, "analysis")
        if dag_args is None:
            dag_args = [f'--generator={self.generator}']
        test_cli(self, 'analyze-logs', pid, *dag_args, '--filter=t0[1-8]', f'--dir={analysis_dir}', *extra_args)

        return analysis_dir

    @staticmethod
    def signatures_table(md_file):
        lines = md_file.read_text().splitlines()
        return list(itertools.takewhile(lambda l: l != "## Tails", lines))

    def test_one_markdown_file_per_state_step_of_unhealthy_tasks(self):
        analysis_dir = self._analyze_logs()

        self.assertEqual(sorted(f.name for f in analysis_dir.iterdir()), ['failed.2-5.md', 'timed-out.1-1.md'])

        def keys_in(file_name):
            return re.findall(r"(t\d+) says hi", Path(analysis_dir, file_name).read_text())

        self.assertEqual(keys_in('timed-out.1-1.md'), ['t03'])
        self.assertEqual(keys_in('failed.2-5.md'), ['t04', 't05', 't06', 't07', 't08'])

    def test_all_tasks(self):
        analysis_dir = self._analyze_logs('--all-tasks')

        self.assertEqual(
            sorted(f.name for f in analysis_dir.iterdir()),
            ['completed-2.md', 'failed.2-5.md', 'timed-out.1-1.md']
        )

    def test_filters_narrow_the_unhealthy_tasks(self):
        analysis_dir = self._analyze_logs('--filter-failed')

        self.assertEqual(sorted(f.name for f in analysis_dir.iterdir()), ['failed.2-5.md'])

    def test_filter_unhealthy(self):
        analysis_dir = self._analyze_logs()
        pid = f'--pipeline-instance-dir={analysis_dir.parent}'

        self.assertEqual(
            sorted(Cli.invoke_and_iterate_lines(
                'list-keys', pid, f'--generator={self.generator}', '--filter=t0[1-8]', '--filter-unhealthy',
                test_mode=True
            )),
            ['t03', 't04', 't05', 't06', 't07', 't08']
        )

    def test_signatures_table(self):
        analysis_dir = self._analyze_logs()

        self.assertEqual(self.signatures_table(Path(analysis_dir, 'failed.2-5.md')), [
            "## Error signatures",
            "",
            "5 tasks, 3 signatures",
            "",
            "| # | tasks | signature |",
            "|---:|---:|---|",
            "| 1 | 3 | `ValueError: bad mass <n> in <path>` |",
            "| 2 | 1 | `KeyError: <str>` |",
            "| 3 | 1 | `IndexError: list index out of range \\| row <n>` |",
            "",
        ])

    def test_signatures_table_with_keys(self):
        analysis_dir = self._analyze_logs('--full')

        self.assertEqual(self.signatures_table(Path(analysis_dir, 'failed.2-5.md'))[4:-1], [
            "| # | tasks | signature | keys |",
            "|---:|---:|---|---|",
            "| 1 | 3 | `ValueError: bad mass <n> in <path>` | t04 t05 t06 |",
            "| 2 | 1 | `KeyError: <str>` | t07 |",
            "| 3 | 1 | `IndexError: list index out of range \\| row <n>` | t08 |",
        ])

    def test_tails_are_fenced_code_blocks(self):
        analysis_dir = self._analyze_logs()

        md = Path(analysis_dir, 'failed.2-5.md').read_text()

        self.assertIn(
            "### 1. t04 (signature 1)\n\n"
            "tail -50 .drypipe/t04/out.log\n\n"
            "```text\n"
            "t04 says hi\n"
            "ValueError: bad mass 12.5 in /data/t04/a.mgf\n"
            "```\n",
            md
        )

        # t08's log contains a ``` line, its fence must be longer
        self.assertIn("````text\nt08 says hi\nIndexError: list index out of range | row 3\n```\n````\n", md)

    def test_signatures_ignore_errors_before_the_classified_lines(self):
        progress = "".join(f"{i}% done\n" for i in range(1000))
        analysis_dir = self._analyze_logs(out_log_of_t03=f"connection failed, retrying\n{progress}")

        self.assertEqual(self.signatures_table(Path(analysis_dir, 'timed-out.1-1.md'))[6:-1], [
            "| 1 | 1 | `<no error line> last: done` |",
        ])

    def test_custom_log_classifier(self):
        analysis_dir = self._analyze_logs('--log-classifier=log_classifier_merging_lookup_errors')

        self.assertEqual(self.signatures_table(Path(analysis_dir, 'failed.2-5.md'))[4:-1], [
            "| # | tasks | signature |",
            "|---:|---:|---|",
            "| 1 | 3 | `ValueError: bad mass <n> in <path>` |",
            "| 2 | 2 | `lookup error` |",
        ])

    @staticmethod
    def analysis_files(analysis_dir):
        return {f.name: f.read_text() for f in analysis_dir.iterdir()}

    def test_fs_generator_gives_the_same_analysis_as_the_generator(self):
        from_generator = self.analysis_files(self._analyze_logs('--all-tasks'))
        from_fs = self.analysis_files(self._analyze_logs('--all-tasks', dag_args=['--fs-generator']))

        self.assertEqual(from_fs, from_generator)

    def test_fs_generator_skips_ignored_tasks(self):
        pid = self._analyze_logs(dag_args=['--fs-generator']).parent
        Path(pid, "drypipe-task-set.rules").write_text("- t04\n- t05\n")
        analysis_dir = Path(pid, "analysis-without-ignored")

        test_cli(self, 'analyze-logs', f'--pipeline-instance-dir={pid}', '--fs-generator', f'--dir={analysis_dir}')

        self.assertEqual(sorted(f.name for f in analysis_dir.iterdir()), ['failed.2-3.md', 'timed-out.1-1.md'])

    def test_fs_generator_with_module_function_log_classifier(self):
        analysis_dir = self._analyze_logs(
            '--log-classifier=dry_pipe_tests.cli_tests:log_classifier_merging_lookup_errors',
            dag_args=['--fs-generator']
        )

        self.assertIn("| 2 | 2 | `lookup error` |", self.signatures_table(Path(analysis_dir, 'failed.2-5.md')))

    def test_fs_generator_rejects_bare_log_classifier_name(self):
        with self.assertRaisesRegex(Exception, "must be given as module:function"):
            self._analyze_logs(
                '--log-classifier=log_classifier_merging_lookup_errors',
                dag_args=['--fs-generator', f'--generator={self.generator}']
            )

    def test_fs_generator_without_tasks(self):
        d = TestSandboxDir(self)
        d.delete_sandbox()

        with self.assertRaisesRegex(Exception, "has no task in .drypipe/"):
            test_cli(self, 'analyze-logs', f'--pipeline-instance-dir={d.sandbox_dir}', '--fs-generator',
                     f'--dir={Path(d.sandbox_dir, "analysis")}')

    def test_generator_or_fs_generator_is_required(self):
        with self.assertRaisesRegex(Exception, "--generator or --fs-generator is required"):
            self._analyze_logs(dag_args=[])


    def _extract_keys(self, analysis_dir, answer, *extra_args):
        with unittest.mock.patch('sys.stdin', io.StringIO(answer)):
            return Cli.invoke_and_get_str(
                'extract-keys', str(Path(analysis_dir, 'failed.2-5.md')), *extra_args, test_mode=True
            ).splitlines()

    def test_extract_keys_of_selected_signatures(self):
        analysis_dir = self._analyze_logs()

        # 1: ValueError (t04 t05 t06), 2: KeyError (t07, its log has a line that looks like a tail heading),
        # 3: IndexError (t08, its signature has a | and its log a ```)
        self.assertEqual(self._extract_keys(analysis_dir, "1, 3\n"), ['t04', 't05', 't06', 't08'])
        self.assertEqual(self._extract_keys(analysis_dir, "2\n"), ['t07'])

    def test_extract_keys_to_dest_then_filter_from(self):
        analysis_dir = self._analyze_logs()
        keys_file = Path(analysis_dir.parent, "keys.txt")

        self.assertEqual(self._extract_keys(analysis_dir, "1\n", f'--dest={keys_file}'), [])
        self.assertEqual(keys_file.read_text(), "t04\nt05\nt06\n")

        self.assertEqual(
            sorted(Cli.invoke_and_iterate_lines(
                'list-keys', f'--pipeline-instance-dir={analysis_dir.parent}', f'--generator={self.generator}',
                f'--filter-from={keys_file}', test_mode=True
            )),
            ['t04', 't05', 't06']
        )

    def test_extract_keys_rejects_invalid_answers(self):
        analysis_dir = self._analyze_logs()

        for answer in ["4\n", "0\n", "a,b\n", "\n"]:
            with self.subTest(answer):
                with self.assertRaises(Exception):
                    self._extract_keys(analysis_dir, answer)

    def _list_keys_filtered_from(self, analysis_dir, filter_from):
        return sorted(Cli.invoke_and_iterate_lines(
            'list-keys', f'--pipeline-instance-dir={analysis_dir.parent}', f'--generator={self.generator}',
            f'--filter-from={filter_from}', test_mode=True
        ))

    def test_filter_from_analysis_file(self):
        analysis_dir = self._analyze_logs()
        md = Path(analysis_dir, 'failed.2-5.md')

        self.assertEqual(self._list_keys_filtered_from(analysis_dir, md), ['t04', 't05', 't06', 't07', 't08'])
        self.assertEqual(self._list_keys_filtered_from(analysis_dir, f"{md}:1,3"), ['t04', 't05', 't06', 't08'])
        self.assertEqual(self._list_keys_filtered_from(analysis_dir, f"{md}:2"), ['t07'])

        with self.assertRaisesRegex(Exception, "valid numbers are 1 to 3"):
            self._list_keys_filtered_from(analysis_dir, f"{md}:4")


    def test_filter_from_intersects_with_other_filters(self):
        analysis_dir = self._analyze_logs()
        md = Path(analysis_dir, 'failed.2-5.md')

        self.assertEqual(
            sorted(Cli.invoke_and_iterate_lines(
                'list-keys', f'--pipeline-instance-dir={analysis_dir.parent}', f'--generator={self.generator}',
                f'--filter-from={md}', '--filter=t0[2-5]', test_mode=True
            )),
            # the file has t04..t08, the glob t02..t05
            ['t04', 't05']
        )

    def test_filter_from_ignores_keys_not_in_the_pipeline(self):
        analysis_dir = self._analyze_logs()
        keys_file = Path(analysis_dir.parent, "keys.txt")
        keys_file.write_text("t04\nnot-a-task\n")

        self.assertEqual(self._list_keys_filtered_from(analysis_dir, keys_file), ['t04'])

