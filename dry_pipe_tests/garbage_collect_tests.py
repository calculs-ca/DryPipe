import shutil
import unittest
from io import StringIO
from pathlib import Path
from unittest import mock

from dry_pipe import DryPipe
from dry_pipe.cli import Cli
from dry_pipe_tests.cli_tests import simple_array_pipeline


MODULE = "dry_pipe_tests.garbage_collect_tests"

KEPT_KEYS = ["t1", "t2"]

DROPPED_KEYS = ["t3", "t4"]


@DryPipe.python_call()
def noop():
    pass


def dag_of(keys):
    def dag(dsl):
        for key in keys:
            yield dsl.task(key=key).outputs(report=dsl.file("report.txt")).calls(noop)()
    return dag


dag_before = dag_of(KEPT_KEYS + DROPPED_KEYS)

dag_after = dag_of(KEPT_KEYS)


def dag_with_empty_array(dsl):
    yield dsl.task(key="ap").slurm_array_parent(children_tasks=[])()


class GarbageCollectTests(unittest.TestCase):

    def setUp(self):
        sandbox = Path(__file__).parent / "sandboxes" / f"{self.__class__.__name__}.{self._testMethodName}"
        if sandbox.exists():
            shutil.rmtree(sandbox)
        sandbox.mkdir(parents=True)
        self.pid = sandbox / "pid"

    def cli(self, command, generator, *args, env=None):
        out = StringIO()
        Cli(
            [
                command,
                f"--pipeline-instance-dir={self.pid}",
                f"--generator={MODULE}:{generator}",
                *args
            ],
            env=env,
            test_mode=True,
            output=out
        ).invoke()
        return out.getvalue()

    def prepare_with_all_keys(self):
        self.cli("prepare", "dag_before")
        for key in KEPT_KEYS + DROPPED_KEYS:
            d = self.pid / "output" / key
            d.mkdir(parents=True, exist_ok=True)
            (d / "report.txt").write_text(key)

    def garbage_collect(self, confirm=False):
        no_confirm = [] if confirm else ["--no-confirm"]
        return self.cli("garbage-collect", "dag_after", *no_confirm)

    def task_dirs(self, parent):
        return {d.name for d in (self.pid / parent).iterdir() if d.is_dir()}

    def assert_dropped_keys_deleted(self):
        self.assertEqual(self.task_dirs("output"), set(KEPT_KEYS))
        self.assertFalse(self.task_dirs(".drypipe") & set(DROPPED_KEYS))
        self.assertTrue(set(KEPT_KEYS) <= self.task_dirs(".drypipe"))

    def assert_nothing_deleted(self):
        all_keys = set(KEPT_KEYS + DROPPED_KEYS)
        self.assertEqual(self.task_dirs("output"), all_keys)
        self.assertTrue(all_keys <= self.task_dirs(".drypipe"))

    def test_deletes_control_and_output_dirs_of_keys_not_yielded(self):
        self.prepare_with_all_keys()
        self.garbage_collect()
        self.assert_dropped_keys_deleted()

    def test_keeps_drypipe_dirs_without_state_file(self):
        self.prepare_with_all_keys()
        (self.pid / ".drypipe" / "not-a-task").mkdir()
        self.garbage_collect()
        self.assertTrue((self.pid / ".drypipe" / "not-a-task").exists())
        self.assertTrue((self.pid / ".drypipe" / "dry_pipe").exists())

    def test_nothing_to_delete_does_not_prompt(self):
        self.cli("prepare", "dag_after")
        with mock.patch("builtins.input") as input_mock:
            out = self.garbage_collect(confirm=True)
        input_mock.assert_not_called()
        self.assertIn("0 directories", out)

    def test_summary_lists_deleted_dirs(self):
        self.prepare_with_all_keys()
        out = self.garbage_collect()
        self.assertIn("4 directories", out)
        for key in DROPPED_KEYS:
            self.assertIn(str(self.pid / "output" / key), out)
            self.assertIn(str(self.pid / ".drypipe" / key), out)

    def test_answer_no_deletes_nothing(self):
        self.prepare_with_all_keys()
        with mock.patch("builtins.input", return_value="n"):
            self.garbage_collect(confirm=True)
        self.assert_nothing_deleted()

    def test_empty_answer_defaults_to_no(self):
        self.prepare_with_all_keys()
        with mock.patch("builtins.input", return_value=""):
            self.garbage_collect(confirm=True)
        self.assert_nothing_deleted()

    def test_answer_yes_deletes(self):
        self.prepare_with_all_keys()
        with mock.patch("builtins.input", return_value="y"):
            self.garbage_collect(confirm=True)
        self.assert_dropped_keys_deleted()

    def ignored_tasks_file(self, f=None):
        if f is None:
            f = self.pid.parent / "ignored-tasks.tsv"
        f.write_text("".join(f"{key}  \tsome reason\n\n" for key in DROPPED_KEYS))
        return f

    def implicit_ignored_tasks_file(self):
        return self.ignored_tasks_file(self.pid / "drypipe-ignored-tasks.tsv")

    def test_ignored_tasks_are_not_prepared(self):
        self.cli("prepare", "dag_before", f"--ignored-tasks={self.ignored_tasks_file()}")
        self.assertEqual(self.task_dirs(".drypipe") & set(KEPT_KEYS + DROPPED_KEYS), set(KEPT_KEYS))

    def garbage_collect_all_keys_dag(self, *args, env=None):
        self.cli("garbage-collect", "dag_before", "--no-confirm", *args, env=env)

    def test_garbage_collect_purges_ignored_tasks(self):
        self.prepare_with_all_keys()
        self.garbage_collect_all_keys_dag(f"--ignored-tasks={self.ignored_tasks_file()}")
        self.assert_dropped_keys_deleted()

    def test_ignored_tasks_from_env_var(self):
        self.prepare_with_all_keys()
        self.garbage_collect_all_keys_dag(env={"DRYPIPE_IGNORED_TASKS": str(self.ignored_tasks_file())})
        self.assert_dropped_keys_deleted()

    def test_missing_ignored_tasks_file_fails(self):
        with self.assertRaisesRegex(Exception, "does not exist"):
            self.cli("prepare", "dag_before", f"--ignored-tasks={self.pid.parent / 'missing.txt'}")

    def test_implicit_ignored_tasks_file_is_used(self):
        self.prepare_with_all_keys()
        self.implicit_ignored_tasks_file()
        self.garbage_collect_all_keys_dag()
        self.assert_dropped_keys_deleted()

    def test_explicit_ignored_tasks_overrides_implicit_file(self):
        self.prepare_with_all_keys()
        self.implicit_ignored_tasks_file()
        empty_file = self.pid.parent / "empty.tsv"
        empty_file.write_text("")
        self.garbage_collect_all_keys_dag(f"--ignored-tasks={empty_file}")
        self.assert_nothing_deleted()

    def test_implicit_ignored_tasks_symlink(self):
        self.prepare_with_all_keys()
        (self.pid / "drypipe-ignored-tasks.tsv").symlink_to(self.ignored_tasks_file())
        self.garbage_collect_all_keys_dag()
        self.assert_dropped_keys_deleted()

    def test_broken_implicit_ignored_tasks_symlink_fails(self):
        self.prepare_with_all_keys()
        (self.pid / "drypipe-ignored-tasks.tsv").symlink_to(self.pid.parent / "missing.tsv")
        with self.assertRaisesRegex(Exception, "broken symlink"):
            self.garbage_collect_all_keys_dag()

    def test_ignored_array_children_are_removed_from_task_keys_of_prepared_array(self):
        self.cli("prepare", "simple_array_pipeline")
        ignored_tasks_file = self.pid.parent / "ignored-tasks.tsv"
        ignored_tasks_file.write_text("t01\n")
        self.cli("prepare", "simple_array_pipeline", f"--ignored-tasks={ignored_tasks_file}")
        task_keys = (self.pid / ".drypipe" / "ap" / "task-keys.tsv").read_text().split()
        self.assertEqual(task_keys, [f"t{i:02d}" for i in range(2, 12)])

    def ignore_all_array_children(self, *extra_keys):
        ignored_tasks_file = self.pid.parent / "ignored-tasks.tsv"
        ignored_tasks_file.write_text("".join(f"{key}\n" for key in [*(f"t{i:02d}" for i in range(1, 12)), *extra_keys]))
        return f"--ignored-tasks={ignored_tasks_file}"

    def test_array_without_children_fails(self):
        with self.assertRaisesRegex(Exception, "has no children tasks"):
            self.cli("garbage-collect", "dag_with_empty_array", "--no-confirm")

    def test_ignoring_all_array_children_fails(self):
        with self.assertRaisesRegex(Exception, "all children tasks of slurm array parent task ap are ignored"):
            self.cli("garbage-collect", "simple_array_pipeline", "--no-confirm", self.ignore_all_array_children())

    def test_ignoring_array_parent_and_all_its_children(self):
        self.cli("prepare", "simple_array_pipeline", self.ignore_all_array_children("ap"))
        self.assertFalse((self.pid / ".drypipe" / "ap").exists())
