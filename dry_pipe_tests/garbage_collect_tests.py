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

dag_of_t1_t3 = dag_of(["t1", "t3"])


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

    def task_set_file(self, f=None):
        if f is None:
            f = self.pid.parent / "task-set.rules"
        (f.parent / "dropped.tsv").write_text("".join(f"{key}  \tsome reason\n\n" for key in DROPPED_KEYS))
        f.write_text("# drop obsolete tasks\n\n- @dropped.tsv\n")
        return f

    def implicit_task_set_file(self):
        return self.task_set_file(self.pid / "drypipe-task-set.rules")

    def rules_file(self, *rules):
        f = self.pid.parent / "task-set.rules"
        f.write_text("".join(f"{rule}\n" for rule in rules))
        return f"--task-set={f}"

    def test_later_rules_win(self):
        self.prepare_with_all_keys()
        (self.pid.parent / "kept.tsv").write_text("".join(f"{key}\n" for key in KEPT_KEYS))
        self.garbage_collect_all_keys_dag(self.rules_file("- t*", "+ @kept.tsv"))
        self.assert_dropped_keys_deleted()

    def test_glob_rules(self):
        self.prepare_with_all_keys()
        self.garbage_collect_all_keys_dag(self.rules_file("- t*", "+ t1", "+ t2"))
        self.assert_dropped_keys_deleted()

    def test_invalid_rule_fails(self):
        with self.assertRaisesRegex(Exception, "task-set.rules:2: expected"):
            self.cli("prepare", "dag_before", self.rules_file("- t1", "t2"))

    def test_missing_keys_file_of_rule_fails(self):
        with self.assertRaisesRegex(Exception, "task-set.rules:1: .*missing.tsv does not exist"):
            self.cli("prepare", "dag_before", self.rules_file("- @missing.tsv"))

    def test_ignored_tasks_are_not_prepared(self):
        self.cli("prepare", "dag_before", f"--task-set={self.task_set_file()}")
        self.assertEqual(self.task_dirs(".drypipe") & set(KEPT_KEYS + DROPPED_KEYS), set(KEPT_KEYS))

    def garbage_collect_all_keys_dag(self, *args, env=None):
        self.cli("garbage-collect", "dag_before", "--no-confirm", *args, env=env)

    def test_garbage_collect_purges_ignored_tasks(self):
        self.prepare_with_all_keys()
        self.garbage_collect_all_keys_dag(f"--task-set={self.task_set_file()}")
        self.assert_dropped_keys_deleted()

    def test_task_set_from_env_var(self):
        self.prepare_with_all_keys()
        self.garbage_collect_all_keys_dag(env={"DRYPIPE_TASK_SET": str(self.task_set_file())})
        self.assert_dropped_keys_deleted()

    def test_missing_task_set_file_fails(self):
        with self.assertRaisesRegex(Exception, "does not exist"):
            self.cli("prepare", "dag_before", f"--task-set={self.pid.parent / 'missing.rules'}")

    def test_implicit_task_set_file_is_used(self):
        self.prepare_with_all_keys()
        self.implicit_task_set_file()
        self.garbage_collect_all_keys_dag()
        self.assert_dropped_keys_deleted()

    def test_explicit_task_set_overrides_implicit_file(self):
        self.prepare_with_all_keys()
        self.implicit_task_set_file()
        empty_file = self.pid.parent / "empty.rules"
        empty_file.write_text("")
        self.garbage_collect_all_keys_dag(f"--task-set={empty_file}")
        self.assert_nothing_deleted()

    def test_implicit_task_set_symlink(self):
        self.prepare_with_all_keys()
        (self.pid / "drypipe-task-set.rules").symlink_to(self.task_set_file())
        self.garbage_collect_all_keys_dag()
        self.assert_dropped_keys_deleted()

    def test_broken_implicit_task_set_symlink_fails(self):
        self.prepare_with_all_keys()
        (self.pid / "drypipe-task-set.rules").symlink_to(self.pid.parent / "missing.rules")
        with self.assertRaisesRegex(Exception, "broken symlink"):
            self.garbage_collect_all_keys_dag()

    def test_ignored_array_children_are_removed_from_task_keys_of_prepared_array(self):
        self.cli("prepare", "simple_array_pipeline")
        self.cli("prepare", "simple_array_pipeline", self.rules_file("- t01"))
        task_keys = (self.pid / ".drypipe" / "ap" / "task-keys.tsv").read_text().split()
        self.assertEqual(task_keys, [f"t{i:02d}" for i in range(2, 12)])

    def ignore_all_array_children(self, *extra_keys):
        return self.rules_file("- t*", *(f"- {key}" for key in extra_keys))

    def test_array_without_children_fails(self):
        with self.assertRaisesRegex(Exception, "has no children tasks"):
            self.cli("garbage-collect", "dag_with_empty_array", "--no-confirm")

    def test_ignoring_all_array_children_fails(self):
        with self.assertRaisesRegex(Exception, "all children tasks of slurm array parent task ap are ignored"):
            self.cli("garbage-collect", "simple_array_pipeline", "--no-confirm", self.ignore_all_array_children())

    def test_ignoring_array_parent_and_all_its_children(self):
        self.cli("prepare", "simple_array_pipeline", self.ignore_all_array_children("ap"))
        self.assertFalse((self.pid / ".drypipe" / "ap").exists())

    def purge_tasks_not_in_task_set(self, generator, *args):
        return self.cli("purge-tasks-not-in-task-set", generator, "--no-confirm", *args)

    def test_purge_tasks_not_in_task_set_deletes_dirs_of_ignored_tasks(self):
        self.prepare_with_all_keys()
        out = self.purge_tasks_not_in_task_set("dag_before", f"--task-set={self.task_set_file()}")
        self.assert_dropped_keys_deleted()
        self.assertIn("4 directories of ignored tasks", out)

    def test_purge_tasks_not_in_task_set_keeps_ignored_tasks_not_yielded_by_the_generator(self):
        self.prepare_with_all_keys()
        self.purge_tasks_not_in_task_set("dag_after", f"--task-set={self.task_set_file()}")
        self.assert_nothing_deleted()

    def test_purge_tasks_not_in_task_set_keeps_tasks_not_yielded_by_the_generator(self):
        self.prepare_with_all_keys()
        self.purge_tasks_not_in_task_set("dag_of_t1_t3", self.rules_file("- t3"))
        self.assertEqual(self.task_dirs("output"), {"t1", "t2", "t4"})

    def test_purge_tasks_not_in_task_set_without_task_set_file_fails(self):
        self.prepare_with_all_keys()
        with self.assertRaisesRegex(Exception, "no ignored tasks"):
            self.purge_tasks_not_in_task_set("dag_before")
        self.assert_nothing_deleted()

    def test_purge_tasks_not_in_task_set_without_output_dirs(self):
        self.cli("prepare", "dag_before")
        out = self.purge_tasks_not_in_task_set("dag_before", f"--task-set={self.task_set_file()}")
        self.assertFalse(self.task_dirs(".drypipe") & set(DROPPED_KEYS))
        self.assertTrue(set(KEPT_KEYS) <= self.task_dirs(".drypipe"))
        self.assertIn("2 directories of ignored tasks", out)

    def test_purge_ignored_array_children(self):
        self.cli("prepare", "simple_array_pipeline")
        self.purge_tasks_not_in_task_set("simple_array_pipeline", self.rules_file("- t01", "- t02"))
        task_dirs = self.task_dirs(".drypipe")
        self.assertFalse(task_dirs & {"t01", "t02"})
        self.assertTrue({"ap", *(f"t{i:02d}" for i in range(3, 12))} <= task_dirs)
