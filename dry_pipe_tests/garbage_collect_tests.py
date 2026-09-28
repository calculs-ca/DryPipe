import shutil
import unittest
from io import StringIO
from pathlib import Path
from unittest import mock

from dry_pipe import DryPipe
from dry_pipe.cli import Cli


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


class GarbageCollectTests(unittest.TestCase):

    def setUp(self):
        sandbox = Path(__file__).parent / "sandboxes" / f"{self.__class__.__name__}.{self._testMethodName}"
        if sandbox.exists():
            shutil.rmtree(sandbox)
        sandbox.mkdir(parents=True)
        self.pid = sandbox / "pid"

    def cli(self, command, generator, *args):
        out = StringIO()
        Cli(
            [
                command,
                f"--pipeline-instance-dir={self.pid}",
                f"--generator={MODULE}:{generator}",
                *args
            ],
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
