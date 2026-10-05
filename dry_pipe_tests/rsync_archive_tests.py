import contextlib
import os
import shutil
import subprocess
import time
import unittest
from io import StringIO
from pathlib import Path
from unittest import mock

from dry_pipe import DryPipe
from dry_pipe.cli import Cli


MODULE = "dry_pipe_tests.rsync_archive_tests"

KEYS = [f"t{i:02d}" for i in range(1, 7)]

UNUSUAL_NAMES = [
    "with space.txt",
    "unicodé-ß.txt",
    "-leading-dash.txt",
    "star*.txt",
    "question?.txt",
    "bracket[1].txt",
    "hash#semi;colon.txt",
]

SCALE_TASK_COUNT = 20000


@DryPipe.python_call()
def noop():
    pass


def rsync_dag(dsl):
    for key in KEYS:
        yield dsl.task(key=key).outputs(
            report=dsl.file(f"{key}_report.txt", tags=["keepers"]),
            both=dsl.file("both.tsv", tags=["keepers", "heavy"]),
            debug=dsl.file("report-all.tsv", tags=["heavy", "for-debug"]),
            plain=dsl.file("plain.tsv"),
        ).calls(noop)()


def unusual_names_dag(dsl):
    yield dsl.task(key="u").outputs(**{
        f"f{i}": dsl.file(name, tags=["keepers"])
        for i, name in enumerate(UNUSUAL_NAMES)
    }).calls(noop)()


def newline_in_output_name_dag(dsl):
    yield dsl.task(key="n").outputs(f=dsl.file("new\nline.txt", tags=["keepers"])).calls(noop)()


def newline_in_input_name_dag(dsl):
    yield dsl.task(key="n").inputs(f=dsl.file("/data/new\nline.txt")).calls(noop)()


def carriage_return_in_output_name_dag(dsl):
    yield dsl.task(key="n").outputs(f=dsl.file("new\rline.txt")).calls(noop)()


def newline_in_key_dag(dsl):
    yield dsl.task(key="n\n1").outputs(f=dsl.file("f.txt")).calls(noop)()


def tab_in_key_dag(dsl):
    yield dsl.task(key="n\t1").outputs(f=dsl.file("f.txt")).calls(noop)()


def drypipe_lookalike_names_dag(dsl):
    yield dsl.task(key="l").outputs(
        a=dsl.file("out.log", tags=["keepers"]),
        b=dsl.file("task-conf.json", tags=["keepers"]),
        c=dsl.file("state.completed", tags=["keepers"]),
    ).calls(noop)()


def scale_dag(dsl):
    for i in range(SCALE_TASK_COUNT):
        yield dsl.task(key=f"s{i:05d}").outputs(
            report=dsl.file(f"s{i:05d}_report.txt", tags=["keepers"]),
            debug=dsl.file("report-all.tsv", tags=["for-debug"]),
        ).calls(noop)()


def keys_filter(*keys):
    def f(key, state_name, step):
        return key in keys
    return f


def keepers(key):
    return [f"{key}_report.txt", "both.tsv"]


def heavy(key):
    return ["both.tsv", "report-all.tsv"]


def for_debug(key):
    return ["report-all.tsv"]


def all_outputs(key):
    return [f"{key}_report.txt", "both.tsv", "report-all.tsv", "plain.tsv"]


def output_paths(keys, *names_funcs):
    return {
        f"output/{key}/{name}"
        for key in keys
        for names in names_funcs
        for name in names(key)
    }


class RsyncArchiveTestCase(unittest.TestCase):

    generator = "rsync_dag"

    def setUp(self):
        sandbox = Path(__file__).parent / "sandboxes" / f"{self.__class__.__name__}.{self._testMethodName}"
        if sandbox.exists():
            shutil.rmtree(sandbox)
        sandbox.mkdir(parents=True)
        self.sandbox = sandbox
        self.pid = sandbox / "pid"
        self.dest = sandbox / "dest"

    def cli(self, command, *args, pid=None, global_args=()):
        out = StringIO()
        Cli(
            [
                *global_args,
                command,
                f"--pipeline-instance-dir={pid or self.pid}",
                f"--generator={MODULE}:{self.generator}",
                *args
            ],
            test_mode=True,
            output=out
        ).invoke()
        return out.getvalue()

    def prepare(self):
        self.cli("prepare")

    def rsync(self, *args, confirm=False, global_args=()):
        no_confirm = [] if confirm else ["--no-confirm"]
        return self.cli("rsync-push", f"--dest={self.dest}/", *no_confirm, *args, global_args=global_args)

    def control_dir(self, key, root=None):
        return (root or self.pid) / ".drypipe" / key

    def set_state(self, key, state):
        control_dir = self.control_dir(key)
        state_files = list(control_dir.glob("state.*"))
        self.assertEqual(len(state_files), 1, f"expected a single state file in {control_dir}")
        state_files[0].rename(control_dir / f"state.{state}")

    def write_outputs(self, key, names):
        d = self.pid / "output" / key
        d.mkdir(parents=True, exist_ok=True)
        for name in names:
            (d / name).write_text(f"{key}/{name}")

    def complete(self, keys, names=all_outputs):
        for key in keys:
            self.set_state(key, "completed")
            self.write_outputs(key, names(key))

    def dest_files(self, root=None):
        root = root or self.dest
        return {
            p.relative_to(root).as_posix()
            for p in root.rglob("*")
            if p.is_file()
        }

    def dest_output_files(self):
        return {f for f in self.dest_files() if f.startswith("output/")}

    def state_files(self, key, root):
        return sorted(p.name for p in self.control_dir(key, root).glob("state.*"))

    def fake_rsync_on_path(self, script_body):
        bin_dir = self.sandbox / "bin"
        bin_dir.mkdir()
        script = bin_dir / "rsync"
        script.write_text(f"#!/bin/sh\n{script_body}\n")
        script.chmod(0o755)
        return mock.patch.dict(os.environ, {"PATH": f"{bin_dir}:{os.environ['PATH']}"})

    def ignore(self, *lines, file=None):
        file = file or self.pid / "drypipe-task-set.rules"
        (file.parent / "ignored.tsv").write_text("".join(f"{line}\n" for line in lines))
        file.write_text("- @ignored.tsv\n")
        return file

    def assert_single_state_file_equal_to_source(self, key):
        self.assertEqual(self.state_files(key, self.dest), self.state_files(key, self.pid))
        self.assertEqual(len(self.state_files(key, self.dest)), 1)


class RsyncSelectionTests(RsyncArchiveTestCase):

    def test_single_tag(self):
        self.prepare()
        self.complete(KEYS)
        self.rsync("--tags=keepers")
        self.assertEqual(self.dest_files(), output_paths(KEYS, keepers))

    def test_many_tags_select_the_union(self):
        self.prepare()
        self.complete(KEYS)
        self.rsync("--tags=keepers,for-debug")
        self.assertEqual(self.dest_files(), output_paths(KEYS, keepers, for_debug))

    def test_no_tags_selects_all_file_outputs_including_untagged(self):
        self.prepare()
        self.complete(KEYS)
        self.rsync()
        self.assertEqual(self.dest_files(), output_paths(KEYS, all_outputs))

    def test_file_with_many_matching_tags_is_copied_once_and_counted_for_each(self):
        self.prepare()
        self.complete(KEYS)
        out = self.rsync("--tags=keepers,heavy")
        self.assertEqual(self.dest_files(), output_paths(KEYS, keepers, heavy))
        self.assertIn("  keepers: 12\n", out)
        self.assertIn("  heavy: 12\n", out)
        self.assertIn("total output files: 18\n", out)

    def test_file_contents_are_copied(self):
        self.prepare()
        self.complete(KEYS)
        self.rsync()
        for f in output_paths(KEYS, all_outputs):
            self.assertEqual((self.dest / f).read_text(), (self.pid / f).read_text())

    def test_missing_files_are_skipped_and_others_copied(self):
        self.prepare()
        self.complete(KEYS)
        (self.pid / "output" / "t02" / "t02_report.txt").unlink()
        shutil.rmtree(self.pid / "output" / "t05")
        self.rsync()
        self.assertEqual(
            self.dest_files(),
            output_paths(KEYS, all_outputs) - {"output/t02/t02_report.txt"} - output_paths(["t05"], all_outputs)
        )

    def test_filters_select_exactly_the_files_of_matching_tasks(self):

        def prepare_tasks_in_various_states():
            self.prepare()
            self.complete(KEYS)
            self.set_state("t04", "failed.1")
            self.set_state("t05", "ready")
            self.set_state("t06", "waiting")

        cases = [
            (["--filter-completed"], ["t01", "t02", "t03"]),
            (["--filter-failed"], ["t04"]),
            (["--filter=t0[45]"], ["t04", "t05"]),
            (["--py-filter={key} in ['t02', 't06']"], ["t02", "t06"]),
            ([f"--func-filter={MODULE}:keys_filter('t01', 't05')"], ["t01", "t05"]),
        ]

        for args, expected_keys in cases:
            with self.subTest(args=args):
                self.setUp()
                prepare_tasks_in_various_states()
                self.rsync("--tags=keepers", *args)
                self.assertEqual(self.dest_files(), output_paths(expected_keys, keepers))


class RsyncDrypipeFilesTests(RsyncArchiveTestCase):

    def add_extra_control_files(self, key):
        control_dir = self.control_dir(key)
        for name in ["out.log", "drypipe.log", "steps.sh", "output_vars"]:
            (control_dir / name).write_text(name)
        (control_dir / "sub").mkdir()
        (control_dir / "sub" / "nested.txt").write_text("nested")

    def test_minimal_copies_only_logs_and_state_file(self):
        self.prepare()
        self.complete(["t01"])
        self.add_extra_control_files("t01")
        self.rsync("--tags=keepers", "--filter=t01", "--include-drypipe-files=minimal")
        self.assertEqual(
            self.dest_files(),
            output_paths(["t01"], keepers) | {
                ".drypipe/t01/out.log",
                ".drypipe/t01/drypipe.log",
                ".drypipe/t01/state.completed",
            }
        )

    def test_all_copies_every_control_dir_file_recursively(self):
        self.prepare()
        self.complete(["t01"])
        self.add_extra_control_files("t01")
        self.rsync("--tags=keepers", "--filter=t01", "--include-drypipe-files")
        self.assertEqual(
            self.dest_files(),
            output_paths(["t01"], keepers) | {
                f".drypipe/t01/{name}"
                for name in [
                    "out.log", "drypipe.log", "steps.sh", "output_vars", "sub/nested.txt",
                    "state.completed", "task-conf.json"
                ]
            }
        )

    def test_minimal_rules_do_not_apply_to_output_files(self):
        self.generator = "drypipe_lookalike_names_dag"
        self.prepare()
        self.complete(["l"], names=lambda _: ["out.log", "task-conf.json", "state.completed"])
        self.rsync("--include-drypipe-files=minimal")
        self.assertEqual(
            self.dest_output_files(),
            {"output/l/out.log", "output/l/task-conf.json", "output/l/state.completed"}
        )

    def add_undeclared_output_files(self, key):
        self.write_outputs(key, ["undeclared.txt"])
        (self.pid / "output" / key / "scratch").mkdir(exist_ok=True)
        (self.pid / "output" / key / "scratch" / "partial.tsv").write_text("partial")

    def test_exhaustive_copies_every_file_of_output_and_control_dirs(self):
        self.prepare()
        self.complete(["t01"])
        self.add_extra_control_files("t01")
        self.add_undeclared_output_files("t01")
        self.rsync("--filter=t01", "--exhaustive")
        self.assertEqual(
            self.dest_files(),
            {
                f"output/t01/{name}"
                for name in [*all_outputs("t01"), "undeclared.txt", "scratch/partial.tsv"]
            } | {
                f".drypipe/t01/{name}"
                for name in [
                    "out.log", "drypipe.log", "steps.sh", "output_vars", "sub/nested.txt",
                    "state.completed", "task-conf.json"
                ]
            }
        )

    def test_exhaustive_cannot_be_combined_with_tags_or_drypipe_files(self):
        self.prepare()
        for args in [["--tags=keepers"], ["--include-drypipe-files"], ["--include-drypipe-files=minimal"]]:
            with self.subTest(args=args):
                with self.assertRaisesRegex(Exception, "--exhaustive cannot be combined"):
                    self.rsync("--exhaustive", *args)
        self.assertFalse(self.dest.exists())


class RsyncUnusualFileNamesTests(RsyncArchiveTestCase):

    def test_unusual_names_are_copied_literally(self):
        self.generator = "unusual_names_dag"
        self.prepare()
        self.complete(["u"], names=lambda _: UNUSUAL_NAMES)
        # decoys: would be copied if the names were interpreted as patterns
        self.write_outputs("u", ["starXYZ.txt", "questionX.txt", "bracket1.txt"])
        self.rsync()
        self.assertEqual(self.dest_files(), {f"output/u/{name}" for name in UNUSUAL_NAMES})

    def test_line_breaks_in_file_names_are_rejected_with_the_task_key(self):
        for generator, expected_error in [
            ("newline_in_output_name_dag", r"task\(n\)\.outputs\(f=\.\.\.\): file names can't contain line breaks"),
            ("newline_in_input_name_dag", r"task\(n\)\.inputs\(f=\.\.\.\): file names can't contain line breaks"),
            ("carriage_return_in_output_name_dag", r"task\(n\)\.outputs\(f=\.\.\.\): file names can't contain line breaks"),
        ]:
            with self.subTest(generator=generator):
                self.generator = generator
                with self.assertRaisesRegex(Exception, expected_error):
                    self.rsync()

    def test_line_breaks_and_tabs_in_task_keys_are_rejected(self):
        for generator, expected_error in [
            ("newline_in_key_dag", r"illegal character '\\n' in task key 'n\\n1'"),
            ("tab_in_key_dag", r"illegal character '\\t' in task key 'n\\t1'"),
        ]:
            with self.subTest(generator=generator):
                self.generator = generator
                with self.assertRaisesRegex(Exception, expected_error):
                    self.rsync()


class RsyncSummaryTests(RsyncArchiveTestCase):

    def test_summary_counts(self):
        self.prepare()
        self.complete(KEYS)
        out = self.rsync("--tags=keepers,kepers")
        self.assertIn("tasks: 6\n", out)
        self.assertIn("  keepers: 12\n", out)
        self.assertIn("  kepers: 0\n", out)
        self.assertIn("total output files: 12\n", out)
        self.assertEqual(len(self.dest_output_files()), 12)

    def test_summary_without_tags_shows_untagged(self):
        self.prepare()
        self.complete(KEYS)
        out = self.rsync()
        self.assertIn("  (untagged): 6\n", out)
        self.assertIn("  keepers: 12\n", out)
        self.assertIn("  heavy: 12\n", out)
        self.assertIn("  for-debug: 6\n", out)
        self.assertIn("total output files: 24\n", out)

    def test_summary_mentions_drypipe_files(self):
        self.prepare()
        out = self.rsync("--include-drypipe-files=minimal")
        self.assertIn("drypipe files (minimal) of 6 tasks\n", out)

    def test_answer_no_transfers_nothing(self):
        self.prepare()
        self.complete(KEYS)
        with mock.patch("builtins.input", return_value="n"):
            self.rsync(confirm=True)
        self.assertFalse(self.dest.exists())

    def test_empty_answer_defaults_to_no(self):
        self.prepare()
        self.complete(KEYS)
        with mock.patch("builtins.input", return_value=""):
            self.rsync(confirm=True)
        self.assertFalse(self.dest.exists())

    def test_answer_yes_transfers(self):
        self.prepare()
        self.complete(KEYS)
        with mock.patch("builtins.input", return_value="y"):
            self.rsync(confirm=True)
        self.assertEqual(self.dest_files(), output_paths(KEYS, all_outputs))


class RsyncExitCodeTests(RsyncArchiveTestCase):

    def fake_rsync_exiting_with(self, code):
        real_rsync = shutil.which("rsync")
        return self.fake_rsync_on_path(
            f'[ "$1" = "--version" ] && exec {real_rsync} --version\n'
            f"cat > /dev/null\n"
            f"exit {code}"
        )

    def test_exit_24_vanished_files_is_success(self):
        self.prepare()
        with self.fake_rsync_exiting_with(24):
            self.rsync()

    def test_other_non_zero_exits_raise(self):
        for code in [1, 23]:
            with self.subTest(code=code):
                self.setUp()
                self.prepare()
                with self.fake_rsync_exiting_with(code):
                    with self.assertRaises(subprocess.CalledProcessError):
                        self.rsync()

    def test_old_or_unknown_rsync_version_raises_before_transfer(self):
        version_outputs = [
            "rsync  version 3.1.2  protocol version 31",
            "rsync  version 3.2.6  protocol version 31",
            "openrsync: protocol version 29",
        ]
        for version_output in version_outputs:
            with self.subTest(version_output=version_output):
                self.setUp()
                self.prepare()
                self.complete(KEYS)
                with self.fake_rsync_on_path(f'echo "{version_output}"'):
                    with self.assertRaisesRegex(Exception, r"requires rsync >= 3\.2\.7"):
                        self.rsync()
                self.assertFalse(self.dest.exists())


class RsyncOptionsTests(RsyncArchiveTestCase):

    def rsync_args(self, *args, global_args=()):
        args_file = self.sandbox / "rsync-args"
        real_rsync = shutil.which("rsync")
        with self.fake_rsync_on_path(
            f'[ "$1" = "--version" ] && exec {real_rsync} --version\n'
            f'printf "%s\\n" "$@" > {args_file}\n'
            f"cat > /dev/null"
        ):
            self.rsync(*args, global_args=global_args)
        return args_file.read_text().splitlines()

    def test_stats_by_default(self):
        self.prepare()
        rsync_args = self.rsync_args()
        self.assertIn("--stats", rsync_args)
        self.assertNotIn("--info=progress2", rsync_args)
        self.assertNotIn("--dry-run", rsync_args)

    def test_info_progress2_replaces_stats(self):
        self.prepare()
        rsync_args = self.rsync_args("--info-progress2")
        self.assertIn("--info=progress2", rsync_args)
        self.assertNotIn("--stats", rsync_args)

    def test_dry_run_is_passed_to_rsync(self):
        self.prepare()
        self.assertIn("--dry-run", self.rsync_args(global_args=["--dry-run"]))

    def test_dry_run_transfers_nothing(self):
        self.prepare()
        self.complete(KEYS)
        self.rsync("--include-drypipe-files", global_args=["--dry-run"])
        self.assertFalse(self.dest.exists())


class RsyncStateFileTests(RsyncArchiveTestCase):

    def test_state_transitions_between_syncs_leave_a_single_state_file(self):
        for mode in ["minimal", "all"]:
            with self.subTest(mode=mode):
                self.setUp()
                self.prepare()
                for state in ["failed.2", "ready", "running", "completed"]:
                    self.set_state("t01", state)
                    self.rsync("--filter=t01", f"--include-drypipe-files={mode}")
                    self.assert_single_state_file_equal_to_source("t01")

    def test_duplicate_state_files_already_at_dest_are_repaired(self):
        self.prepare()
        self.complete(["t01"])
        dest_control_dir = self.control_dir("t01", self.dest)
        dest_control_dir.mkdir(parents=True)
        (dest_control_dir / "state.failed.1").touch()
        (dest_control_dir / "state.running").touch()
        self.rsync("--filter=t01", "--include-drypipe-files=minimal")
        self.assertEqual(self.state_files("t01", self.dest), ["state.completed"])

    def test_full_then_minimal_keeps_a_single_state_file_and_excluded_files(self):
        self.prepare()
        self.set_state("t01", "failed.1")
        self.rsync("--filter=t01", "--include-drypipe-files")
        self.set_state("t01", "completed")
        self.rsync("--filter=t01", "--include-drypipe-files=minimal")
        self.assert_single_state_file_equal_to_source("t01")
        self.assertTrue((self.control_dir("t01", self.dest) / "task-conf.json").exists())

    def test_sync_without_drypipe_files_leaves_dest_drypipe_untouched(self):
        self.prepare()
        self.set_state("t01", "failed.1")
        self.rsync("--filter=t01", "--include-drypipe-files=minimal")
        drypipe_files_before = {f for f in self.dest_files() if f.startswith(".drypipe/")}
        self.set_state("t01", "completed")
        self.rsync("--filter=t01")
        self.assertEqual({f for f in self.dest_files() if f.startswith(".drypipe/")}, drypipe_files_before)
        self.assertEqual(self.state_files("t01", self.dest), ["state.failed.1"])

    def test_tasks_outside_a_later_filter_keep_their_old_state(self):
        self.prepare()
        self.complete(["t01"])
        self.set_state("t02", "failed.1")
        self.rsync("--include-drypipe-files=minimal")
        self.set_state("t02", "running")
        self.rsync("--filter-completed", "--include-drypipe-files=minimal")
        self.assertEqual(self.state_files("t02", self.dest), ["state.failed.1"])

    def test_delete_spares_everything_but_the_listed_control_dirs(self):
        self.prepare()
        self.complete(KEYS)
        dest_only_files = [
            "output/t01/dest_only.txt",
            "output/t02/dest_only.txt",
            ".drypipe/t02/state.failed.1",
            ".drypipe/t99/state.completed",
            ".drypipe/conf.json",
            ".drypipe/instance.log",
            "unrelated/file.txt",
        ]
        for f in dest_only_files:
            (self.dest / f).parent.mkdir(parents=True, exist_ok=True)
            (self.dest / f).write_text("dest only")

        self.rsync("--filter=t01", "--include-drypipe-files=minimal")

        self.assertTrue(set(dest_only_files) <= self.dest_files())
        self.assertEqual(self.state_files("t01", self.dest), ["state.completed"])

    def test_state_renamed_between_listing_and_transfer(self):
        self.prepare()
        self.set_state("t01", "running")
        real_rsync = shutil.which("rsync")
        control_dir = self.control_dir("t01")
        with self.fake_rsync_on_path(
            f'[ "$1" = "--version" ] || mv {control_dir}/state.running {control_dir}/state.completed\n'
            f'exec {real_rsync} "$@"'
        ):
            self.rsync("--filter=t01", "--include-drypipe-files=minimal")
        self.assertEqual(self.state_files("t01", self.dest), ["state.completed"])


class RsyncIgnoredTasksTests(RsyncArchiveTestCase):

    def dest_files_with_contents(self, key):
        return {
            f: (self.dest / f).read_text()
            for f in self.dest_files()
            if f.startswith((f"output/{key}/", f".drypipe/{key}/"))
        }

    def test_newly_ignored_task_is_left_untouched_at_dest(self):
        for mode in ["minimal", "all"]:
            with self.subTest(mode=mode):
                self.setUp()
                self.prepare()
                self.complete(KEYS)
                self.set_state("t02", "failed.1")
                self.rsync(f"--include-drypipe-files={mode}")
                t02_at_dest_before = self.dest_files_with_contents("t02")

                self.ignore("t02")
                self.set_state("t02", "completed")
                self.write_outputs("t02", ["t02_report.txt"])
                (self.pid / "output" / "t02" / "t02_report.txt").write_text("changed after being ignored")
                (self.pid / "output" / "t02" / "both.tsv").unlink()
                self.rsync(f"--include-drypipe-files={mode}")

                self.assertEqual(self.dest_files_with_contents("t02"), t02_at_dest_before)
                self.assertEqual(self.state_files("t02", self.dest), ["state.failed.1"])
                self.assert_single_state_file_equal_to_source("t01")

    def test_ignored_tasks_are_excluded(self):

        def implicit_file():
            self.ignore("t02", "t05\tobsolete")
            return [], {}

        def option():
            file = self.ignore("t02", "t05\tobsolete", file=self.sandbox / "task-set.rules")
            return [f"--task-set={file}"], {}

        def env_var():
            file = self.ignore("t02", "t05\tobsolete", file=self.sandbox / "task-set.rules")
            return [], {"DRYPIPE_TASK_SET": str(file)}

        def option_wins_over_implicit_file():
            self.ignore("t01")
            return option()

        for ignore_source in [implicit_file, option, env_var, option_wins_over_implicit_file]:
            with self.subTest(ignore_source=ignore_source.__name__):
                self.setUp()
                self.prepare()
                self.complete(KEYS)
                args, env = ignore_source()
                with mock.patch.dict(os.environ, env):
                    self.rsync("--tags=keepers", *args)
                self.assertEqual(self.dest_files(), output_paths(["t01", "t03", "t04", "t06"], keepers))

    def test_summary_reports_ignored_and_unmatched_keys(self):
        self.prepare()
        self.complete(KEYS)
        self.ignore("t02", "t03_obsolete", "t_04")
        out = self.rsync()
        self.assertIn("tasks: 5\n", out)
        self.assertIn(f"ignored tasks: 1 (from {self.pid / 'drypipe-task-set.rules'})\n", out)
        self.assertIn("keys of task set files not yielded by the generator: 2 (ex: 't03_obsolete', 't_04')\n", out)

    def test_summary_without_ignore_file_has_no_ignore_lines(self):
        self.prepare()
        out = self.rsync()
        self.assertNotIn("ignored", out)


class RsyncRoundTripTests(RsyncArchiveTestCase):

    def list_tasks(self, pid):
        out = StringIO()
        with contextlib.redirect_stdout(out):
            self.cli("list", pid=pid)
        return sorted(out.getvalue().splitlines())

    def test_dest_lists_the_same_keys_and_states_as_source(self):
        self.prepare()
        self.complete(["t01", "t02", "t03"])
        self.set_state("t04", "failed.2")
        self.set_state("t05", "ready")
        self.rsync("--include-drypipe-files=minimal")
        self.assertEqual(self.list_tasks(self.dest), self.list_tasks(self.pid))


class RsyncPullTests(RsyncArchiveTestCase):

    def test_pulls_tagged_files_pushed_from_another_instance(self):
        self.prepare()
        self.complete(KEYS)
        self.rsync()

        puller_pid = self.sandbox / "puller"
        self.cli("prepare", pid=puller_pid)
        self.cli("rsync-pull", f"--source={self.dest}/", "--tags=keepers", "--no-confirm", pid=puller_pid)

        self.assertEqual(
            {f for f in self.dest_files(puller_pid) if f.startswith("output/")},
            output_paths(KEYS, keepers)
        )

    def test_state_files_only_renames_local_state_files_according_to_remote(self):
        self.prepare()
        self.complete(["t01", "t02"])
        self.set_state("t03", "failed.2")
        self.rsync("--include-drypipe-files=minimal")

        puller_pid = self.sandbox / "puller"
        self.cli("prepare", pid=puller_pid)
        local_log = self.control_dir("t01", puller_pid) / "out.log"
        local_log.write_text("local")

        def non_state_files():
            return {f for f in self.dest_files(puller_pid) if "/state." not in f}

        other_local_files = non_state_files()

        self.cli("rsync-pull", f"--source={self.dest}/", "--state-files-only", "--no-confirm", pid=puller_pid)

        for key in KEYS:
            self.assertEqual(self.state_files(key, puller_pid), self.state_files(key, self.dest))
        self.assertEqual(non_state_files(), other_local_files)
        self.assertEqual(local_log.read_text(), "local")

    def test_state_files_only_cannot_be_combined_with_tags(self):
        with self.assertRaises(Exception):
            self.cli("rsync-pull", f"--source={self.dest}/", "--state-files-only", "--tags=keepers", "--no-confirm")


class ArchiveTests(RsyncArchiveTestCase):

    def archive(self, *args, name=None, confirm=False, global_args=()):
        no_confirm = [] if confirm else ["--no-confirm"]
        return self.cli("archive", f"--name={name or self.archive_file}", *no_confirm, *args, global_args=global_args)

    @property
    def archive_file(self):
        return self.sandbox / "archive.tar.gz"

    def extract(self, archive_file=None):
        self.dest.mkdir()
        subprocess.run(["tar", "-xf", archive_file or self.archive_file, "-C", self.dest], check=True)

    def staging_dirs(self):
        return list((self.pid / ".drypipe").glob("archive-staging-*"))

    def test_archives_selected_files_and_minimal_drypipe_files(self):
        self.prepare()
        self.complete(KEYS)
        out = self.archive("--tags=keepers", "--filter=t01", "--include-drypipe-files=minimal")
        self.extract()
        self.assertEqual(self.dest_files(), output_paths(["t01"], keepers) | {".drypipe/t01/state.completed"})
        self.assertIn(f"archive: {self.archive_file}\n", out)

    def test_file_contents_are_archived(self):
        self.prepare()
        self.complete(KEYS)
        self.archive()
        self.extract()
        for f in output_paths(KEYS, all_outputs):
            self.assertEqual((self.dest / f).read_text(), (self.pid / f).read_text())

    def test_custom_archive_command(self):
        self.prepare()
        self.complete(KEYS)
        tar_file = self.sandbox / "archive.tar"
        self.archive("--tags=heavy", "--filter=t02", "--archive-command=tar -cf {archive} .", name=tar_file)
        self.extract(tar_file)
        self.assertEqual(self.dest_files(), output_paths(["t02"], heavy))

    def test_name_with_space_and_relative_to_cwd(self):
        self.prepare()
        self.complete(KEYS)
        with contextlib.chdir(self.sandbox):
            self.archive("--filter=t01", name="my archive.tar.gz")
        self.extract(self.sandbox / "my archive.tar.gz")
        self.assertEqual(self.dest_files(), output_paths(["t01"], all_outputs))

    def test_files_are_staged_as_hard_links(self):
        self.prepare()
        self.complete(KEYS)
        link_count_file = self.sandbox / "link-count.txt"
        self.archive(
            "--filter=t01", "--archive-command=stat -c %h output/t01/both.tsv > {archive}", name=link_count_file
        )
        self.assertEqual(link_count_file.read_text().strip(), "2")

    def test_dry_run_prints_command_and_creates_no_archive(self):
        self.prepare()
        self.complete(KEYS)
        out = self.archive(global_args=["--dry-run"])
        self.assertIn(f"DRY RUN tar -czf {self.archive_file} .\n", out)
        self.assertFalse(self.archive_file.exists())

    def test_exhaustive_archives_undeclared_outputs_and_control_files(self):
        self.prepare()
        self.complete(KEYS)
        (self.pid / "output" / "t01" / "undeclared.txt").write_text("undeclared")
        self.archive("--filter=t01", "--exhaustive")
        self.extract()
        self.assertEqual(
            self.dest_files(),
            output_paths(["t01"], all_outputs) | {
                "output/t01/undeclared.txt", ".drypipe/t01/state.completed", ".drypipe/t01/task-conf.json"
            }
        )

    def test_staging_dir_is_removed(self):

        def succeeding():
            self.archive()

        def failing_archive_command():
            with self.assertRaises(subprocess.CalledProcessError):
                self.archive("--archive-command=false")

        def declined():
            with mock.patch("builtins.input", return_value="n"):
                self.archive(confirm=True)
            self.assertFalse(self.archive_file.exists())

        for scenario in [succeeding, failing_archive_command, declined]:
            with self.subTest(scenario=scenario.__name__):
                self.setUp()
                self.prepare()
                self.complete(KEYS)
                scenario()
                self.assertEqual(self.staging_dirs(), [])

    def test_exclude_tags_archives_files_having_none_of_them(self):
        self.prepare()
        self.complete(KEYS)
        out = self.archive("--exclude-tags=heavy,for-dbug", "--filter=t01")
        self.extract()
        self.assertEqual(self.dest_files(), {"output/t01/t01_report.txt", "output/t01/plain.tsv"})
        self.assertIn("excluded tags: for-dbug, heavy\n", out)
        self.assertIn("total output files: 2\n", out)

    def test_exclude_tags_cannot_be_combined_with_tags_or_exhaustive(self):
        self.prepare()
        for args, expected_error in [
            (["--tags=keepers"], "--tags cannot be combined with --exclude-tags"),
            (["--exhaustive"], "--exhaustive cannot be combined"),
        ]:
            with self.subTest(args=args):
                with self.assertRaisesRegex(Exception, expected_error):
                    self.archive("--exclude-tags=heavy", *args)
        self.assertFalse(self.archive_file.exists())


class PurgeOutputsTests(RsyncArchiveTestCase):

    def purge(self, *args, confirm=False, global_args=()):
        no_confirm = [] if confirm else ["--no-confirm"]
        return self.cli("purge-outputs", *no_confirm, *args, global_args=global_args)

    def output_files(self):
        return {f for f in self.dest_files(self.pid) if f.startswith("output/")}

    def test_tags_select_the_purged_files(self):
        self.prepare()
        self.complete(KEYS)
        out = self.purge("--tags=heavy")
        self.assertEqual(self.output_files(), output_paths(KEYS, all_outputs) - output_paths(KEYS, heavy))
        self.assertIn("deleted 12 output files\n", out)

    def test_exclude_tags_spare_the_files_having_any_of_them(self):
        self.prepare()
        self.complete(KEYS)
        self.purge("--exclude-tags=keepers")
        self.assertEqual(self.output_files(), output_paths(KEYS, keepers))

    def test_only_files_of_matching_tasks_are_purged(self):
        self.prepare()
        self.complete(KEYS)
        self.purge("--filter=t01")
        self.assertEqual(self.output_files(), output_paths(KEYS[1:], all_outputs))

    def test_undeclared_outputs_and_drypipe_files_are_spared(self):
        self.prepare()
        self.complete(["t01"])
        self.write_outputs("t01", ["undeclared.txt"])
        drypipe_files_before = self.dest_files(self.pid / ".drypipe")
        self.purge()
        self.assertEqual(self.output_files(), {"output/t01/undeclared.txt"})
        self.assertEqual(self.dest_files(self.pid / ".drypipe"), drypipe_files_before)

    def test_missing_files_are_skipped(self):
        self.prepare()
        self.complete(["t01"], names=keepers)
        out = self.purge()
        self.assertEqual(self.output_files(), set())
        self.assertIn("deleted 2 output files\n", out)

    def test_dry_run_lists_and_deletes_nothing(self):
        self.prepare()
        self.complete(["t01"])
        out = self.purge("--tags=for-debug", global_args=["--dry-run"])
        self.assertIn(f"DRY RUN rm {self.pid / 'output/t01/report-all.tsv'}\n", out)
        self.assertEqual(self.output_files(), output_paths(["t01"], all_outputs))

    def test_answer_no_deletes_nothing(self):
        self.prepare()
        self.complete(KEYS)
        with mock.patch("builtins.input", return_value=""):
            out = self.purge(confirm=True)
        self.assertIn("total output files: 24\n", out)
        self.assertEqual(self.output_files(), output_paths(KEYS, all_outputs))

    def test_warns_only_when_no_tag_spec_selects_all_outputs(self):
        self.prepare()
        for args, warned in [([], True), (["--tags=heavy"], False), (["--exclude-tags=keepers"], False)]:
            with self.subTest(args=args):
                out = self.purge(*args, global_args=["--dry-run"])
                self.assertEqual("ALL output files of matching tasks will be deleted" in out, warned)

    def test_tags_cannot_be_combined_with_exclude_tags(self):
        self.prepare()
        self.complete(KEYS)
        with self.assertRaisesRegex(Exception, "--tags cannot be combined with --exclude-tags"):
            self.purge("--tags=heavy", "--exclude-tags=keepers")
        self.assertEqual(self.output_files(), output_paths(KEYS, all_outputs))


class ArchiveThenPurgeRoundTripTests(RsyncArchiveTestCase):

    def output_files_with_contents(self):
        return {
            f: (self.pid / f).read_bytes()
            for f in self.dest_files(self.pid)
            if f.startswith("output/")
        }

    def archived_files(self, archive_file):
        tar = subprocess.run(["tar", "-tf", archive_file], capture_output=True, text=True, check=True)
        return {Path(name).as_posix() for name in tar.stdout.splitlines()}

    def archive_purge_and_untar(self, *selection_args, archive_args=()):
        """returns the purged files, after asserting that untarring the archive over the purged instance restores it"""
        archive_file = self.sandbox / "archive.tar.gz"
        before = self.output_files_with_contents()

        self.cli("archive", f"--name={archive_file}", "--no-confirm", *archive_args, *selection_args)
        self.cli("purge-outputs", "--no-confirm", *selection_args)
        purged = before.keys() - self.output_files_with_contents().keys()

        subprocess.run(["tar", "-xf", archive_file, "-C", self.pid], check=True)
        self.assertEqual(self.output_files_with_contents(), before)
        return purged, self.archived_files(archive_file)

    def test_selections_restore_the_instance(self):
        for selection_args, archive_args, expected_purged in [
            ([], [], output_paths(KEYS, all_outputs)),
            (["--tags=heavy"], [], output_paths(KEYS, heavy)),
            (["--tags=keepers,for-debug"], [], output_paths(KEYS, keepers, for_debug)),
            (["--exclude-tags=keepers"], [], output_paths(KEYS, all_outputs) - output_paths(KEYS, keepers)),
            (["--exclude-tags=heavy,for-debug"], [], output_paths(KEYS, lambda key: [f"{key}_report.txt", "plain.tsv"])),
            (["--tags=heavy", "--filter=t01"], [], output_paths(["t01"], heavy)),
            (["--tags=keepers"], ["--include-drypipe-files=minimal"], output_paths(KEYS, keepers)),
            ([], ["--exhaustive"], output_paths(KEYS, all_outputs)),
        ]:
            with self.subTest(selection_args=selection_args, archive_args=archive_args):
                self.setUp()
                self.prepare()
                self.complete(KEYS)
                purged, _ = self.archive_purge_and_untar(*selection_args, archive_args=archive_args)
                self.assertEqual(purged, expected_purged)

    def test_missing_outputs(self):
        self.prepare()
        self.complete(KEYS, names=keepers)
        purged, _ = self.archive_purge_and_untar()
        self.assertEqual(purged, output_paths(KEYS, keepers))

    def test_unusual_file_names(self):
        self.generator = "unusual_names_dag"
        self.prepare()
        self.complete(["u"], names=lambda _: UNUSUAL_NAMES)
        purged, _ = self.archive_purge_and_untar("--tags=keepers")
        self.assertEqual(purged, {f"output/u/{name}" for name in UNUSUAL_NAMES})

    def test_ignored_tasks_are_neither_archived_nor_purged(self):
        self.prepare()
        self.complete(KEYS)
        self.ignore("t02")
        purged, archived = self.archive_purge_and_untar()
        self.assertEqual(purged, output_paths(KEYS, all_outputs) - output_paths(["t02"], all_outputs))
        self.assertFalse(any(f.startswith("output/t02") for f in archived))


@unittest.skipUnless(
    os.environ.get("DRYPIPE_TEST_RSYNC_REMOTE_DEST"),
    "set DRYPIPE_TEST_RSYNC_REMOTE_DEST=user@host:/abs/dir to run"
)
class RsyncRemoteDestTests(RsyncArchiveTestCase):

    def remote_state_files(self, user_at_host, remote_dir, key):
        ls = subprocess.run(
            ["ssh", user_at_host, f"ls {remote_dir}/.drypipe/{key}"],
            capture_output=True, text=True, check=True
        )
        return sorted(name for name in ls.stdout.split() if name.startswith("state."))

    def test_state_transitions_leave_a_single_state_file_at_remote_dest(self):
        remote_dest = os.environ["DRYPIPE_TEST_RSYNC_REMOTE_DEST"]
        user_at_host, remote_dir = remote_dest.split(":", 1)
        subprocess.run(["ssh", user_at_host, f"rm -rf {remote_dir}"], check=True)
        self.dest = remote_dest
        self.prepare()
        for state in ["failed.2", "ready", "completed"]:
            self.set_state("t01", state)
            self.cli(
                "rsync-push", f"--dest={remote_dest}/", "--no-confirm",
                "--filter=t01", "--include-drypipe-files=minimal"
            )
            self.assertEqual(self.remote_state_files(user_at_host, remote_dir, "t01"), [f"state.{state}"])


@unittest.skipUnless(os.environ.get("DRYPIPE_SLOW_TESTS"), "set DRYPIPE_SLOW_TESTS=1 to run")
class RsyncScaleTests(RsyncArchiveTestCase):

    generator = "scale_dag"

    def test_many_tasks(self):
        self.prepare()
        keys = [f"s{i:05d}" for i in range(SCALE_TASK_COUNT)]
        self.complete(keys, names=lambda key: [f"{key}_report.txt", "report-all.tsv"])

        start = time.time()
        out = self.rsync("--tags=keepers", "--filter-completed", "--include-drypipe-files=minimal")
        print(f"rsync of {SCALE_TASK_COUNT} tasks: {time.time() - start:.1f}s")

        self.assertIn(f"  keepers: {SCALE_TASK_COUNT}\n", out)
        self.assertEqual(self.dest_output_files(), {f"output/{key}/{key}_report.txt" for key in keys})
        self.assertTrue(all(self.state_files(key, self.dest) == ["state.completed"] for key in keys))
