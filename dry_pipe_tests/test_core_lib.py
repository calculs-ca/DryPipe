import os
import shutil
import tempfile
import unittest
from pathlib import Path

from base_pipeline_test import TestWithDirectorySandbox
from dry_pipe.core_lib import expandvars_from_dict, read_last_step_lines, reversed_lines, is_step_banner, \
    read_out_log_last_step, read_drypipe_log_last_step
from dry_pipe import AutoRestartManager
from test_utils import TestSandboxDir, DummyLogger


class TestExpandVars(unittest.TestCase):


    def test(self):

        # Replace with os.environ as desired
        envars = {"foo": "bar", "baz": "$Baz"}

        tests = {r"foo": r"foo",
                 r"$foo": r"bar",
                 r"$$": r"$$",  # This could be considered a bug
                 r"$$foo": r"$bar",  # This could be considered a bug
                 r"\n$foo\r": r"nbarr",  # This could be considered a bug
                 r"$bar": r"",
                 r"$baz": r"$Baz",
                 r"bar$foo": r"barbar",
                 r"$foo$foo": r"barbar",
                 r"$foobar": r"",
                 r"$foo bar": r"bar bar",
                 r"$foo-Bar": r"bar-Bar",
                 r"$foo_Bar": r"",
                 r"${foo}bar": r"barbar",
                 r"baz${foo}bar": r"bazbarbar",
                 r"foo\$baz": r"foo$baz",
                 r"foo\\$baz": r"foo\$Baz",
                 r"\$baz": r"$baz",
                 r"\\$foo": r"\bar",
                 r"\\\$foo": r"\$foo",
                 r"\\\\$foo": r"\\bar",
                 r"\\\\\$foo": r"\\$foo"}

        for t, v in tests.items():
            g = expandvars_from_dict(t, envars)
            self.assertEqual(g, v)


class TestReadLastStepLines(unittest.TestCase):

    def _out_log(self, text):
        tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tmp_dir.cleanup)
        out_log = Path(tmp_dir.name, "out.log")
        out_log.write_text(text, newline="")
        return out_log

    def test_reversed_lines_span_blocks(self):
        # 200 chars lines span more than one of the 8192 bytes blocks
        lines = [f"{i:03d} {'x' * 195}\r\n" for i in range(60)] + ["no newline at the end"]
        out_log = self._out_log("".join(lines))
        self.assertEqual(list(reversed_lines(out_log)), [l.encode() for l in reversed(lines)])

    def test_last_step_starts_at_its_banner(self):
        out_log = self._out_log(
            "================ t1: step-1.sh ====================\n"
            "Error: old\n"
            "\n================ step 1 restarted after failure =====================\n\n"
            "================ t1: step-1.sh ====================\n"
            "ok\n"
        )
        self.assertEqual(read_out_log_last_step(out_log, 1000), b"================ t1: step-1.sh ====================\nok\n")

    def test_restart_banner_is_not_a_step_start(self):
        out_log = self._out_log(
            "================ t1: step-1.sh ====================\n"
            "Error: old\n"
            "\n================ step 1 restarted after failure =====================\n\n"
        )
        self.assertTrue(read_out_log_last_step(out_log, 1000).startswith(b"================ t1: step-1.sh"))

    def test_drypipe_log_last_step_starts_at_its_timer(self):
        drypipe_log = self._out_log(
            "2026-10-07 INFO START_TIMER_FOR:STEP-0\n"
            "2026-10-07 INFO TIME_ELAPSED_FOR:STEP-0: 00:00:01, 1.0\n"
            "2026-10-07 INFO START_TIMER_FOR:STEP-1\n"
            "2026-10-07 INFO killed\n"
        )
        self.assertEqual(
            read_drypipe_log_last_step(drypipe_log, 100),
            b"2026-10-07 INFO START_TIMER_FOR:STEP-1\n2026-10-07 INFO killed\n"
        )

    def test_long_last_step_keeps_its_banner(self):
        banner = "================ mod:func(1,{}) ====================\n"
        lines = [f"{i}\n" for i in range(10)]
        out_log = self._out_log("before\n" + banner + "".join(lines))
        self.assertEqual(read_last_step_lines(out_log, is_step_banner, 3), (banner + "".join(lines[-3:])).encode())

    def test_no_banner(self):
        out_log = self._out_log("a\nb\nc\n")
        self.assertEqual(read_last_step_lines(out_log, is_step_banner, 2), b"b\nc\n")


class MockStateFileForAutoRestartManager:
    def __init__(self, mock_control_dir, auto_restart_condition_regexp_per_log_file):
        self.task_key = "t"
        self.mock_control_dir = mock_control_dir
        self.out_log = Path(self.mock_control_dir, "out.log")
        self.drypipe_log = Path(self.mock_control_dir, "drypipe.log")
        self.auto_restart_condition_regexp_per_log_file = auto_restart_condition_regexp_per_log_file
        shutil.rmtree(mock_control_dir, ignore_errors=True)
        os.mkdir(mock_control_dir)

    def control_dir(self):
        return self.mock_control_dir

    def _write(self, lines, mode, log_file):
        with open(log_file, mode) as f:
            for line in lines:
                f.write(f"{line}\n")

    def reset_with_lines(self, log_file, *lines):
        self._write(lines, "w", log_file)

    def append_lines(self, log_file, *lines):
        self._write(lines, "a", log_file)

    def auto_restart_manager(self):
        d = self.mock_control_dir
        class AutoRestarter4Tests(AutoRestartManager):
            def restart_file(self, state_file):
                return Path(d, "restarts.tsv")
        return AutoRestarter4Tests(self.auto_restart_condition_regexp_per_log_file)

class TestAutoRestarter(TestWithDirectorySandbox):


    def test_auto_restarter_basics(self):
        d = TestSandboxDir(self)

        msf = MockStateFileForAutoRestartManager(d.sandbox_dir, {
            "drypipe.log": [".*BrokenPipeError.*"],
            "out.log": [".*Bus\\ error.*", None]
        })

        ar = msf.auto_restart_manager()

        msf.reset_with_lines(
            msf.out_log,
            "allo",
            "123tdr ter t"
        )

        logger = DummyLogger()

        should_restart, matching_line_number, restart_count, matching_log_filename = \
            ar.should_restart_with_details(msf, logger)

        self.assertEqual(
            [should_restart, matching_line_number, restart_count],
            [False,          None,                 0]
        )

        msf.reset_with_lines(
            msf.out_log,
            "allo",
            "123tdr ter t",
            "123tdr Bus error ter t"
        )

        should_restart, matching_line_number, restart_count, matching_log_filename = \
            ar.should_restart_with_details(msf, logger)

        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [True,           3,                    0,             "out.log"]
        )

        # untouched log, should be treated as absent log ?
        should_restart, matching_line_number, restart_count, matching_log_filename = \
            ar.should_restart_with_details(msf, logger)

        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [False,          None,                 1,             None]
        )

        msf.append_lines(msf.out_log, "nothing")

        should_restart, matching_line_number, restart_count, matching_log_filename = \
            ar.should_restart_with_details(msf, logger)

        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [False,          None,                 1,             None]
        )

        msf.reset_with_lines(
            msf.drypipe_log,
            "ergtert123",
            "aaaa BrokenPipeError 123 b"
        )

        should_restart, matching_line_number, restart_count, matching_log_filename = \
            ar.should_restart_with_details(msf, logger)

        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [True,           2,                    1,             "drypipe.log"]
        )

class TestAutoRestarterMissingLogFileRestarts(TestWithDirectorySandbox):

    def setUp(self):
        d = TestSandboxDir(self)

        self.msf = MockStateFileForAutoRestartManager(d.sandbox_dir, {
            "drypipe.log": [".*BrokenPipeError.*"],
            "out.log": [".*Bus\\ error.*", None]
        })

        self.ar = self.msf.auto_restart_manager()


class TestAutoRestarterMissingLogFileRestartsAtMostOnce(TestAutoRestarterMissingLogFileRestarts):

    def test(self):

        ar = self.ar
        msf = self.msf

        logger = DummyLogger()

        should_restart, matching_line_number, restart_count, matching_log_filename = \
            self.ar.should_restart_with_details(msf, logger)

        # should restart when out.log is missing
        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [True,           None,                 0,             "out.log"]
        )

        should_restart, matching_line_number, restart_count, matching_log_filename = \
            self.ar.should_restart_with_details(msf, logger)

        # but should restart only once for this reason
        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [False,          None,                 1,             None]
        )

class TestAutoRestarterMissingLogFileRestartsOnlySpecifiedFile(TestAutoRestarterMissingLogFileRestarts):

    def test(self):

        ar = self.ar
        msf = self.msf

        msf.append_lines(msf.out_log, "nothing")

        logger = DummyLogger()

        should_restart, matching_line_number, restart_count, matching_log_filename = \
            self.ar.should_restart_with_details(msf, logger)


        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [False,          None,                 0,             None]
        )


all_tests = [
    TestExpandVars,
    TestAutoRestarter,
    TestAutoRestarterMissingLogFileRestartsAtMostOnce,
    TestAutoRestarterMissingLogFileRestartsOnlySpecifiedFile
]