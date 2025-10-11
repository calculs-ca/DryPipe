import os
import shutil
import unittest
from pathlib import Path

from base_pipeline_test import TestWithDirectorySandbox
from dry_pipe.core_lib import expandvars_from_dict
from dry_pipe.slurm_array_task import AutoRestartManager
from test_utils import TestSandboxDir


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


        should_restart, matching_line_number, restart_count, matching_log_filename = ar.should_restart_with_details(msf)

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

        should_restart, matching_line_number, restart_count, matching_log_filename = ar.should_restart_with_details(msf)

        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [True,           3,                    0,             "out.log"]
        )

        # untouched log, should be treated as absent log ?
        should_restart, matching_line_number, restart_count, matching_log_filename = ar.should_restart_with_details(msf)

        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [False,          None,                 1,             None]
        )

        msf.append_lines(msf.out_log, "nothing")

        should_restart, matching_line_number, restart_count, matching_log_filename = ar.should_restart_with_details(msf)

        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [False,          None,                 1,             None]
        )

        msf.reset_with_lines(
            msf.drypipe_log,
            "ergtert123",
            "aaaa BrokenPipeError 123 b"
        )

        should_restart, matching_line_number, restart_count, matching_log_filename = ar.should_restart_with_details(msf)

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

        should_restart, matching_line_number, restart_count, matching_log_filename = self.ar.should_restart_with_details(msf)

        # should restart when out.log is missing
        self.assertEqual(
            [should_restart, matching_line_number, restart_count, matching_log_filename],
            [True,           None,                 0,             "out.log"]
        )

        should_restart, matching_line_number, restart_count, matching_log_filename = ar.should_restart_with_details(msf)

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

        should_restart, matching_line_number, restart_count, matching_log_filename = ar.should_restart_with_details(msf)


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