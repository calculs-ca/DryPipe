import os
import tempfile
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


class TestAutoRestarter(TestWithDirectorySandbox):


    def test_auto_restarter(self):
        d = TestSandboxDir(self)


        os.mkdir(d.sandbox_dir)
        out_log = Path(d.sandbox_dir, "out.log")

        auto_restart_failed_regexen = {
            "drypipe.log": [".*BrokenPipeError.*"],
            "out.log": [".*Bus\\ error.*", None]
        }

        class AutoRestarter4Tests(AutoRestartManager):
            def restart_file(self, state_file):
                return Path(d.sandbox_dir, "restarts.tsv")


        class MockStateFile:
            def __init__(self):
                self.task_key = "t"

            def control_dir(self):
                return d.sandbox_dir

        ar = AutoRestarter4Tests(auto_restart_failed_regexen)

        with open(out_log, "w") as f:
            f.write("allo\n")
            f.write("123tdr ter t\n")

        self.assertEqual(
            list(ar.should_restart(MockStateFile())),
            list((False, None, 0))
        )

        with open(out_log, "w") as f:
            f.write("allo\n")
            f.write("123tdr ter t\n")
            f.write("123tdr Bus error ter t\n")

        self.assertEqual(
            list(ar.should_restart(MockStateFile())),
            list((True, 3, 0))
        )

        self.assertEqual(
            list(ar.should_restart(MockStateFile())),
            list((False, None, 1))
        )

        with open(out_log, "w") as f:
            f.write("allo\n")
            f.write("123tdr ter t\n")
            f.write("123tdr Bus error ter t\n")
            f.write("zzz\n")

        self.assertEqual(
            list(ar.should_restart(MockStateFile())),
            list((False, None))
        )
