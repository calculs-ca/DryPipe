import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from dry_pipe.cli import cli_argument_parser
from dry_pipe.cli_completion import completions

BASH_WORDBREAKS = " \t\n\"'@><=;|&(:"


class CliCompletionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.parser = cli_argument_parser()

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.chdir(tmp.name)

        Path("keys.tsv").write_text("k1\nk2\n")
        Path("sub").mkdir()
        Path("pid/.drypipe/arr").mkdir(parents=True)
        Path("pid/.drypipe/arr/array.1.tsv").touch()
        Path("failed.3-3.md").write_text(
            "| # | tasks | signature |\n|---:|---:|---|\n| 1 | 2 | `a` |\n| 2 | 1 | `b` |\n"
        )

    def completed(self, line):
        """the completions bash receives in COMPREPLY"""
        with unittest.mock.patch.dict(os.environ, clear=True):
            return sorted(completions(self.parser, line, BASH_WORDBREAKS))

    def test_command(self):
        self.assertEqual(self.completed("drypipe status"), ["status ", "status-db "])

    def test_option_names(self):
        self.assertEqual(
            self.completed("drypipe status --where-state"), ["--where-state ", "--where-state-not "]
        )

    def test_states_after_comma(self):
        self.assertEqual(self.completed("drypipe status --where-state failed,ti"), ["failed,timed-out "])

    def test_value_after_equals_replaces_what_follows_equals(self):
        self.assertEqual(self.completed("drypipe status --where-state=fai"), ["failed "])

    def test_choices(self):
        self.assertEqual(
            self.completed("drypipe rsync-push --drypipe-files "), ["all ", "logs ", "none ", "state "]
        )

    def test_key_list_files_keep_the_at_sign(self):
        self.assertEqual(
            self.completed("drypipe status --where-key k*,@"),
            ["@failed.3-3.md ", "@keys.tsv ", "@pid/", "@sub/"]
        )

    def test_signature_numbers_of_report(self):
        self.assertEqual(self.completed("drypipe status --where-key @failed.3-3.md:1,"), ["1,1 ", "1,2 "])

    def test_array_submissions(self):
        self.assertEqual(
            self.completed("drypipe array-submit -pid pid -k arr --where-key @array."),
            ["@array.1 ", "@array.last "]
        )

    def test_key_list_in_where_expression(self):
        self.assertEqual(self.completed('drypipe status --where "key in @ke'), ["key in @keys.tsv "])

    def test_dirs(self):
        self.assertEqual(self.completed("drypipe status -pid "), ["pid/", "sub/"])

    def test_analysis_file_positional(self):
        self.assertEqual(self.completed("drypipe extract-keys "), ["failed.3-3.md ", "pid/", "sub/"])

    def test_no_completion_for_globs(self):
        self.assertEqual(self.completed("drypipe status --where-key k"), [])


all_tests = [
    CliCompletionTests
]
