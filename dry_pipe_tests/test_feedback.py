import io
import os
import unittest
import unittest.mock

from dry_pipe.cli import print_table
from dry_pipe.feedback import CliFeedback, focal, done


class TerminalStream(io.StringIO):

    def isatty(self):
        return True


class CliFeedbackTests(unittest.TestCase):

    def written(self, write, stream=None, **kwargs):
        stream = stream or io.StringIO()
        write(CliFeedback(stream, **kwargs))
        return stream.getvalue()

    def test_focal_span_keeps_the_color_of_its_line(self):
        out = self.written(lambda f: f.danger(f"delete {focal(24)} output files ?"), color=True)
        self.assertEqual(out, "\033[31mdelete \033[1m24\033[22m output files ?\033[39m\n")

    def test_warning_colors_its_prefix_only(self):
        out = self.written(lambda f: f.warn("no tasks to submit"), color=True)
        self.assertEqual(out, "\033[33mwarning:\033[39m no tasks to submit\n")

    def test_styles_are_removed_when_not_a_terminal(self):
        out = self.written(lambda f: f.danger(f"delete {focal(24)} output files ?"))
        self.assertEqual(out, "delete 24 output files ?\n")

    def test_terminal_is_colored(self):
        with unittest.mock.patch.dict(os.environ, {"TERM": "xterm"}):
            os.environ.pop("NO_COLOR", None)
            out = self.written(lambda f: f.done("deleted 3 output files"), TerminalStream())
        self.assertEqual(out, "\033[32mdeleted 3 output files\033[39m\n")

    def test_no_color_and_dumb_terminal_are_not_colored(self):
        for env in [{"NO_COLOR": "1"}, {"TERM": "dumb"}]:
            with self.subTest(env), unittest.mock.patch.dict(os.environ, env):
                out = self.written(lambda f: f.done("deleted 3 output files"), TerminalStream())
                self.assertEqual(out, "deleted 3 output files\n")

    def confirm(self, answer, **kwargs):
        stream = io.StringIO()
        with unittest.mock.patch("sys.stdin", io.StringIO(answer)):
            confirmed = CliFeedback(stream).confirm("delete them ?", **kwargs)
        return confirmed, stream.getvalue()

    def test_confirm(self):
        for answer, default, expected in [
            ("y\n", False, True), ("yes\n", False, True), ("n\n", True, False),
            ("\n", False, False), ("\n", True, True), ("", True, True), ("x\n", True, False)
        ]:
            with self.subTest(answer=answer, default=default):
                confirmed, prompt = self.confirm(answer, default=default)
                self.assertEqual(confirmed, expected)
                self.assertEqual(prompt, f"delete them ? {'[Y/n]' if default else '[y/N]'} ")


class PrintTableTests(unittest.TestCase):

    rows = [["state", "count"], ["completed", "3"], ["ready", "12"]]

    @staticmethod
    def style_of_completed(column_index, cell):
        return done if cell == "completed" else None

    def test_cells_are_styled_after_alignment(self):
        out = TerminalStream()
        with unittest.mock.patch.dict(os.environ, {"TERM": "xterm"}):
            os.environ.pop("NO_COLOR", None)
            print_table(self.rows, out, self.style_of_completed)
        self.assertEqual(out.getvalue(), "state      count\n\033[32mcompleted\033[39m      3\nready         12\n")

    def test_cells_are_not_styled_when_not_a_terminal(self):
        out = io.StringIO()
        print_table(self.rows, out, self.style_of_completed)
        self.assertEqual(out.getvalue(), "state\tcount\ncompleted\t3\nready\t12\n")


all_tests = [
    CliFeedbackTests,
    PrintTableTests
]
