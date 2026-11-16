"""
feedback roles, each one a style:

    colors tell what kind of message it is: warning, error, danger, done
    intensity tells how important a part of it is: focal, context

the two use different terminal attributes, so a focal span keeps its line's color:

    feedback.danger(f"delete {focal(24)} output files ?")
"""

import os
import re
import sys


def _style(on, off):
    def style(text):
        return f"\033[{on}m{text}\033[{off}m"
    return style


focal = _style("1", "22")
context = _style("2", "22")
warning = _style("33", "39")
error = _style("31", "39")
danger = _style("31", "39")
done = _style("32", "39")


class CliFeedback:
    """
    messages to the user (progress, warnings, errors, prompts) on stderr,
    stdout being reserved for the data output of commands
    """

    def __init__(self, stream=None, color=None):
        self._stream = stream
        self._color = color

    @property
    def stream(self):
        # resolved when writing, tests patch sys.stderr after the Cli is created
        return self._stream or sys.stderr

    def is_colored(self):
        if self._color is not None:
            return self._color
        return self.stream.isatty() and not os.environ.get("NO_COLOR") and os.environ.get("TERM") != "dumb"

    def _write(self, message, end="\n"):
        text = str(message)
        if not self.is_colored():
            text = re.sub(r"\033\[[0-9;]*m", "", text)
        print(text, end=end, file=self.stream, flush=True)

    def info(self, message):
        self._write(message)

    def context(self, message):
        self._write(context(message))

    def done(self, message):
        self._write(done(message))

    def danger(self, message):
        self._write(danger(message))

    def warn(self, message):
        self._write(f"{warning('warning:')} {message}")

    def error(self, message):
        self._write(f"{error('error:')} {message}")

    def dry_run(self, message):
        self.context(f"DRY RUN {message}")

    def ask(self, prompt):
        self._write(prompt, end="")
        return sys.stdin.readline().strip()

    def confirm(self, question, default=False, dangerous=False):
        styled_question = danger(question) if dangerous else focal(question)
        answer = self.ask(f"{styled_question} {'[Y/n]' if default else '[y/N]'} ").lower()
        if answer == "":
            return default
        return answer in ("y", "yes")
