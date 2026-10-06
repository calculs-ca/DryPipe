import functools
import logging
import os
import re
from pathlib import Path

from dry_pipe import TaskConf
from dry_pipe.reports import parse_timers_in_log
from dry_pipe.task_process import resolve_inputs_outputs

module_logger = logging.getLogger(__name__)


class TaskClassifier:
    """
    signature(key, out_log, drypipe_log, state, step, task_inputs, task_outputs, runtime_metrics) reduces a task
    to a string, tasks with equal signatures have the same kind of error, None leaves the task out.
    Missing logs are None. The default signature only looks at out_log.
    Customize by editing the lists (they are regexes), or by overriding methods in a subclass.
    """

    def __init__(self, error_words=None, stack_frames=None, noise_lines=None, masks=None, max_length=160):

        self.error_words = [
            r"error", r"exception", r"fatal", r"panic", r"traceback",
            r"segmentation fault", r"core dumped", r"abort", r"killed", r"\boom\b", r"out of memory",
            r"not enough", r"insufficient", r"denied", r"not found", r"no such",
            r"cannot", r"can't", r"could not", r"couldn't", r"unable to",
            r"fail", r"cancel", r"exceeded", r"timed? ?out", r"terminate called", r"assert",
            r"no space left", r"non-zero exit", r"exit (status|code) [1-9]", r"execution halted",
            r"invalid", r"illegal", r"unexpected", r"missing", r"corrupt",
        ] if error_words is None else list(error_words)

        self.stack_frames = [
            r"^\s*at\s",                                        # Java, JavaScript
            r"^\s*\.\.\. \d+ more",                             # Java
            r"^\s*File \"",                                     # Python
            r"^\s*[\^~]+\s*$",                                  # Python error markers
            r"^\s*Caused by:\s*$",                              # Java
            r"^\s*\d+:\s+\S",                                   # Rust backtrace, R traceback
            r"^goroutine \d+",                                  # Go
            r"^\s+\S+\.go:\d+",                                 # Go
            r"^#\d+\s+0x",                                      # gdb
        ] if stack_frames is None else list(stack_frames)

        # lines never taken as the error line, besides stack frames
        self.noise_lines = [
            r"^\++ ",                                           # bash xtrace
            r"\b0 (errors?|failures?|failed)\b",
            r"\b(errors?|failures?|failed)\s*[:=]\s*0\b",
            r"\bno errors?\b",
        ] if noise_lines is None else list(noise_lines)

        day = r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*"
        month = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*"

        # applied in order, case sensitive
        self.masks = [
            (r"\x1b\[[0-9;?]*[A-Za-z]", ""),                    # ANSI colors and cursor moves
            (r"[\x00-\x08\x0b-\x1f\x7f]", " "),                 # backspaces and other control chars of progress bars
            (rf"\b(?:{day},? )?{month} +\d+,?(?: \d{{4}})? [\d:.]+(?: ?[AP]M)?(?: [A-Z]{{3,4}}\b)?(?: \d{{4}})?", "<date>"),
            (r"\b\d{4}-\d{2}-\d{2}[T ][\d:.,]+(?:Z|[+-]\d{2}:?\d{2})?", "<date>"),
            (r"https?://\S+", "<url>"),
            (r"(?:[A-Za-z]:)?(?:[\w.+\-=@]*[/\\])+[\w.+\-=@]*", "<path>"),
            (r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b", "<uuid>"),
            (r"\b0x[0-9a-fA-F]+\b|\b(?=[0-9a-f]*\d)(?=[0-9a-f]*[a-f])[0-9a-f]{6,}\b", "<hex>"),
            (r"'[^']*'|\"[^\"]*\"|`[^`]*`|‘[^’]*’", "<str>"),
            (r"\d+(\.\d+)?", "<n>"),
            (r"(<\w+>[^\s<]{0,3})(\s*\1)+", r"\1…"),            # collapse repeats: "<n>% <n>% <n>%" -> "<n>%…"
            (r"^\s*<n>%…?\s*", ""),                             # progress output glued at the start of a line
            (r"\s+", " "),
        ] if masks is None else list(masks)

        self.max_length = max_length

    def compile(self):
        self._error_regex = re.compile("|".join(self.error_words), re.IGNORECASE)
        self._stack_frame_regex = re.compile("|".join(self.stack_frames))
        self._noise_regex = re.compile("|".join(self.noise_lines), re.IGNORECASE)
        self._mask_regexes = [(re.compile(regex), replacement) for regex, replacement in self.masks]
        return self

    def is_stack_frame(self, line):
        return self._stack_frame_regex.search(line) is not None

    def is_error_line(self, line):
        return (
            self._error_regex.search(line) is not None
            and not self.is_stack_frame(line)
            and self._noise_regex.search(line) is None
        )

    def mask(self, line, key):
        # a placeholder of word chars, so that a key inside a path gets masked along with the path
        line = line.replace(key, "__task_key__")
        for regex, replacement in self._mask_regexes:
            line = regex.sub(replacement, line)
        return line.replace("__task_key__", "<key>").strip()[:self.max_length]

    def is_meaningful(self, masked_line):
        """has at least one word outside of masks, a bare progress line like '<n>%…' is not meaningful"""
        return re.search(r"(?<!<)\b[A-Za-z]{3,}", masked_line) is not None

    def signature(self, key, out_log, drypipe_log, state, step, task_inputs, task_outputs, runtime_metrics):

        if out_log is None:
            return "<no out.log>"

        lines = [l for l in out_log.splitlines() if l.strip()]

        error_line = next((l for l in reversed(lines) if self.is_error_line(l)), None)
        if error_line is not None:
            return self.mask(error_line, key)

        return self.no_error_line_signature(lines, key)

    def no_error_line_signature(self, lines, key):

        def top_stack_frame():
            return next((l for l in lines if self.is_stack_frame(l)), None)

        def last_meaningful_line():
            masked_lines = (self.mask(l, key) for l in reversed(lines))
            return next((m for m in masked_lines if self.is_meaningful(m)), None)

        frame = top_stack_frame()
        if frame is not None:
            return "<no error line> frame: " + self.mask(frame, key)

        last = last_meaningful_line()
        if last is not None:
            return "<no error line> last: " + last

        return "<no error line>"


def lazy_task_inputs_outputs(control_dir):
    """
    (task_inputs, task_outputs) of a task, task-conf.json is loaded at the first attribute access of either.
    A file is a Path, a file_set a list of Path, a var its value, None if not produced yet.
    """

    task_key = os.path.basename(control_dir)

    @functools.cache
    def inputs_and_outputs():
        task_conf = TaskConf.from_json_file(control_dir)
        return resolve_inputs_outputs(task_conf, control_dir, False, module_logger)

    def input_value(task_input):
        if task_input.type == "file":
            return Path(task_input.resolved_value)
        if task_input.type == "file_set":
            upstream_control_dir = os.path.join(os.path.dirname(control_dir), task_input.upstream_task_key)
            _, upstream_outputs = lazy_task_inputs_outputs(upstream_control_dir)
            return getattr(upstream_outputs, task_input.name_in_upstream_task)
        return task_input.resolved_value

    def output_value(task_output):
        if task_output.type == "file":
            return Path(os.fspath(task_output))
        if task_output.type == "file_set":
            return list(task_output)
        return task_output._resolved_value

    return (
        LazyAttributes(lambda: inputs_and_outputs()[0], input_value, f"task {task_key} has no input"),
        LazyAttributes(lambda: inputs_and_outputs()[1], output_value, f"task {task_key} has no output")
    )


class LazyAttributes:

    def __init__(self, load_declared_by_name, value_of, missing_message):
        self._load_declared_by_name = load_declared_by_name
        self._value_of = value_of
        self._missing_message = missing_message

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        declared = self._load_declared_by_name().get(name)
        if declared is None:
            raise Exception(f"{self._missing_message} '{name}'")
        return self._value_of(declared)


class RuntimeMetrics:
    """
    read from drypipe.log at the first access, an unavailable metric is None.
    Only elapsed times are recorded today, see todo/task-runtime-metrics-recording.md
    """

    cpu_time = None
    max_rss = None
    cpus = None
    exit_code = None
    hostname = None

    def __init__(self, drypipe_log_file):
        self._drypipe_log_file = drypipe_log_file

    @functools.cached_property
    def _seconds_by_timer(self):
        if not Path(self._drypipe_log_file).exists():
            return {}
        # a restarted task logs its timers again, the last ones win
        return {label: float(seconds) for label, _, seconds in parse_timers_in_log(self._drypipe_log_file)}

    @property
    def elapsed(self):
        return self._seconds_by_timer.get("TASK")

    @property
    def elapsed_by_step(self):
        by_step = {
            int(label.removeprefix("STEP-")): seconds
            for label, seconds in self._seconds_by_timer.items()
            if label.startswith("STEP-")
        }
        return by_step or None
