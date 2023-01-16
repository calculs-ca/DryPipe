import glob
import os
import time
import pathlib
import mmap
import copy
from datetime import datetime
from itertools import groupby
from subprocess import TimeoutExpired

from dry_pipe.actions import TaskAction
from dry_pipe.script_lib import parse_in_out_meta, PortablePopen, load_pid, load_slurm_job_id, ps_resources

"""

 /x/y/z/.drypipe/<task_key>/state.<state_name>.<next_or_current_step>
 
 All state files of a pipeline can be fetched with f"{pipeline_instance_dir}.drypipe/*/state.*
 
 And the name of files can 
"""

VALID_TRANSITIONS_BY_JANITORS = {

    "":                    ["waiting-for-deps"],
    "waiting-for-deps":    ["prepared"],
    "prepared":            ["queued",             "queued-for-upload"],

    "queued-for-upload":   ["upload-started"],
    "upload-started":      ["upload-completed",   "upload-failed"],
    "upload-completed":    ["queued"],

    "queued-for-download": ["download-started"],
    "download-started":    ["download-completed", "download-failed"],
    "download-completed":  ["completed"],

    "upload-failed":       ["upload-started"],
    "download-failed":     ["download-started"],

    "completed-unsigned":  ["completed", "queued-for-download"]
}

VALID_TRANSITIONS_BY_TASK_WORKERS = {
    "queued":              ["launched",           "scheduled"],
    "launched":            ["step-started",       "crashed", "queued-for-download"],
    "scheduled":           ["step-started",       "crashed"],
    "step-started":        ["step-completed",     "crashed",            "failed",             "timed-out"],
    "step-completed":      ["completed-unsigned"]
}

VALID_TRANSITIONS_BY_TASK_ADMIN = {
    "crashed":             ["launched",           "scheduled",          "step-started"],
    "failed":              ["queued",             "ignored"],
    "timed-out":           ["queued"],
    "killed":              ["queued"],
    "completed":           ["waiting-for-deps"],
    "ignored":             ["waiting-for-deps"]
}

VALID_TRANSITIONS = {
    ** VALID_TRANSITIONS_BY_JANITORS,
    ** VALID_TRANSITIONS_BY_TASK_WORKERS,
    ** VALID_TRANSITIONS_BY_TASK_ADMIN
}

STATE_EQUIVALENCE_FOR_DISPLAY = {

    "waiting-for-deps":    "waiting",
    "prepared":            "waiting",
    "upload-failed":       "waiting",
    "download-failed":     "waiting",

    "launched":            "scheduled",
    "scheduled":           "scheduled",

    "download-started":    "transit",
    "upload-started":      "transit",
    "queued-for-upload":   "transit",
    "upload-completed":    "transit",
    "queued-for-download": "transit",
    "download-completed":  "transit",
    "queued":              "transit",
    "step-completed":      "transit",

    "completed-unsigned": "running",
    "step-started": "running",

    "completed":           "completed",

    "failed":              "failed",
    "timed-out":           "timed-out",
    "killed":              "killed",
#    "ignored":             "ignored",
    "crashed":             "failed"
}

STATE_EQUIVALENCE_FOR_DISPLAY_WEB = {

    "waiting-for-deps":    "waiting",
    "prepared":            "waiting",
    "upload-failed":       "waiting",
    "download-failed":     "waiting",
    "scheduled":           "waiting",
    "killed":              "killed",

    "launched":            "running",
    "completed-unsigned":  "running",
    "step-started":        "running",
    "download-started":    "running",
    "upload-started":      "running",
    "queued-for-upload":   "running",
    "upload-completed":    "running",
    "queued-for-download": "running",
    "download-completed":  "running",
    "queued":              "running",
    "step-completed":      "running",

    "completed":           "completed",

    "failed":              "failed",
    "timed-out":           "failed",
    "crashed":             "failed",
    "ignored":             "ignored"
}


NON_TERMINAL_STATES = [

    "prepared",
    "launched",
    "scheduled",
    "completed-unsigned",
    "step-started",
    "download-started",
    "upload-started",
    "queued-for-upload",
    "upload-completed",
    "queued-for-download",
    "download-completed",
    "queued",
    "step-completed"
]


class TaskState:

    @staticmethod
    def display_state_groups():

        return [
            (group_display_label, [t[0] for t in state_names_group_iterator])
            for group_display_label, state_names_group_iterator
            in groupby(STATE_EQUIVALENCE_FOR_DISPLAY.items(), key=lambda t: t[1])
        ]

    @staticmethod
    def state_display_names():
        return [
            display_name
            for display_name, state_names
            in TaskState.display_state_groups()
        ]

    state_display_names_web = list(set(STATE_EQUIVALENCE_FOR_DISPLAY_WEB.values()))

    @staticmethod
    def display_groups_of_state_names():
        return [
            list(state_names)
            for display_name, state_names
            in TaskState.display_state_groups()
        ]

    @staticmethod
    def fetch(pipeline, pattern, pipeline_instance_dir=None):

        if pipeline_instance_dir is None:
            pipeline_instance_dir = pipeline.pipeline_instance_dir

        for f in glob.glob(os.path.join(pipeline_instance_dir, f".drypipe/*/{pattern}")):
            yield TaskState(f)

    @staticmethod
    def from_task_control_dir(pipeline_instance_dir, task_key):

        state_file_pattern = os.path.join(pipeline_instance_dir, ".drypipe", task_key, "state.*")

        g = glob.glob(state_file_pattern)

        if len(g) == 0:
            return None

        return TaskState(g[0])

    @staticmethod
    def fetch_all(pipeline_instance_dir):
        return TaskState.fetch(None, "state.*", pipeline_instance_dir)

    @staticmethod
    def failed_task_states(pipeline_instance_dir):
        return TaskState.fetch(None, "state.failed.*", pipeline_instance_dir)

    @staticmethod
    def queued_for_upload_task_states(pipeline):
        return TaskState.fetch(pipeline, "state.queued-for-upload.*")

    @staticmethod
    def queued_for_dowload_task_states(pipeline):
        return TaskState.fetch(pipeline, "state.queued-for-download.*")

    @staticmethod
    def queued_for_remote_execution_task_states(pipeline):
        return TaskState.fetch(pipeline, "state.upload-completed.*")

    @staticmethod
    def queued_task_states(pipeline):
        return TaskState.fetch(pipeline, "state.queued.*")

    @staticmethod
    def prepared_task_states(pipeline):
        return TaskState.fetch(pipeline, "state.prepared.*")

    @staticmethod
    def waiting_for_deps_task_states(pipeline):
        return TaskState.fetch(pipeline, "state.waiting-for-deps")

    @staticmethod
    def count_running_local(pipeline):
        return sum([
            1 for f in glob.glob(os.path.join(pipeline.pipeline_instance_dir, f".drypipe/*/state.step-started*"))
        ])


    @staticmethod
    def completed_stale_task_states(pipeline):

        for f in glob.glob(os.path.join(pipeline.pipeline_instance_dir, f".drypipe/*/in.sig.stale")):
            task_control_dir = os.path.dirname(f)
            state_file = os.path.join(task_control_dir, "state.completed")

            if not os.path.exists(state_file):
                raise Exception(f"file {f} should not exist given that task is not in state.completed")

            yield TaskState(state_file)

    @staticmethod
    def completed_unsigned_task_states(pipeline):
        return TaskState.fetch(pipeline, "state.completed-unsigned")

    @staticmethod
    def reset_stalled_transfers(pipeline):

        for task_state in TaskState.fetch(pipeline, "state.upload-started"):
            task_state.reset_upload()

        for task_state in TaskState.fetch(pipeline, "state.download-started"):
            task_state.reset_download()

    @staticmethod
    def create_non_existing(task_control_dir):

        p = pathlib.Path(os.path.join(task_control_dir, "state.waiting-for-deps"))

        p.touch(exist_ok=False)

        return TaskState(p.absolute())

    @staticmethod
    def parse_history_timestamp(s):
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f')

    def __init__(self, state_file_name_with_abs_path, step_number=None):
        self._state_file_name_with_abs_path = state_file_name_with_abs_path
        self._control_dir = os.path.dirname(state_file_name_with_abs_path)
        self.task_key = os.path.split(self._control_dir)[-1]
        self._base_file_name = os.path.basename(state_file_name_with_abs_path)

        name_parts = self._base_file_name.split(".")

        self.state_name = name_parts[1]

        if VALID_TRANSITIONS.get(self.state_name) is None:
            raise Exception(f"file {state_file_name_with_abs_path} has invalid state: '{self.state_name}'")
        elif self.is_waiting_for_deps() or self.is_completed() or self.is_completed_unsigned():
            self._step_number = 0
        elif len(name_parts) == 3:
            self._step_number = int(name_parts[-1])
        else:
            self._step_number = step_number

    def __repr__(self):
        return self.base_file_name()

    def pid(self):
        return load_pid(self.control_dir())

    def slurm_job_id(self):
        return load_slurm_job_id(self.control_dir())

    def gen_meta_dict(self):
        task_env = os.path.join(self.control_dir(), "task-env.sh")

        def gen():
            with open(task_env) as f:
                for line in f.readlines():
                    if line.startswith("export __meta_"):
                        yield tuple(line[7:].split("="))
                    elif line.startswith("export END_META"):
                        break

        return dict(gen())

    def is_all_deps_ready(self):

        pid = self.pipeline_instance_dir()
        for task_key, _, _ in parse_in_out_meta(self.gen_meta_dict()):
            if task_key == "":
                continue
            task_state = TaskState.from_task_control_dir(pid, task_key)
            if not task_state.is_completed():
                return False

        return True

    def pipeline_instance_dir(self):
        return os.path.dirname(os.path.dirname(self.control_dir()))

    def base_file_name(self):
        return self._base_file_name

    def abs_file_name(self):
        return self._state_file_name_with_abs_path

    def control_dir(self):
        return self._control_dir

    def is_prepared(self):
        return self.state_name == "prepared"

    def is_queued(self):
        return self.state_name == "queued"

    def is_launched(self):
        return self.state_name == "launched"

    def is_scheduled(self):
        return self.state_name == "scheduled"

    def is_completed(self):
        return self.state_name == "completed"

    def is_failed(self):
        return self.state_name == "failed"

    def is_timed_out(self):
        return self.state_name == "timed-out"

    def is_crashed(self):
        return self.state_name == "crashed"

    def is_waiting_for_deps(self):
        return self.state_name == "waiting-for-deps"

    def is_step_started(self):
        return self.state_name == "step-started"

    def is_step_completed(self):
        return self.state_name == "step-completed"

    def is_completed_unsigned(self):
        return self.state_name == "completed-unsigned"

    def is_killed(self):
        return self.state_name == "killed"

    def is_input_signature_changed(self):
        return os.path.exists(os.path.join(self._control_dir, "in.sig.changed"))

    def step_number(self):
        return self._step_number

    def tail_err_if_failed(self, n):

        if self.is_failed():
            err_file = os.path.join(self._control_dir, "err.log")
            return tail(err_file, n)
        else:
            return None

    def tail_out_err_drypipe(self):
        tails = [
            tail(os.path.join(self.control_dir(), d), 20)
            for d in ["out.log", "err.log", "drypipe.log"]
        ]

        return tuple(tails)


    def _ensure_transition_valid(self, next_state):
        valid_next_states = VALID_TRANSITIONS.get(self.state_name)

        if valid_next_states is None:
            raise Exception(f"task {self.task_key} is in unknown state {self.state_name}")

        if next_state not in valid_next_states:
            raise Exception(f"task {self.task_key} can't transition from '{self.state_name}' to '{next_state}'")

    def _transition(self, next_state, suffix="", col_4="", force=False):

        if not force:
            self._ensure_transition_valid(next_state)

        last_update_time = datetime.fromtimestamp(os.stat(self._state_file_name_with_abs_path).st_ctime)

        state_to_dump = self.state_name

        if self.state_name == "completed-unsigned":
            state_to_dump = "completed"

        history_line = [
            state_to_dump,
            last_update_time.strftime('%Y-%m-%dT%H:%M:%S.%f'),
            "", #str(getattr(self, "_step_number", "")),
            col_4
        ]

        with open(os.path.join(self._control_dir, "history.tsv"), "a") as f:
            f.write("\t".join(history_line)+"\n")

        next_file = os.path.join(self._control_dir, f"state.{next_state}{suffix}")
        os.rename(
            os.path.join(self._control_dir, self._base_file_name),
            next_file
        )

        return next_file

    def _step_and_retry_suffix(self, step=None):
        s = step if step is not None else self._step_number
        return f".{s}"

    def reset_upload(self):
        self._transition("queued-for-upload", ".0.0.3", force=True)

    def reset_download(self):
        self._transition("queued-for-download", ".0.0.3", force=True)

    def transition_to_waiting_for_deps(self):
        self._transition("waiting-for-deps")

    def transition_to_prepared(self, force=False):
        #TODO: eliminate prepared state
        self._transition("prepared", f".{self.step_number()}", force=force)

        from dry_pipe import Task
        task = Task.load_from_task_state(self)
        task_state = task.get_state()
        if task.is_remote():
            task_state.transition_to_queued_remote_upload()
        else:
            task_state.transition_to_queued()

    def transition_to_queued_remote_upload(self):
        self._transition("queued-for-upload", ".0.0.3")

    def transition_to_queued_remote_download(self, pipeline_instance_dir, task_key):

        state_file_expr = os.path.join(pipeline_instance_dir, ".drypipe", task_key, "state.*")

        for f in glob.glob(state_file_expr):
            task_state = TaskState(f)

            task_state._transition("queued-for-download", ".0.0.3", force=True)
            return

        raise Exception(f"expected state file {state_file_expr} to exist")

    def assign_remote_state_to_local_state_file(self, pipeline_instance_dir, remote_task_state):

        control_dir = os.path.join(pipeline_instance_dir, ".drypipe", remote_task_state.task_key)

        state_file_expr = os.path.join(control_dir, "state.*")

        for local_state_file in glob.glob(state_file_expr):

            if local_state_file != os.path.join(control_dir, remote_task_state.base_file_name()):
                os.rename(
                    local_state_file,
                    os.path.join(control_dir, remote_task_state.base_file_name())
                )
            return

        raise Exception(f"expected state file {state_file_expr} to exist")


    def transition_to_queued(self, increment_retry=True, step=None, force=False):

        if self.is_failed():
            c = copy.copy(self)
            c._transition("queued", c._step_and_retry_suffix(step), force=force)
        else:
            self._transition("queued", self._step_and_retry_suffix(step), force=force)

    def transition_to_killed(self):
        self._transition("killed", self._step_and_retry_suffix(), force=True)

    def transition_to_launched(self, executer, task, wait_for_completion=False, fail_silently=False):
        self._transition("launched", self._step_and_retry_suffix())
        task.launch(executer, wait_for_completion, fail_silently=fail_silently)

    def transition_to_completed(self, task):
        task.verify_output_files_produced()
        task.write_output_signature_file()
        self._transition("completed")

    def transition_to_upload_completed(self):
        return TaskState(self._transition("upload-completed"), 0)

    def transition_to_upload_started(self):
        return TaskState(self._transition("upload-started"))

    def transition_to_download_completed(self):
        return TaskState(self._transition("download-completed"), 0)

    def transition_to_download_started(self):
        return TaskState(self._transition("download-started"))

    def action_if_exists(self):
        return TaskAction.load_from_task_control_dir(self._control_dir)

    def history_steps_time_intervals(self):

        rows = [
            (int(r[2]), r[0], TaskState.parse_history_timestamp(r[1]))
            for r in self.load_history_rows()
            if r[0] in ["step-started", "step-completed"]
        ]

        for step, rows in groupby(rows, key=lambda t: t[0]):
            rows = list(rows)
            step_started = [s[2] for s in rows if s[1] == "step-started"]
            step_started = None if len(step_started) == 0 else step_started[0]
            step_completed = [s[2] for s in rows if s[1] == "step-completed"]
            step_completed = None if len(step_completed) == 0 else step_completed[0]
            yield rows[0][0], step_started, step_completed


    def load_history_rows(self, history_tsv_as_string=None):

        if history_tsv_as_string is None:
            h = os.path.join(self._control_dir, "history.tsv")
            if os.path.exists(h):
                with open(h) as f:
                    history_tsv_as_string = f.read()
            else:
                history_tsv_as_string = ""

        for line in history_tsv_as_string.split("\n"):
            if line != "":
                yield line.split("\t")

    def validate_history(self):

        rows = list(self.load_history_rows())

        for i in range(0, len(rows) - 1):
            state1 = rows[i][0]
            state2 = rows[i + 1][0]
            if state1 not in VALID_TRANSITIONS:
                yield f"invalid state: {state1}"
            else:
                possible_next_states = VALID_TRANSITIONS[state1]
                if state2 not in possible_next_states:
                    yield f"{state1} can't be followed by {state2}"

    def as_json(self, timeout=None):

        def file_content(file):
            f = os.path.join(self.control_dir(), file)
            if not os.path.exists(f):
                return None

            n = 1000

            return f"tail -{n}\n {tail(f, lines=n, timeout=timeout)}"

        action = TaskAction.load_from_task_state(self)

        action = None if action is None else action.action_name

        snapshot_time = int(time.time_ns())

        def _ps_resources():
            pid_file = os.path.join(self.control_dir(), "pid")
            if os.path.exists(pid_file):
                with open(pid_file) as f:
                    return [
                        list(t)
                        for t in ps_resources(int(f.read()))
                    ]

        return {
            'key': self.task_key,
            'state': self.state_name,
            'step': self.step_number(),
            'out': file_content("out.log"),
            'err': file_content("err.log"),
            'drypipe_log': file_content("drypipe.log"),
            'ps': _ps_resources(),
            'history': list(self.load_history_rows()),
            'action': action,
            'snapshot_time': snapshot_time
        }


def tail(filename, lines=20, line_limit=1000, timeout=None):

    if not os.path.exists(filename):
        return ""

    cmd = f"tail -{lines} {filename}"

    with PortablePopen(cmd.split(" ")) as p:
        try:
            p.wait_and_raise_if_non_zero(timeout)
        except TimeoutExpired:
            return f"tail command timed out: {cmd}"

        def read():
            for line in p.read_stdout_lines():
                if len(line) <= line_limit:
                    yield line
                else:
                    yield f"{line[0:line_limit]}...(line was truncated as it's length exceeded {line_limit} chars)\n"

        return "".join(read())
