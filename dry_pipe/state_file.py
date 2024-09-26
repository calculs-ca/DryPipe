import fnmatch
import os
from pathlib import Path


class StateFile:

    def __init__(self, task_key, current_hash_code, tracker, path=None, slurm_array_id=None):
        self.tracker = tracker
        self.task_key = task_key
        if path is not None:
            self.path = path
        else:
            self.path = os.path.join(tracker.pipeline_work_dir, task_key, "state.waiting")
        self.hash_code = current_hash_code
        self.inputs = None
        self.outputs = None
        self.is_slurm_array_child = False
        if slurm_array_id is not None:
            self.slurm_array_id = slurm_array_id
        self.is_parent_task = False

    def __repr__(self):
        return f"/{self.task_key}/{os.path.basename(self.path)}"

    def refresh(self, new_path):
        assert self.path != new_path
        self.path = new_path
        if self.path.endswith("state.completed"):
            # load from file:
            self.outputs = None
            self.inputs = None

    def transition_to_pre_launch(self, reset_failed=False):
        _, _, s = self.key_state_step()

        if reset_failed:
            s = 0

        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, f"state._step-started.{s}")

    def transition_to_crashed(self):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state.crashed")

    def transition_to_ready(self):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state.ready")

    def state_as_string(self):
        return os.path.basename(self.path)

    def key_state_step(self):

        state = os.path.basename(self.path)[6:]

        step = 0

        if "." in state:
            state_p, suffix = state.rsplit(".", 1)
            if suffix.isdigit():
                step = int(suffix)
                state = state_p

        return self.task_key, state, step

    def is_completed(self):
        return self.path.endswith("state.completed")

    def is_failed(self):
        return fnmatch.fnmatch(self.path, "*/state.failed.*")

    def is_crashed(self):
        return fnmatch.fnmatch(self.path, "*/state.crashed.*")

    def is_timed_out(self):
        return fnmatch.fnmatch(self.path, "*/state.timed-out.*")

    def is_waiting(self):
        return self.path.endswith("state.waiting")

    def has_ended(self):
        return self.is_completed() or self.is_timed_out() or self.is_failed() or self.is_crashed()

    def is_ready(self):
        return self.path.endswith("state.ready")

    def is_ready_or_passed(self):
        return not self.is_waiting()

    def is_in_pre_launch(self):
        return fnmatch.fnmatch(self.path, "*/state._step-started")

    def control_dir(self):
        return os.path.join(self.tracker.pipeline_work_dir, self.task_key)

    def output_dir(self):
        return os.path.join(self.tracker.pipeline_output_dir, self.task_key)

    def task_conf_file(self):
        return os.path.join(self.control_dir(), "task-conf.json")

    def touch_initial_state_file(self):
        Path(self.path).touch(exist_ok=False)
