import fnmatch
import glob
import os
from pathlib import Path


class StateFile:

    @staticmethod
    def create_from_path(task_key, path):

        sf = StateFile(task_key, None, None, path)
        return sf

    def __init__(self, task_key, current_hash_code, pipeline_work_dir, path=None, slurm_array_id=None):
        self.pipeline_work_dir = pipeline_work_dir
        self.task_key = task_key
        if path is not None:
            self.path = path
        else:
            self.path = os.path.join(pipeline_work_dir, task_key, "state.waiting")
        self.hash_code = current_hash_code
        self.inputs = None
        self.outputs = None
        self.is_slurm_array_child = False
        if slurm_array_id is not None:
            self.slurm_array_id = slurm_array_id
        self.is_parent_task = False

    def __repr__(self):
        return f"{self.task_key}/{os.path.basename(self.path)}"

    def refresh(self, new_path):
        assert self.path != new_path
        self.path = new_path
        if self.path.endswith("state.completed"):
            # load from file:
            self.outputs = None
            self.inputs = None

    def reload(self):
        from dry_pipe import StateFileTracker
        p = StateFileTracker.find_state_file_path_if_exists(self.control_dir())
        self.path = p.path

    def transition_to_pre_launch(self, reset_failed=False):
        _, _, s = self.key_state_step()

        if s is None:
            s = 0

        self.path = os.path.join(self.pipeline_work_dir, self.task_key, f"state._step-started.{s}")

    def transition_to_crashed(self):

        step = self.step_idx()

        step_ending = "" if step is None else f".{step}"

        self.path = os.path.join(self.pipeline_work_dir, self.task_key, f"state.crashed{step_ending}")

    def transition_to_ready(self):
        self.path = os.path.join(self.pipeline_work_dir, self.task_key, "state.ready")

    def rewind_to_step(self, step):
        # validate that step is an int
        step = int(step)
        prev_path = self.path

        self.path = os.path.join(self.pipeline_work_dir, self.task_key, f"state.ready.{step}")

        os.rename(prev_path, self.path)

    def transition_to_state(self, state_name, step=None):
        
        if step is not None:
            step = int(step)

        prev_path = self.path

        if step is not None:
            self.path = os.path.join(self.pipeline_work_dir, self.task_key, f"state.{state_name}.{step}")
        else:
            self.path = os.path.join(self.pipeline_work_dir, self.task_key, f"state.{state_name}")

        os.rename(prev_path, self.path)        


    def state_as_string(self):
        return os.path.basename(self.path)

    def state(self):
        state = self.state_as_string()
        # strip "state.":
        state = state[6:]
        # strip step number if applicable:
        if "." in state:
            state = state.split(".")[0]
        return state

    def key_state_step(self):

        state = os.path.basename(self.path)[6:]

        step = None

        if "." in state:
            state_p, suffix = state.rsplit(".", 1)
            if suffix.isdigit():
                step = int(suffix)
                state = state_p

        return self.task_key, state, step

    def step_idx(self):
        task_key, state, step = self.key_state_step()
        return step

    def is_completed(self):
        return self.path.endswith("state.completed")

    def is_failed(self):
        return fnmatch.fnmatch(self.path, "*/state.failed*")

    def is_crashed(self):
        """
        "crashed" is for task processes that die before they have a chance to rename the state file.

        It is an abnormal state that can result from a power shut down, or a bug in DryPipe itself.
        """
        return fnmatch.fnmatch(self.path, "*/state.crashed*")

    def is_killed(self):
        return fnmatch.fnmatch(self.path, "*/state.killed*")

    def is_timed_out(self):
        return fnmatch.fnmatch(self.path, "*/state.timed-out*")

    def is_waiting(self):
        return self.path.endswith("state.waiting")

    def has_ended(self):
        return self.is_completed() or self.is_timed_out() or self.is_failed() or self.is_crashed() or self.is_killed()

    def did_not_succeed(self):
        return self.is_failed() or self.is_crashed() or self.is_killed() or self.is_timed_out()

    def is_ready(self):
        return self.path.endswith("state.ready")

    def is_ready_or_passed(self):
        return not self.is_waiting()

    def is_in_pre_launch(self):
        return fnmatch.fnmatch(self.path, "*/state._step-started.*")

    def control_dir(self):
        return os.path.join(self.pipeline_work_dir, self.task_key)

    def output_dir(self):
        return Path(self.pipeline_work_dir).parent.joinpath("output").joinpath(self.task_key).__str__()

    def task_conf_file(self):
        return os.path.join(self.control_dir(), "task-conf.json")

    def touch_initial_state_file(self):
        Path(self.path).touch(exist_ok=False)
