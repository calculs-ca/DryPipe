import glob
import json
import logging
import os
import pathlib

PIPELINE_TRANSITIONS = {
    "running": ["stopped", "stalled", "completed"],
    "stopped": ["running"],
    "stalled": ["running"],
    "completed": ["running"]
}

PIPELINE_STATES = PIPELINE_TRANSITIONS.keys()

logger = logging.getLogger(__name__)

class PipelineState:

    @staticmethod
    def iterate_from_instances_dir(pipeline_instances_dir):
        for d in glob.glob(os.path.join(pipeline_instances_dir, "*", ".drypipe", "state.*")):
            d = os.path.dirname(d)
            yield PipelineState.from_pipeline_work_dir(d, create_if_not_exists=False)

    @staticmethod
    def from_pipeline_work_dir(work_dir, create_if_not_exists=False, none_if_not_exists=False):

        state_file = glob.glob(os.path.join(work_dir, "state.*"))

        if len(state_file) > 1:
            raise Exception(f"More than one pipeline state files: {state_file}")
        elif len(state_file) == 1:
            pipeline_state = PipelineState(state_file[0], state_file_exists=True)
        else:

            if none_if_not_exists:
                return

            state_file = [os.path.join(work_dir, "state.running")]
            state_file_exists = False
            if create_if_not_exists:
                pathlib.Path(state_file[0]).touch(exist_ok=False)
                state_file_exists = True

            pipeline_state = PipelineState(state_file[0], state_file_exists)

        return pipeline_state

    @staticmethod
    def from_pipeline_instance_dir(pipeline_instance_dir, none_if_not_exists=False):
        return PipelineState.from_pipeline_work_dir(
            os.path.join(pipeline_instance_dir, ".drypipe"),
            none_if_not_exists=none_if_not_exists
        )

    @staticmethod
    def from_state_file(state_file):
        return PipelineState(state_file, True)

    def __init__(self, state_file, state_file_exists):

        self.state_file = os.path.abspath(state_file)
        self.pipeline_instance_work_dir = os.path.dirname(self.state_file)
        self.state_file_exists = state_file_exists

        if os.path.basename(self.pipeline_instance_work_dir) != ".drypipe":
            raise Exception(f"dir {state_file} can't be a pipeline state file, must be in a dir named '.drypipe'")

        state_file_basename = os.path.basename(self.state_file)

        if not state_file_basename.startswith("state."):
            raise Exception(f"invalid statefile, should start with 'state.'")

        self.state_name = state_file_basename.split(".")[1]

        if self.state_name not in PIPELINE_STATES:
            raise Exception(f"invalid state file name {state_file}")

        self.last_update_time = self.refresh_last_update_time()

    def __str__(self):
        return f"PipelineState({self.state_name},{self.instance_dir()})"

    def refresh_last_update_time(self):
        self.last_update_time = pathlib.Path(self.pipeline_instance_work_dir).stat()
        return self.last_update_time

    def instance_dir(self):
        return os.path.dirname(self.pipeline_instance_work_dir)

    def is_completed(self):
        return self.state_name == "completed"

    def is_stalled(self):
        return self.state_name == "stalled"

    def is_running(self):
        return self.state_name == "running"

    def _transition_is_valid(self, next_state):

        valid_next_states = PIPELINE_TRANSITIONS.get(self.state_name)

        if valid_next_states is None:
            raise Exception(f"invalid pipeline state '{next_state}'")

        if next_state not in valid_next_states:
            raise Exception(f"pipeline can't transition from {self.state_name} to {next_state}")

    def touch(self):

        if len(list(glob.glob(os.path.join(self.pipeline_instance_work_dir, "state.*")))) > 0:
            return

        pathlib.Path(self.state_file).touch(exist_ok=False)
        self.state_file_exists = True

    def _transition_to(self, next_state):

        self._transition_is_valid(next_state)

        dst = os.path.join(self.pipeline_instance_work_dir, f"state.{next_state}")
        os.rename(
            self.state_file,
            dst
        )

        return dst

    def dir_basename(self):
        return os.path.basename(os.path.dirname(self.pipeline_instance_work_dir))

    def instance_dir(self):
        return os.path.dirname(self.pipeline_instance_work_dir)

    def instance_dir_key(self):

        parent, pid = self.instance_dir().split("/")[-2:]
        return f"{parent}|{pid}"

    def _counts_cache_file(self):
        return os.path.join(self.pipeline_instance_work_dir, f"counts.json")

    def transition_to_completed(self):

        from dry_pipe.monitoring import PipelineMetricsTable

        with open(self._counts_cache_file(), "w") as _f:
            _f.write(
                json.dumps(PipelineMetricsTable.totals_row(self.instance_dir()), indent=4)
            )

        self._transition_to("completed")

    def transition_to_running_if_not_running(self):
        if not self.is_running():
            self._transition_to("running")

    def totals_row(self):

        from dry_pipe.monitoring import PipelineMetricsTable

        if self.is_completed():

            cf = self._counts_cache_file()
            if os.path.exists(cf):
                with open(cf) as f:
                    return json.loads(f.read())

        return PipelineMetricsTable.totals_row(self.instance_dir())
