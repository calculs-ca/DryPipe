import os
from pathlib import Path

from dry_pipe.state_file import StateFile
from dry_pipe.state_file_tracker import StateFileTracker


class FrozenTask:

    def __init__(self, key):
        self.key = key


class FrozenDAGGenerator:
    """
    the DAG as materialized in .drypipe/, read without running the DAG generator: a task is a
    sub directory of .drypipe/ having a state file
    """

    def __init__(self, pipeline_instance_dir, is_ignored=lambda key: False):
        self.pipeline_work_dir = Path(pipeline_instance_dir, ".drypipe")
        self.is_ignored = is_ignored

        if next(self._tasks_on_disk(), None) is None:
            raise Exception(
                f"{pipeline_instance_dir} has no task in .drypipe/, the DAG can't be read from the file system"
            )

    def _tasks_on_disk(self):
        """(key, state file path) of each task, ordered by key"""
        if not self.pipeline_work_dir.is_dir():
            return

        with os.scandir(self.pipeline_work_dir) as entries:
            control_dirs = sorted((e for e in entries if e.is_dir()), key=lambda e: e.name)

        for control_dir in control_dirs:
            state_file_path = StateFileTracker.find_state_file_path_if_exists(control_dir.path)
            if state_file_path is not None:
                yield control_dir.name, state_file_path.path

    def iterate_key_state_steps(self, key_universe=None):

        def is_selected(key):
            return not self.is_ignored(key) and (key_universe is None or key in key_universe)

        for key, state_file_path in self._tasks_on_disk():
            if is_selected(key):
                yield FrozenTask(key), StateFile(key, None, str(self.pipeline_work_dir), path=state_file_path)
