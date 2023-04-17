import json
import os
from pathlib import Path

from dry_pipe.state_machine import StateFile


class TaskMockup:
    def __init__(self, key, upstream_dep_keys=[]):
        self.key = key
        self._upstream_dep_keys = upstream_dep_keys
        self.inputs = [1, 5, 4]
        self.is_slurm_array_child = False

    def upstream_dep_keys(self):
        return self._upstream_dep_keys

    def cause_change_of_hash_code(self, data):
        self.inputs.append(data)

    def compute_hash_code(self):
        return ",".join(str(i) for i in self.inputs)

    def save(self, state_file, hash_code):
        task_control_dir = Path(state_file.tracker.pipeline_instance_dir, ".drypipe", self.key)
        task_control_dir.mkdir(exist_ok=True, parents=True)
        inputs = [
            {
                "upstream_task_key": k
            }
            for k in self.upstream_dep_keys()
        ]
        with open(os.path.join(task_control_dir, "task-conf.json"), "w") as tc:
            tc.write(json.dumps({
                "hash_code": hash_code,
                "inputs": inputs
            }))


class StateFileTrackerMockup:

    def __init__(self):
        self.state_files_in_memory: dict[str, StateFile] = {}
        # task_key -> path
        self.task_keys_to_task_states_on_mockup_disk: dict[str, str] = {}
        self.pipeline_work_dir = "/"

    def prepare_instance_dir(self):
        pass

    def set_completed_on_disk(self, task_key):
        self.task_keys_to_task_states_on_mockup_disk[task_key] = "state.completed"

    def all_tasks(self):
        return self.state_files_in_memory.values()

    def lookup_state_file_from_memory(self, task_key):
        return self.state_files_in_memory[task_key]

    def register_pre_launch(self, state_file):
        state_file.transition_to_pre_launch()
        self.task_keys_to_task_states_on_mockup_disk[state_file.task_key] = os.path.basename(state_file.path)

    def completed_task_keys(self):
        for k, state_file in self.state_files_in_memory.items():
            if state_file.is_completed():
                yield k

    def fetch_true_state_and_update_memory_if_changed(self, task_key):

        true_task_state = self.task_keys_to_task_states_on_mockup_disk.get(task_key)
        state_file_in_memory = self.state_files_in_memory.get(task_key)

        if state_file_in_memory is None:
            raise Exception(f"{task_key} should be in memory by now")

        if str(state_file_in_memory) == f"/{task_key}/{true_task_state}":
            return None, state_file_in_memory
        else:
            state_file_in_memory.refresh(f"/{task_key}/{true_task_state}")
            return state_file_in_memory, state_file_in_memory

    def create_true_state_if_new_else_fetch_from_memory(self, task):
        """
        :param task:
        :return: (task_is_new, state_file)
        """
        true_task_state = self.task_keys_to_task_states_on_mockup_disk.get(task.key)

        if true_task_state is None:
            s = StateFile(task.key, task.compute_hash_code(), self)
            self.task_keys_to_task_states_on_mockup_disk[task.key] = s.state_as_string()
            self.state_files_in_memory[task.key] = s
            return True, s
        else:
            return False, self.state_files_in_memory[task.key]
