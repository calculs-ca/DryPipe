import os.path
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from multiprocessing import SimpleQueue
from pathlib import Path
from threading import Thread


class StateFile:
    def __init__(self, pipeline_work_dir, task_key, current_hash_code, tracker):
        self.tracker = tracker
        self.task_key = task_key
        self.path = os.path.join(pipeline_work_dir, task_key, "state.waiting")
        self.last_loaded_hash_code = current_hash_code

    def __str__(self):
        return f"/{self.task_key}/{os.path.basename(self.path)}"

    def update_in_memory(self, path):
        self.path = path

    def state_as_string(self):
        return os.path.basename(self.path)

    def is_completed(self):
        return self.path.endswith("state.completed")

class StateFileTracker:

    def __init__(self, pipeline_instance_dir):
        self.pipeline_instance_dir = pipeline_instance_dir
        self.pipeline_work_dir = os.path.join(pipeline_instance_dir, ".drypipe")
        self.state_files: dict[str, StateFile] = {}

    def state_file_in_memory(self, task_key):
        return self.state_files[task_key]

    def _find_state_file_in_task_control_dir(self, task_key):
        with os.scandir(os.path.join(self.pipeline_work_dir, task_key)) as i:
            for f in i:
                if f.name.startswith("state."):
                    return f
        raise None

    def next_state_file_if_changed(self, task_key, last_state_file):
        if os.path.exists(last_state_file.path):
            return None
        else:
            file = self._find_state_file_in_task_control_dir(task_key)
            if file is None:
                raise Exception(f"no state file exists in {os.path.join(self.pipeline_work_dir, task_key)}")
            state_file = self.state_files[task_key]
            state_file.update_in_memory(file.path)
            return state_file

    def get_state_file_and_save_if_required(self, task):
        """
        :return: (task_is_new, state_file)
        """
        state_file_in_memory = self.state_files.get(task.key)
        if state_file_in_memory is not None:
            return False, state_file_in_memory
        else:
            current_hash_code = task.hash_code()
            state_file_in_memory = StateFile(self.pipeline_work_dir, task.key, current_hash_code, self)
            self.state_files[task.key] = state_file_in_memory
            state_file_on_disc = self._find_state_file_in_task_control_dir(task.key)
            assert state_file_in_memory.path == state_file_on_disc.path
            if state_file_on_disc is None:
                task.save()
                Path(state_file_on_disc.path).touch(exist_ok=False)
                return True, state_file_in_memory
            else:
                last_saved_hash_code = self.load_last_saved_hash_code(state_file_in_memory)
                if last_saved_hash_code != current_hash_code:
                    task.save()
                    state_file_in_memory.last_loaded_hash_code = current_hash_code
                return False, state_file_in_memory

class TaskInputsOutputs:
    pass

class StateMachine:

    def __init__(self, state_file_tracker: StateFileTracker, observer=None, queue_only_func=None):

        if state_file_tracker is None:
            raise Exception(f"state_file_tracker can't be None")

        self.state_file_tracker = state_file_tracker
        if queue_only_func is None:
            self.queue_only_func = lambda i: i
        else:
            self.queue_only_func = queue_only_func

        self._shutdown_requested = False
        self.observer = observer

        self.completed_task_keys: dict[str, TaskInputsOutputs] = {}
        self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys: dict[str, set[str]] = {}
        self.keys_of_tasks_waiting_for_external_events: set[str] = set()

    def _register_upstream_task_dependencies_if_any_and_return_ready_status(self, task) -> bool:

        task_key = task.key

        if len(task.upstream_dep_keys) == 0:
            return True
        else:
            # copy the set, because it will be mutated
            self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys[task_key] = {
                k
                for k in task.upstream_dep_keys
            }

            return False

    def _register_completion_of_upstream_tasks(self, task_key, set_of_newly_completed_task_keys):

        upstream_task_keys = self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys[task_key]

        upstream_task_keys.difference_update(set_of_newly_completed_task_keys)

        if len(upstream_task_keys) == 0:
            del self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys[task_key]
            return True
        else:
            return False

    def incomplete_upstream_tasks_for(self, task_key) -> set[str]:
        upstream_task_keys = self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys.get(task_key)
        if upstream_task_keys is None:
            return set()
        return set(upstream_task_keys)

    def all_incomplete_upstream_tasks(self) -> set[str]:
        return set().union(*self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys.values())

    def set_of_completed_task_keys(self) ->  set[str]:
        return set(self.completed_task_keys.keys())

    def iterate_tasks_to_launch(self, task_generator):

        for task in task_generator():
            is_new, state_file = self.state_file_tracker.create_true_state_if_new_else_fetch_from_memory(task)
            if is_new:
                if self._register_upstream_task_dependencies_if_any_and_return_ready_status(task):
                    self.keys_of_tasks_waiting_for_external_events.add(state_file.task_key)
                    yield state_file

        def newly_completed_state_files():
            for task_key in self.keys_of_tasks_waiting_for_external_events:
                true_state_file = self.state_file_tracker.fetch_true_state_and_update_memory_if_changed(task_key)
                if true_state_file is not None:
                    if true_state_file.is_completed():
                        self.completed_task_keys[true_state_file.task_key] = TaskInputsOutputs()
                        yield task_key

        completed_task_keys = list(newly_completed_state_files())

        self.keys_of_tasks_waiting_for_external_events.difference_update(completed_task_keys)

        # track state_file changes, update dependency map, and yield newly ready tasks
        for key_of_waiting_task in list(self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys.keys()):
            if self._register_completion_of_upstream_tasks(key_of_waiting_task, completed_task_keys):
                state_file_of_ready_task = self.state_file_tracker.state_file_in_memory(key_of_waiting_task)
                self.keys_of_tasks_waiting_for_external_events.add(state_file_of_ready_task.task_key)
                yield state_file_of_ready_task

    def run_sync(self):

        for state_file in self.iterate_tasks_to_launch():
            if state_file is None:
                time.sleep(1)
            else:
                self.launch(state_file)

    def start(self):
        launch_queue = SimpleQueue()

        def launch_daemon():

            def process_task(state_file):
                state_file

            with ThreadPoolExecutor(max_workers=5) as executor:
                while not self._shutdown_requested:
                    batch = self.launch_queue.get()
                    executor.map(process_task, batch)

        def main_daemon():
            last_inactive_round_count = 0
            sleep_prescriptions = [0, 1, 2, 2, 4, 4, 5, 8, 9, 10]
            while not self._shutdown_requested:
                work_done = 0
                while batch := list(islice(self.iterate_tasks_to_launch(), 50)):
                    work_done += len(batch)
                    self.launch_queue.put(batch)
                if work_done > 0:
                    last_inactive_round_count = 0
                    continue
                elif last_inactive_round_count < len(sleep_prescriptions):
                    last_inactive_round_count += 1

                if last_inactive_round_count > 0:
                    time.sleep(sleep_prescriptions[last_inactive_round_count])

        main_thread = Thread(target=main_daemon)
        main_thread.start()
        launch_thread = Thread(target=launch_daemon)
        launch_thread.start()

        self.threads.append(main_thread)
        self.threads.append(launch_thread)
