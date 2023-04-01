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
        return f"{self.task_key}/{os.path.basename(self.path)}"

    def update_in_memory(self, path):
        self.path = path

    def state_name(self):
        return os.path.basename(self.path).split(".")[1]

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


class UpstreamTaskDependencies:

    def __init__(self, upstream_state_files):
        self._upstream_task_key_to_last_state_file: dict[str, StateFile] = {
            task_key: last_state_file
            for task_key, last_state_file in upstream_state_files.items()
        }

    def set_of_wait_relationships(self):
        return {
            f"{k}->{state.state_name()}"
            for k, state in self._upstream_task_key_to_last_state_file.items()
        }

    def __str__(self):
        return ",".join(self.set_of_wait_relationships())

    def register_completion_of_upstream_tasks(self, completed_task_keys):
        for k in completed_task_keys:
            if k in self._upstream_task_key_to_last_state_file:
                del self._upstream_task_key_to_last_state_file[k]

    def all_dependencies_satisfied(self):
        return len(self._upstream_task_key_to_last_state_file) == 0


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
        self.keys_of_waiting_tasks_to_upstream_task_dependencies: dict[str, UpstreamTaskDependencies] = {}
        self.keys_of_tasks_waiting_for_external_event_to_last_state_file: dict[str, StateFile] = {}

    def _register_upstream_task_dependencies_if_any(self, task):

        task_key = task.key
        upstream_dep_keys = task.upstream_dep_keys

        if len(upstream_dep_keys) == 0:
            return False

        upstream_task_dependencies = self.keys_of_waiting_tasks_to_upstream_task_dependencies.get(task_key)

        if upstream_task_dependencies is None:
            utd = UpstreamTaskDependencies({
                k: self.state_file_tracker.state_file_in_memory(k)
                for k in upstream_dep_keys
                if k not in self.completed_task_keys
            })
            self.keys_of_waiting_tasks_to_upstream_task_dependencies[task_key] = utd

        return True

    def set_of_wait_relationships_of(self, task_key):
        ud = self.keys_of_waiting_tasks_to_upstream_task_dependencies.get(task_key)
        if ud is None:
            return set()
        return ud.set_of_wait_relationships()

    def set_of_all_wait_relationships(self):
        def gen():
            for task_key, ud in self.keys_of_waiting_tasks_to_upstream_task_dependencies:
                yield None



    def set_of_completed_task_keys(self):
        return self.completed_task_keys.keys()

    def iterate_tasks_to_launch(self, task_generator):

        def newly_completed_state_files():
            for task_key in list(self.keys_of_tasks_waiting_for_external_event_to_last_state_file.keys()):
                last_state_file = self.keys_of_tasks_waiting_for_external_event_to_last_state_file[task_key]
                next_state_file = self.state_file_tracker.next_state_file_if_changed(task_key, last_state_file)
                if next_state_file is not None:
                    if next_state_file.is_completed():
                        del self.keys_of_tasks_waiting_for_external_event_to_last_state_file[task_key]
                        self.completed_task_keys[next_state_file.task_key] = TaskInputsOutputs()
                        yield task_key
                    else:
                        self.keys_of_tasks_waiting_for_external_event_to_last_state_file[task_key] = next_state_file

        for task in task_generator():
            is_new, state_file = self.state_file_tracker.get_state_file_and_save_if_required(task)
            if is_new:
                if not self._register_upstream_task_dependencies_if_any(task):
                    self.keys_of_tasks_waiting_for_external_event_to_last_state_file[task.key] = state_file
                    yield state_file

        completed_task_keys = list(newly_completed_state_files())

        # track state_file changes, update dependency map, and yield newly ready tasks
        for key_of_waiting_task in list(self.keys_of_waiting_tasks_to_upstream_task_dependencies.keys()):
            upstream_deps_task_keys = self.keys_of_waiting_tasks_to_upstream_task_dependencies.get(key_of_waiting_task)
            upstream_deps_task_keys.register_completion_of_upstream_tasks(completed_task_keys)
            if upstream_deps_task_keys.all_dependencies_satisfied():
                del self.keys_of_waiting_tasks_to_upstream_task_dependencies[key_of_waiting_task]
                state_file_of_ready_task = self.state_file_tracker.state_file_in_memory(key_of_waiting_task)
                self.keys_of_tasks_waiting_for_external_event_to_last_state_file[state_file_of_ready_task.task_key] = \
                    state_file_of_ready_task
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
