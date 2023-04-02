import fnmatch
import json
import os.path
from pathlib import Path


# TODO: rename to TaskState
class StateFile:

    def __init__(self, task_key, current_hash_code, tracker, path=None):
        self.tracker = tracker
        self.task_key = task_key
        if path is not None:
            self.path = path
        else:
            self.path = os.path.join(tracker.pipeline_work_dir, task_key, "state.waiting")
        self.hash_code = current_hash_code
        self.inputs = None
        self.outputs = None

    def __str__(self):
        return f"/{self.task_key}/{os.path.basename(self.path)}"

    def update_in_memory(self, path):
        self.path = path

    def state_as_string(self):
        return os.path.basename(self.path)

    def is_completed(self):
        return self.path.endswith("state.completed")

    def set_completed(self, task_outputs, resolved_task_inputs):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state.completed")
        self.outputs = task_outputs
        self.inputs = resolved_task_inputs


class StateFileTracker:

    def __init__(self, pipeline_instance_dir):
        self.pipeline_instance_dir = pipeline_instance_dir
        self.pipeline_work_dir = os.path.join(pipeline_instance_dir, ".drypipe")
        self.state_files_in_memory: dict[str, StateFile] = {}
        self.load_from_disk_count = 0
        self.resave_count = 0
        self.new_save_count = 0

    def set_completed_on_disk(self, task_key):
        os.rename(
            self.state_files_in_memory[task_key].path,
            os.path.join(self.pipeline_work_dir, task_key, "state.completed")
        )

    def state_file_in_memory(self, task_key):
        return self.state_files_in_memory[task_key]

    def _find_state_file_in_task_control_dir(self, task_key):
        try:
            with os.scandir(os.path.join(self.pipeline_work_dir, task_key)) as i:
                for f in i:
                    if f.name.startswith("state."):
                        return f
        except FileNotFoundError:
            pass
        return None

    def fetch_true_state_and_update_memory_if_changed(self, task_key):
        cached_state_file = self.state_file_in_memory(task_key)
        if os.path.exists(cached_state_file.path):
            return None
        else:
            file = self._find_state_file_in_task_control_dir(task_key)
            if file is None:
                raise Exception(f"no state file exists in {os.path.join(self.pipeline_work_dir, task_key)}")
            cached_state_file.update_in_memory(file.path)
            return cached_state_file

    def _load_task_conf(self, task_control_dir):
        with open(os.path.join(task_control_dir, "task-conf.json")) as tc:
            return json.loads(tc.read())

    def load_from_existing_file_on_disc_and_resave_if_required(self, task, state_file_path):

        task_control_dir = os.path.dirname(state_file_path)
        task_key = os.path.basename(task_control_dir)
        pipeline_work_dir = os.path.dirname(task_control_dir)
        assert task.key == task_key
        task_conf = self._load_task_conf(task_control_dir)
        current_hash_code = task.compute_hash_code()
        state_file = StateFile(task_key, current_hash_code, self, path=state_file_path)
        if state_file.is_completed():
            pass
            #load inputs and outputs
        else:
            if task_conf["hash_code"] != state_file.hash_code:
                task.save(os.path.dirname(pipeline_work_dir), current_hash_code)
                state_file.hash_code = current_hash_code
                self.resave_count += 1
        self.load_from_disk_count += 1
        return state_file

    def _iterate_all_tasks(self, glob_filter):
        with os.scandir(self.pipeline_work_dir) as pwd_i:
            for task_control_dir_entry in pwd_i:
                task_control_dir = task_control_dir_entry.path
                task_key = os.path.basename(task_control_dir)

                if glob_filter is not None:
                    if not fnmatch.fnmatch(task_key, glob_filter):
                        continue

                state_file_dir_entry = self._find_state_file_in_task_control_dir(task_key)
                if state_file_dir_entry is None:
                    raise Exception(f"no state file exists in {task_control_dir_entry.path}")

                yield task_key, task_control_dir, state_file_dir_entry

    def load_completed_tasks_for_query(self, glob_filter=None):
        for task_key, task_control_dir, state_file_dir_entry in self._iterate_all_tasks(glob_filter):
            if state_file_dir_entry.name == "state.completed":
                yield task_key, TaskInputsOutputs()

    def load_state_files_for_run(self, glob_filter=None):
        for task_key, task_control_dir, state_file_dir_entry in self._iterate_all_tasks(glob_filter):
            state_file = StateFile(
                task_key, None, self, path=state_file_dir_entry.path
            )
            self.state_files_in_memory[task_key] = state_file
            task_conf = self._load_task_conf(task_control_dir)
            if state_file.is_completed():
                yield True, state_file, None, TaskInputsOutputs()
            else:
                upstream_task_keys = set()
                for i in task_conf["inputs"]:
                    k = i.get("upstream_task_key")
                    if k is not None:
                        upstream_task_keys.add(k)
                yield False, state_file, upstream_task_keys, None

    def create_true_state_if_new_else_fetch_from_memory(self, task):
        """
        :return: (task_is_new, state_file)
        """
        state_file_in_memory = self.state_files_in_memory.get(task.key)
        if state_file_in_memory is not None:
            # state_file_in_memory is assumed up to date, since task declarations don't change between runs
            # by design, DAG generators that violate this assumption are considered at fault
            return False, state_file_in_memory
        else:
            # we have a new task OR process was restarted
            state_file_on_disc = self._find_state_file_in_task_control_dir(task.key)
            if state_file_on_disc is not None:
                # process was restarted, task is NOT new
                state_file_in_memory = self.load_from_existing_file_on_disc_and_resave_if_required(task, state_file_on_disc)
                self.state_files_in_memory[task.key] = state_file_in_memory
                return False, state_file_in_memory
            else:
                # task is new
                hash_code = task.compute_hash_code()
                task.save(self.pipeline_instance_dir, hash_code)
                state_file_in_memory = StateFile(task.key, hash_code, self)
                self.state_files_in_memory[task.key] = state_file_in_memory
                Path(state_file_in_memory.path).touch(exist_ok=False)
                self.new_save_count += 1
                return True, state_file_in_memory

class TaskInputsOutputs:
    pass

class StateMachine:

    def __init__(self, state_file_tracker: StateFileTracker, task_generator=None, observer=None, queue_only_func=None):

        if state_file_tracker is None:
            raise Exception(f"state_file_tracker can't be None")

        self.state_file_tracker = state_file_tracker
        if queue_only_func is None:
            self.queue_only_func = lambda i: i
        else:
            self.queue_only_func = queue_only_func

        self.task_generator = task_generator
        self.observer = observer

        self.completed_task_keys: dict[str, TaskInputsOutputs] = {}
        self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys: dict[str, set[str]] = {}
        self.keys_of_tasks_waiting_for_external_events: set[str] = set()
        self._ready_state_files_loaded_from_disk = None

    def prepare_for_run_without_generator(self):
        self._ready_state_files_loaded_from_disk = []
        incomplete_task_files_with_upstream_task_keys = []
        for is_completed, state_file, upstream_task_keys, inputs_outputs in \
                self.state_file_tracker.load_state_files_for_run():
            if is_completed:
                self.completed_task_keys[state_file.task_key] = inputs_outputs
            else:
                incomplete_task_files_with_upstream_task_keys.append((state_file, upstream_task_keys))

        for state_file, upstream_task_keys in incomplete_task_files_with_upstream_task_keys:
            if self._register_upstream_task_dependencies_if_any_and_return_ready_status(
                    state_file.task_key, upstream_task_keys
            ):
                self._ready_state_files_loaded_from_disk.append(state_file)

    def _register_upstream_task_dependencies_if_any_and_return_ready_status(self, task_key, upstream_dep_keys) -> bool:
        if len(upstream_dep_keys) == 0:
            return True
        else:
            # copy the set, because it will be mutated
            self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys[task_key] = {
                k for k in upstream_dep_keys
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

    def set_of_completed_task_keys(self) ->  set[str]:
        return set(self.completed_task_keys.keys())

    def _ready_state_files_from_generator(self):
        for task in self.task_generator():
            is_new, state_file = self.state_file_tracker.create_true_state_if_new_else_fetch_from_memory(task)
            if is_new:
                if self._register_upstream_task_dependencies_if_any_and_return_ready_status(
                    task.key, task.upstream_dep_keys
                ):
                    self.keys_of_tasks_waiting_for_external_events.add(state_file.task_key)
                    yield state_file

    def iterate_tasks_to_launch(self):

        if self.task_generator is not None:
            yield from self._ready_state_files_from_generator()
        elif self._ready_state_files_loaded_from_disk is not None:
            for state_file in self._ready_state_files_loaded_from_disk:
                self.keys_of_tasks_waiting_for_external_events.add(state_file.task_key)
                yield state_file
            self._ready_state_files_loaded_from_disk = []
        else:
            raise Exception(
                f"{type(StateMachine)} must be constructed with either a dag_generator, or " +
                "load_from_instance_dir() must be invoked before calling this method"
            )

        def gen_newly_completed_task_keys():
            for task_key in self.keys_of_tasks_waiting_for_external_events:
                true_state_file = self.state_file_tracker.fetch_true_state_and_update_memory_if_changed(task_key)
                if true_state_file is not None:
                    if true_state_file.is_completed():
                        self.completed_task_keys[true_state_file.task_key] = TaskInputsOutputs()
                        yield task_key

        newly_completed_task_keys = list(gen_newly_completed_task_keys())

        self.keys_of_tasks_waiting_for_external_events.difference_update(newly_completed_task_keys)

        # track state_file changes, update dependency map, and yield newly ready tasks
        for key_of_waiting_task in list(self.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys.keys()):
            if self._register_completion_of_upstream_tasks(key_of_waiting_task, newly_completed_task_keys):
                state_file_of_ready_task = self.state_file_tracker.state_file_in_memory(key_of_waiting_task)
                self.keys_of_tasks_waiting_for_external_events.add(state_file_of_ready_task.task_key)
                yield state_file_of_ready_task
