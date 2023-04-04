import fnmatch
import json
import os.path
import shutil
from pathlib import Path

from dry_pipe import TaskBuilder, TaskConf, script_lib
from dry_pipe.script_lib import FileCreationDefaultModes, write_pipeline_lib_script, iterate_out_vars_from, TaskOutput, \
    iterate_file_task_outputs
from dry_pipe.task import TaskOutputs, TaskInputs


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

    def refresh(self, new_path):
        assert self.path != new_path
        self.path = new_path
        if self.path.endswith("state.completed"):
            # load from file:
            self.outputs = None
            self.inputs = None

    def transition_to_pre_launch(self):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state._step-started")

    def state_as_string(self):
        return os.path.basename(self.path)

    def is_completed(self):
        return self.path.endswith("state.completed")

    def is_failed(self):
        return fnmatch.fnmatch(self.path, "*/state.failed.*")

    def control_dir(self):
        return os.path.join(self.tracker.pipeline_work_dir, self.task_key)

    def load_task_conf_json(self):
        with open(os.path.join(self.control_dir(), "task-conf.json")) as tc:
            return json.loads(tc.read())

    def load_task_conf(self):
        return TaskConf.from_json(self.load_task_conf_json())


class StateFileTracker:

    def __init__(self, pipeline_instance_dir):
        self.pipeline_instance_dir = pipeline_instance_dir
        self.pipeline_work_dir = os.path.join(pipeline_instance_dir, ".drypipe")
        self.state_files_in_memory: dict[str, StateFile] = {}
        self.load_from_disk_count = 0
        self.resave_count = 0
        self.new_save_count = 0

    def prepare_instance_dir(self, conf_dict):
        Path(self.pipeline_instance_dir).mkdir(
            exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)
        Path(self.pipeline_work_dir).mkdir(
            exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)
        Path(self.pipeline_instance_dir, "output").mkdir(
            exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)

        with open(Path(self.pipeline_work_dir, "conf.json"), "w") as conf_file:
            conf_file.write(json.dumps(conf_dict, indent=4))

        shutil.copy(script_lib.__file__, self.pipeline_work_dir)

        script_lib_file = os.path.join(self.pipeline_work_dir, "script_lib")
        with open(script_lib_file, "w") as script_lib_file_handle:
            write_pipeline_lib_script(script_lib_file_handle)
        os.chmod(script_lib_file, FileCreationDefaultModes.pipeline_instance_scripts)


    def set_completed_on_disk(self, task_key):
        os.rename(
            self.state_files_in_memory[task_key].path,
            os.path.join(self.pipeline_work_dir, task_key, "state.completed")
        )

    def register_pre_launch(self, state_file):
        previous_path = state_file.path
        state_file.transition_to_pre_launch()
        os.rename(previous_path, state_file.path)

    def completed_task_keys(self):
        for k, state_file in self.state_files_in_memory.items():
            if state_file.is_completed():
                yield k

    def all_tasks(self):
        return self.state_files_in_memory.values()

    def lookup_state_file_from_memory(self, task_key):
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
        state_file = self.lookup_state_file_from_memory(task_key)
        if os.path.exists(state_file.path):
            return None
        else:
            file = self._find_state_file_in_task_control_dir(task_key)
            if file is None:
                raise Exception(f"no state file exists in {os.path.join(self.pipeline_work_dir, task_key)}")
            state_file.refresh(file.path)
            return state_file

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

    def _iterate_all_tasks_from_disk(self, glob_filter):
        with os.scandir(self.pipeline_work_dir) as pwd_i:
            for task_control_dir_entry in pwd_i:
                if not task_control_dir_entry.is_dir():
                    continue
                if task_control_dir_entry.name == "__pycache__":
                    continue
                task_control_dir = task_control_dir_entry.path
                task_key = os.path.basename(task_control_dir)

                if glob_filter is not None:
                    if not fnmatch.fnmatch(task_key, glob_filter):
                        continue

                state_file_dir_entry = self._find_state_file_in_task_control_dir(task_key)
                if state_file_dir_entry is None:
                    raise Exception(f"no state file exists in {task_control_dir_entry.path}")

                yield task_key, task_control_dir, state_file_dir_entry

    def load_tasks_for_query(self, glob_filter=None, include_non_completed=False):
        pod = os.path.join(self.pipeline_instance_dir, "output")
        for task_key, task_control_dir, state_file_dir_entry in self._iterate_all_tasks_from_disk(glob_filter):
            if state_file_dir_entry.name == "state.completed" or include_non_completed:
                state_file = StateFile(
                    task_key, None, self, path=state_file_dir_entry.path
                )
                task_conf = state_file.load_task_conf_json()
                var_file = os.path.join(task_control_dir, "output_vars")
                is_complete = state_file_dir_entry.name == "state.completed"

                class Task:
                    def __init__(self):
                        self.key = task_key
                        self.inputs = TaskInputs(self, task_conf, pipeline_work_dir=pod)

                        task_outputs = {}
                        unparsed_out_vars = dict(iterate_out_vars_from(var_file))

                        for o in task_conf["outputs"]:
                            o = TaskOutput.from_json(o)
                            if o.type != 'file':
                                o.set_resolved_value(unparsed_out_vars.get(o.name))
                                task_outputs[o.name] = o
                            else:
                                o.set_resolved_value(os.path.join(pod, task_key, o.produced_file_name))
                                task_outputs[o.name] = o

                        self.outputs = TaskOutputs(self, task_outputs)
                        self.state_file = state_file

                    def __str__(self):
                        return f"Task(key={self.key})"

                    def is_completed(self):
                        return state_file.is_completed()

                    def is_failed(self):
                        return state_file.is_failed()

                    def state_name(self):
                        return state_file.state_as_string()

                yield Task()

    def load_state_files_for_run(self, glob_filter=None):
        for task_key, task_control_dir, state_file_dir_entry in self._iterate_all_tasks_from_disk(glob_filter):
            state_file = StateFile(
                task_key, None, self, path=state_file_dir_entry.path
            )
            self.state_files_in_memory[task_key] = state_file
            task_conf = self._load_task_conf(task_control_dir)
            if state_file.is_completed():
                yield True, state_file, None
            else:
                upstream_task_keys = set()
                for i in task_conf["inputs"]:
                    k = i.get("upstream_task_key")
                    if k is not None:
                        upstream_task_keys.add(k)
                yield False, state_file, upstream_task_keys

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

class AllRunnableTasksCompletedOrInError(Exception):
    pass

class InvalidQueryInTaskGenerator(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class StateMachine:

    def __init__(self, state_file_tracker: StateFileTracker, task_generator=None, observer=None, queue_only_func=None):

        if state_file_tracker is None:
            raise Exception(f"state_file_tracker can't be None")

        self.state_file_tracker = state_file_tracker
        if queue_only_func is None:
            self._queue_only_func = lambda i: i
        else:
            self._queue_only_func = queue_only_func

        self._task_generator = task_generator
        self._observer = observer

        self._keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys: dict[str, set[str]] = {}
        self._keys_of_tasks_waiting_for_external_events: set[str] = set()
        self._keys_of_failed_tasks: set[str] = set()

        self._ready_state_files_loaded_from_disk = None
        self._pending_queries: dict[str, int] = {}

        if self._task_generator is not None:
            self._generator_exhausted = False
        else:
            self._generator_exhausted = True

        self._no_runnable_tasks_left = False

        #TODO: clean up:
        self.task_conf = TaskConf.default()

    def task(self, key=None, task_conf=None):
        if key is None:
            raise Exception(f"key can't be none")

        if task_conf is None:
            task_conf = TaskConf.default()
        return TaskBuilder(key, task_conf=task_conf, dsl=self)

    def file(self, p):
        return Path(p)

    def query_all_completed(self, glob_expression):

        query_all_matches_count = 0
        query_completed_matches = []

        for state_file in self.state_file_tracker.all_tasks():
            if fnmatch.fnmatch(state_file.task_key, glob_expression):
                query_all_matches_count += 1
                if state_file.is_completed():
                    query_completed_matches.append(state_file)

        if query_all_matches_count == 0:
            return []
        elif query_all_matches_count == len(query_completed_matches):
            self._pending_queries.pop(glob_expression, None)

            class Match:
                def __init__(self):
                    self.tasks = query_completed_matches

            return Match(),
        else:
            prev_count = self._pending_queries.get(glob_expression)
            if prev_count is None:
                self._pending_queries[glob_expression] = query_all_matches_count
                return []
            elif prev_count != query_all_matches_count:
                raise InvalidQueryInTaskGenerator(
                    f"Warning: query {glob_expression} matched on tasks that were yielded AFTER the first" +
                    " invocation of 'query_all_completed'"
                )
            else:
                return []

    def prepare_for_run_without_generator(self):
        self._work_dir_prepared = True
        # TODO: validate instance dir
        self._ready_state_files_loaded_from_disk = []
        incomplete_task_files_with_upstream_task_keys = []
        for is_completed, state_file, upstream_task_keys in self.state_file_tracker.load_state_files_for_run():
            if not is_completed:
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
            self._keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys[task_key] = {
                k for k in upstream_dep_keys
            }
            return False

    def _register_completion_of_upstream_tasks(self, task_key, set_of_newly_completed_task_keys):

        upstream_task_keys = self._keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys[task_key]

        upstream_task_keys.difference_update(set_of_newly_completed_task_keys)

        if len(upstream_task_keys) == 0:
            del self._keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys[task_key]
            return True
        else:
            return False

    def set_of_completed_task_keys(self) ->  set[str]:
        return set(self.state_file_tracker.completed_task_keys())

    def _ready_state_files_from_generator(self):

        new_generated_tasks = 0

        for task in self._task_generator(self):
            is_new, state_file = self.state_file_tracker.create_true_state_if_new_else_fetch_from_memory(task)
            if is_new:
                new_generated_tasks += 1
                if self._register_upstream_task_dependencies_if_any_and_return_ready_status(
                    task.key, task.upstream_dep_keys()
                ):
                    yield state_file

        if new_generated_tasks == 0 and len(self._pending_queries) == 0:
            self._generator_exhausted = True

    def iterate_tasks_to_launch(self):

        if self._no_runnable_tasks_left:
            raise AllRunnableTasksCompletedOrInError()

        def register_pre_launch(state_file):
            self._keys_of_tasks_waiting_for_external_events.add(state_file.task_key)
            self.state_file_tracker.register_pre_launch(state_file)
            return state_file

        if self._task_generator is not None:
            for state_file in self._ready_state_files_from_generator():
                yield register_pre_launch(state_file)
        elif self._ready_state_files_loaded_from_disk is not None:
            for state_file in self._ready_state_files_loaded_from_disk:
                yield register_pre_launch(state_file)
            self._ready_state_files_loaded_from_disk = []
        else:
            raise Exception(
                f"{type(StateMachine)} must be constructed with either a dag_generator, or " +
                "load_from_instance_dir() must be invoked before calling this method"
            )

        newly_failed_task_keys = []

        def gen_newly_completed_task_keys():
            for task_key in self._keys_of_tasks_waiting_for_external_events:
                true_state_file = self.state_file_tracker.fetch_true_state_and_update_memory_if_changed(task_key)
                if true_state_file is not None:
                    if true_state_file.is_completed():
                        yield task_key
                    elif true_state_file.is_failed():
                        self._keys_of_failed_tasks.add(true_state_file.task_key)
                        newly_failed_task_keys.append(true_state_file.task_key)

        newly_completed_task_keys = list(gen_newly_completed_task_keys())

        self._keys_of_tasks_waiting_for_external_events.difference_update(newly_completed_task_keys)
        self._keys_of_tasks_waiting_for_external_events.difference_update(newly_failed_task_keys)

        # track state_file changes, update dependency map, and yield newly ready tasks
        for key_of_waiting_task in list(self._keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys.keys()):
            if self._register_completion_of_upstream_tasks(key_of_waiting_task, newly_completed_task_keys):
                state_file_of_ready_task = self.state_file_tracker.lookup_state_file_from_memory(key_of_waiting_task)
                yield register_pre_launch(state_file_of_ready_task)

        if self._generator_exhausted:
            if len(self._keys_of_tasks_waiting_for_external_events) == 0:
                self._no_runnable_tasks_left = True
