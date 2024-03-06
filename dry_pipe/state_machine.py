import fnmatch
import os.path
from pathlib import Path

from dry_pipe import TaskBuilder, TaskConf
from dry_pipe.core_lib import StateFileTracker
from dry_pipe.task_process import TaskProcess


# TODO: rename to TaskState


class AllRunnableTasksCompletedOrInError(Exception):
    pass

class InvalidQueryInTaskGenerator(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class InvalidTaskDefinition(Exception):

    def __init__(self, task_builder, dag_gen):
        self.task_builder = task_builder
        self.dag_gen = dag_gen

    def __str__(self):
        file_of_dag_gen = self.dag_gen.__globals__['__file__']
        raise Exception(f"bad task definition task({self.task_builder.key}) in {file_of_dag_gen}" +
                        ", expression is a function " +
                        f" with zero args that must be called before yield.")



class StateMachine:

    def __init__(self, state_file_tracker: StateFileTracker, task_generator=None, observer=None, until_patterns=None):

        if state_file_tracker is None:
            raise Exception(f"state_file_tracker can't be None")

        self.state_file_tracker = state_file_tracker
        if until_patterns is None:
            self._queue_only_func = lambda i: False
        else:
            funcs = [
                lambda task_key: fnmatch.fnmatch(task_key, until_pattern)
                for until_pattern in until_patterns
            ]

            def _queue_only(k):
                for f in funcs:
                    if f(k):
                        return True
                return False

            self._queue_only_func = _queue_only

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

    def task(self, key=None, task_conf=None, is_slurm_array_child=False):
        if key is None:
            raise Exception(f"key can't be none")

        if task_conf is None:
            task_conf = TaskConf.default()

        if task_conf.executer_type == "process" and is_slurm_array_child:
            raise Exception(f"task {key} has is_slurm_array_child=True, and executer_type='process', should be 'slurm'")

        return TaskBuilder(key, task_conf=task_conf, dsl=self, is_slurm_array_child=is_slurm_array_child)

    def file(self, p):
        return Path(p)

    def pipeline_instance_dir(self):
        return self.state_file_tracker.pipeline_instance_dir

    def query_all_or_nothing(self, glob_expression, state="completed"):

        def is_required_state(state_file):
            if state == "completed":
                return state_file.is_completed()
            elif state == "ready":
                return state_file.is_ready_or_passed()
            else:
                raise Exception(f"invalid state: {state}, must be 'completed', or 'ready'")

        query_all_matches_count = 0
        query_with_required_state_matches = []
        match_all_impossible = False

        for state_file in self.state_file_tracker.all_state_files():
            if fnmatch.fnmatch(state_file.task_key, glob_expression):
                query_all_matches_count += 1
                if is_required_state(state_file):
                    query_with_required_state_matches.append(state_file)
                elif state == "completed" and self._queue_only_func(state_file.task_key):
                    self._pending_queries.pop(glob_expression, None)
                    self._keys_of_tasks_waiting_for_external_events.discard(state_file.task_key)
                    match_all_impossible = True

        if query_all_matches_count == 0 or match_all_impossible:
            return []
        elif query_all_matches_count == len(query_with_required_state_matches):
            self._pending_queries.pop(glob_expression, None)

            class Match:
                def __init__(self):
                    self.tasks = sorted([
                        TaskProcess(
                            state_file.control_dir(), ensure_all_upstream_deps_complete=False, no_logger=True
                        ).resolve_task(
                            state_file
                        )
                        for state_file in query_with_required_state_matches
                    ], key=lambda t: t.key)

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

            if isinstance(task, TaskBuilder):
                raise InvalidTaskDefinition(task, self._task_generator)

            is_new, state_file = self.state_file_tracker.create_true_state_if_new_else_fetch_from_memory(task)
            if is_new:
                new_generated_tasks += 1
                upstream_dep_keys = task.upstream_dep_keys()

                if state_file.is_slurm_array_child:
                    self.state_file_tracker.set_ready_on_disk_and_in_memory(state_file.task_key)
                    continue

                if self._register_upstream_task_dependencies_if_any_and_return_ready_status(
                    task.key, upstream_dep_keys
                ):
                    if self._queue_only_func(task.key) and (state_file.is_waiting() or state_file.is_ready()):
                        self.state_file_tracker.set_ready_on_disk_and_in_memory(state_file.task_key)
                    else:
                        yield state_file


        if new_generated_tasks == 0 and len(self._pending_queries) == 0:
            self._generator_exhausted = True

    def iterate_tasks_to_launch(self):

        if self._no_runnable_tasks_left:
            raise AllRunnableTasksCompletedOrInError()

        def register_pre_launch(state_file):
            self._keys_of_tasks_waiting_for_external_events.add(state_file.task_key)
            #if state_file.is_slurm_array_child:
            #    self.state_file_tracker.set_ready_on_disk_and_in_memory(state_file.task_key)
            #    # child tasks can also have upstream deps (!), we simply inhibit launches
            #    pass
            if state_file.is_parent_task:
                with open(os.path.join(state_file.control_dir(), "task-keys.tsv")) as tc:
                    for k in tc:
                        k = k.strip()
                        if k != "":
                            self._keys_of_tasks_waiting_for_external_events.add(k)
                            #self.state_file_tracker.fetch_true_state_and_update_memory_if_changed(k)
                yield state_file
            else:
                self.state_file_tracker.register_pre_launch(state_file)
                yield state_file

        if self._task_generator is not None:
            for state_file in self._ready_state_files_from_generator():
                yield from register_pre_launch(state_file)
        elif self._ready_state_files_loaded_from_disk is not None:
            for state_file in self._ready_state_files_loaded_from_disk:
                yield from register_pre_launch(state_file)
            self._ready_state_files_loaded_from_disk = []
        else:
            raise Exception(
                f"{type(StateMachine)} must be constructed with either a dag_generator, or " +
                "load_from_instance_dir() must be invoked before calling this method"
            )

        def populate_task_keys_of_dep_graph_changing_status_changes():
            newly_completed_task_keys = []
            newly_failed_task_keys = []

            for task_key in self._keys_of_tasks_waiting_for_external_events:
                true_state_file, _ = self.state_file_tracker.fetch_true_state_and_update_memory_if_changed(task_key)
                if true_state_file is not None:
                    if true_state_file.is_completed():
                        newly_completed_task_keys.append(task_key)
                    elif true_state_file.is_failed():
                        self._keys_of_failed_tasks.add(true_state_file.task_key)
                        newly_failed_task_keys.append(true_state_file.task_key)

            self._keys_of_tasks_waiting_for_external_events.difference_update(newly_completed_task_keys)
            self._keys_of_tasks_waiting_for_external_events.difference_update(newly_failed_task_keys)
            return newly_completed_task_keys

        newly_completed_task_keys = populate_task_keys_of_dep_graph_changing_status_changes()

        # track state_file changes, update dependency map, and yield newly ready tasks
        for key_of_waiting_task in list(self._keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys.keys()):
            if self._register_completion_of_upstream_tasks(key_of_waiting_task, newly_completed_task_keys):
                state_file_of_ready_task = self.state_file_tracker.lookup_state_file_from_memory(key_of_waiting_task)
                yield from register_pre_launch(state_file_of_ready_task)

        if self._generator_exhausted:
            if len(self._keys_of_tasks_waiting_for_external_events) == 0:
                self._no_runnable_tasks_left = True
