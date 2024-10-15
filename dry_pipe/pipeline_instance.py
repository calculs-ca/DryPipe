import logging
import time
from itertools import groupby

from dry_pipe.state_machine import StateMachine, AllRunnableTasksCompletedOrInError
from dry_pipe.state_file_tracker import StateFileTracker
from dry_pipe.task_process import TaskProcess

logger = logging.getLogger(__name__)

class PipelineInstance:

    def __init__(self, pipeline, pipeline_instance_dir):
        self.pipeline = pipeline
        self.state_file_tracker = StateFileTracker(pipeline_instance_dir)
        if not self.state_file_tracker.instance_exists():
            self.prepare_instance_dir()

    def prepare_instance_dir(self):
        self.state_file_tracker.prepare_instance_dir({
            "__pipeline_code_dir": self.pipeline.pipeline_code_dir,
            "__containers_dir": self.pipeline.containers_dir
        })

    def run_sync(self, until_patterns=None, run_tasks_in_process=True, monitor=None):
        self._run(until_patterns, run_tasks_in_process, True, monitor, [0, 0, 0, 1])

    def run(self, until_patterns=None, monitor=None, restart_failed=False, reset_failed=False):
        self._run(
            until_patterns, False, False,
            monitor, [0, 1, 5, 10], restart_failed, reset_failed
        )

    def _run(
        self, until_patterns, run_tasks_in_process, run_tasks_sync, monitor, sleep_schedule,
        restart_failed=False, reset_failed=False
    ):

        if until_patterns is not None and not isinstance(until_patterns, list):
            raise Exception(f"invalid type for until_patterns: {type(until_patterns).__name__}, must be List[str]")

        state_machine = StateMachine(
            self.state_file_tracker,
            self.pipeline.task_generator,
            until_patterns=until_patterns
        )

        def mon():
            if monitor is not None:
                monitor.dump(
                    state_machine.state_file_tracker.all_state_files()
                )

        def iterate_work_rounds(restart_failed, reset_failed):
            try:
                sleep_idx = 0
                max_sleep_idx = len(sleep_schedule) - 1
                while True:
                    c = 0
                    for state_file in state_machine.iterate_tasks_to_launch(
                            monitor=monitor, restart_failed=restart_failed, reset_failed=reset_failed
                    ):
                        control_dir = state_file.control_dir()
                        as_subprocess = not run_tasks_in_process
                        wait_for_completion = run_tasks_sync
                        tp = TaskProcess(control_dir, as_subprocess=as_subprocess,
                                         wait_for_completion=wait_for_completion)
                        yield lambda: tp.run(by_pipeline_runner=True), None
                        c += 1
                        sleep_idx = 0

                    if c == 0:
                        if sleep_idx < max_sleep_idx:
                            sleep_idx += 1
                        yield None, sleep_schedule[sleep_idx]

                    restart_failed = False
                    reset_failed = False

            except AllRunnableTasksCompletedOrInError:
                logger.info(f"no more tasks to launch")
                yield None, None

        for func, suggested_sleep in iterate_work_rounds(restart_failed, reset_failed):
            if func is not None:
                func()
                mon()
            elif suggested_sleep is not None:
                time.sleep(suggested_sleep)
                mon()
            else:
                mon()

            restart_failed = False
            reset_failed = False


    def query(self, glob_pattern, include_incomplete_tasks=False):
        yield from self.state_file_tracker.load_tasks_for_query(
            glob_pattern, include_non_completed=include_incomplete_tasks
        )

    def lookup_single_task_or_none(self, task_key, include_incomplete_tasks=False):
        return self.state_file_tracker.load_single_task_or_none(
            task_key, include_non_completed=include_incomplete_tasks
        )

    def lookup_single_task(self, task_key, include_incomplete_tasks=False):
        task = self.lookup_single_task_or_none(task_key, include_incomplete_tasks)
        if task is not None:
            return task
        else:
            raise Exception(f"expected a task with key {task_key}, found none")

    def restart_failed(self):
        self.state_file_tracker

class Monitor:
    def __init__(self, task_grouper=None):

        if task_grouper is None:
            self.task_grouper = lambda s: self.default_grouper(s)
        else:
            self.task_grouper = task_grouper

    def on_task_fail(self, state_file):
        pass

    def default_grouper(self, task_key):
        if "." in task_key:
            task_group_key = task_key.rsplit(".", 1)[0]
            task_group_key = f"{task_group_key}.*"
            return task_group_key
        else:
            return task_key

    def dump(self, all_state_files):
        for task_group, state_counts in self.produce_report(all_state_files):
            dump_counts = ",".join([
                f"{s}: {c}" for s, c in state_counts
            ])
            #print(f"{task_group}: ({dump_counts})")

    def produce_report(self, all_state_files):
        def g():
            for state_file in all_state_files:
                key, state, _ = state_file.key_state_step()

                task_group_key = self.task_grouper(key)

                if state.startswith("_"):
                    state = "launched"

                yield task_group_key, state

        def gf(t):
            task_group_key, state = t
            return task_group_key

        for task_group, states in groupby(sorted(g(), key=gf), key=gf):
            yield task_group, [
                (state, len(list(all_states)))
                for state, all_states in groupby(sorted([s for _, s in states]))
            ]
