import logging
import os
import time
import traceback
from io import StringIO
from itertools import groupby
from logging.handlers import RotatingFileHandler

from dry_pipe.core_lib import TimeLogger, current_stack_as_string
from dry_pipe.state_machine import StateMachine, AllRunnableTasksCompletedOrInError
from dry_pipe.state_file_tracker import StateFileTracker
from dry_pipe.task_process import TaskProcess

logger = logging.getLogger(__name__)

class PipelineInstance:

    def __init__(self, pipeline, pipeline_instance_dir, logger=None, instance_log_is_debug=False):
        self.pipeline = pipeline
        self.state_file_tracker = StateFileTracker(pipeline_instance_dir)
        if not self.state_file_tracker.instance_exists():
            self.prepare_instance_dir()
        self.monitor = None

        if logger is not None:
            self.instance_logger = logger
        else:
            self.instance_logger = logging.getLogger(f"pipeline-instance-logger-{os.path.basename(pipeline_instance_dir)}")
            self.instance_logger.propagate = False
            for h in self.instance_logger.handlers:
                h.close()
            self.instance_logger.handlers.clear()

            file_handler = RotatingFileHandler(
                filename=os.path.join(self.state_file_tracker.pipeline_work_dir, "instance.log"),
                maxBytes=1024 * 1024 * 10, backupCount=3
            )

            if instance_log_is_debug:
                logging_level = logging.DEBUG
            else:
                logging_level = logging.INFO

            file_handler.setLevel(logging_level)
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S%z')
            )
            self.instance_logger.addHandler(file_handler)

            self.instance_logger.setLevel(logging_level)

            self.instance_logger.info("log level: %s", logging.getLevelName(logging_level))


    def pipeline_instance_dir(self):
        return self.state_file_tracker.pipeline_instance_dir

    def prepare_instance_dir(self):
        self.state_file_tracker.prepare_instance_dir({
            "__pipeline_code_dir": self.pipeline.pipeline_code_dir,
            "__containers_dir": self.pipeline.containers_dir
        })

    def reset_state_tracker(self):
        self.state_file_tracker = StateFileTracker(self.state_file_tracker.pipeline_instance_dir)

    def run_sync(self, until_patterns=None, run_tasks_in_process=True, filters=(), sleep_schedule=None):
        self._run(until_patterns, run_tasks_in_process, True, sleep_schedule, filters=filters)

    def run(self, until_patterns=None, restart_failed=False, reset_failed=False, sleep_schedule=None):
        self._run(
            until_patterns, False, False,
            sleep_schedule, restart_failed, reset_failed
        )

    def _run(
        self, until_patterns, run_tasks_in_process, run_tasks_sync, sleep_schedule,
        restart_failed=False, reset_failed=False, filters=()
    ):

        if sleep_schedule is None:
            sleep_schedule = [0, 1, 5, 10]

        self.instance_logger.info("sleep schedule: %s", sleep_schedule)

        if until_patterns is not None and not isinstance(until_patterns, list):
            raise Exception(f"invalid type for until_patterns: {type(until_patterns).__name__}, must be List[str]")

        state_machine = StateMachine(
            self.state_file_tracker,
            self.pipeline.task_generator,
            until_patterns=until_patterns,
            instance_logger=self.instance_logger,
            filters=filters
        )

        def iterate_work_rounds(restart_failed, reset_failed):
            try:
                sleep_idx = 0
                max_sleep_idx = len(sleep_schedule) - 1
                while True:
                    c = 0
                    for state_file in state_machine.iterate_tasks_to_launch(
                        monitor=self.monitor, restart_failed=restart_failed, reset_failed=reset_failed
                    ):
                        control_dir = state_file.control_dir()
                        as_subprocess = not run_tasks_in_process
                        wait_for_completion = run_tasks_sync
                        tp = TaskProcess(
                            control_dir, as_subprocess=as_subprocess, wait_for_completion=wait_for_completion
                        )

                        def r():
                            with TimeLogger(tp.task_key, self.instance_logger.debug):
                                tp.run(by_pipeline_runner=True)

                        yield r, None
                        c += 1
                        sleep_idx = 0

                    self.instance_logger.debug("DAG round completed")

                    if c == 0:
                        if sleep_idx < max_sleep_idx:
                            sleep_idx += 1
                        yield None, sleep_schedule[sleep_idx]

                    restart_failed = False
                    reset_failed = False

            except AllRunnableTasksCompletedOrInError:
                msg = f"no more tasks to launch"
                logger.info(msg)
                self.instance_logger.info(msg)
                yield None, None

            except Exception as ex:
                self.instance_logger.debug(f"entrypoint of exception %s", current_stack_as_string())
                self.instance_logger.error(f"unexpected error in %s", exc_info=ex)
                yield None, None

        def mon():
            if self.monitor is not None:
                self.monitor.dump(
                    state_machine.state_file_tracker
                )

        for func, suggested_sleep in iterate_work_rounds(restart_failed, reset_failed):
            if func is not None:
                func()
                mon()
            elif suggested_sleep is not None:
                self.instance_logger.debug("will sleep %s", suggested_sleep)
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

    def query_all_tasks_by_key(self):
        return {
            t.key: t
            for t in self.query("*", include_incomplete_tasks=True)
        }

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
            task_group_key = task_key.split(".", 1)[0]
            task_group_key = f"{task_group_key}.*"
            return task_group_key
        else:
            return task_key

    def dump(self, state_file_tracker):

        for task_group, state_counts in self.produce_report(state_file_tracker.all_state_files()):
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
