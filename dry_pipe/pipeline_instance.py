import time

from dry_pipe.pipeline_runner import PipelineRunner
from dry_pipe.state_machine import StateMachine
from dry_pipe.core_lib import StateFileTracker


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

    def run_sync(self, until_patterns=None, run_tasks_in_process=True):

        if until_patterns is not None and not isinstance(until_patterns, list):
            raise Exception(f"invalid type for until_patterns: {type(until_patterns).__name__}, must be List[str]")

        state_machine = StateMachine(
            self.state_file_tracker,
            self.pipeline.task_generator,
            until_patterns=until_patterns
        )
        pr = PipelineRunner(
            state_machine,
            run_tasks_in_process=run_tasks_in_process,
            run_tasks_async=False,
            sleep_schedule=[0, 0, 0, 1]
        )

        for func, suggested_sleep in pr.iterate_work_rounds():
            if func is not None:
                func()
            elif suggested_sleep is not None:
                time.sleep(suggested_sleep)
            else:
                break

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
