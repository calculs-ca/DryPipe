import os
import pathlib

from dry_pipe.pipeline_runner import PipelineRunner
from dry_pipe.state_machine import StateFileTracker, StateMachine


class PipelineInstance:

    def __init__(self, pipeline, pipeline_instance_dir):
        self.pipeline = pipeline
        self.state_file_tracker = StateFileTracker(pipeline_instance_dir)
        self.state_file_tracker.prepare_instance_dir({
            "__pipeline_code_dir": pipeline.pipeline_code_dir,
            "__containers_dir": pipeline.containers_dir
        })

    @staticmethod
    def _hint_file(instance_dir):
        return os.path.join(instance_dir, ".drypipe", "hints")

    @staticmethod
    def guess_pipeline_from_hints(instance_dir):
        return PipelineInstance.load_hints(instance_dir).get("pipeline")

    @staticmethod
    def write_hint_file_if_not_exists(instance_dir, module_func_pipeline):
        hint_file = PipelineInstance._hint_file(instance_dir)

        if not os.path.exists(hint_file):
            pathlib.Path(os.path.dirname(hint_file)).mkdir(exist_ok=True, parents=True)
            with open(hint_file, "w") as _f:
                _f.write(f"pipeline={module_func_pipeline}\n")

    @staticmethod
    def load_hints(instance_dir):
        hf = PipelineInstance._hint_file(instance_dir)
        if not os.path.exists(hf):
            return {}

        def hints():
            with open(hf) as _hf:
                for line in _hf.readlines():
                    line = line.strip()
                    if line != "":
                        k, v = line.split("=")
                        yield k.strip(), v.strip()

        return dict(hints())

    def hints(self):
        return PipelineInstance.load_hints(self.pipeline_instance_dir)

    def output_dir(self):
        return self._publish_dir

    def remote_sites_task_confs(self, for_zombie_detection=False):

        def f(task):
            if task.is_remote():
                if not for_zombie_detection:
                    return True
                task_state = task.get_state()
                return task_state.is_step_started() or task_state.is_launched() or task_state.is_scheduled()
            else:
                return False

        return {
            task.task_conf.remote_site_key: task.task_conf
            for task in self.tasks
            if f(task)
        }.values()


    def get_state(self, create_if_not_exists=False):
        return PipelineState.from_pipeline_work_dir(self.state_file_tracker.pipeline_work_dir, create_if_not_exists)

    def run_sync(self, queue_only_pattern=None, fail_silently=True, sleep=1, run_tasks_in_process=True):

        state_machine = StateMachine(
            self.state_file_tracker,
            lambda dsl: self.pipeline.task_generator(dsl),
            queue_only_pattern=queue_only_pattern
        )
        pr = PipelineRunner(state_machine, run_tasks_in_process=run_tasks_in_process)
        pr.run_sync(fail_silently=fail_silently, sleep=sleep)

        tasks_by_keys = {
            t.key: t
            for t in self.state_file_tracker.load_tasks_for_query()
        }

        return tasks_by_keys

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
