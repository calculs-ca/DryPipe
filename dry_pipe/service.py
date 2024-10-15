import os.path
import time
from pathlib import Path

from dry_pipe.state_machine import StateMachine, AllRunnableTasksCompletedOrInError
from dry_pipe.task_process import TaskProcess


class RunningPipelineInstance:

    def __init__(self, pipeline, pipeline_state_file):
        self.pipeline = pipeline
        self.pipeline_state_file = pipeline_state_file
        self.pipeline_instance = pipeline.create_pipeline_instance(Path(pipeline_state_file).parent.parent)
        self.state_machine = StateMachine(
            self.pipeline_instance.state_file_tracker,
            self.pipeline_instance.pipeline.task_generator
        )

    def _change_state(self, new_state):
        next_state = Path(self.pipeline_instance.state_file_tracker.pipeline_work_dir, new_state)
        os.rename(
            self.pipeline_state_file,
            next_state
        )
        self.pipeline_state_file = next_state

    def set_running(self):
        self._change_state("state.running")

    def set_stopped(self):
        self._change_state("state.stopped")

    def is_running(self):
        return str(self.pipeline_state_file).endswith(".running")


class PipelineRunner:

    def __init__(self, instances_dir_to_pipelines, run_sync=False, run_tasks_in_process=False, sleep_schedule = [0, 0, 0, 1, 5]):
        self.instances_dir_to_pipelines = instances_dir_to_pipelines
        self.run_sync = run_sync
        self.run_tasks_in_process = run_tasks_in_process
        self.pipeline_instances = []
        self.sleep_schedule = sleep_schedule


    def iterate_work(self):
        sleep_idx = 0

        while True:

            work_done = 0

            for instances_dir, pipeline in self.instances_dir_to_pipelines.items():

                for state_file_path in Path(instances_dir).glob("*/.drypipe/state.*"):

                    state_file_path = Path(state_file_path).absolute()
                    pipeline_state_file = os.path.basename(state_file_path)

                    if pipeline_state_file.endswith(".ready"):
                        rpi = RunningPipelineInstance(pipeline,state_file_path)
                        self.pipeline_instances.append(rpi)
                        rpi.set_running()
                        rpi.pipeline_instance.prepare_instance_dir()

                        work_done += 1

            for running_pipeline_instance in self.pipeline_instances:

                if running_pipeline_instance.is_running():
                    try:
                        for state_file in running_pipeline_instance.state_machine.iterate_tasks_to_launch():
                            control_dir = state_file.control_dir()
                            tp = TaskProcess(
                                control_dir,
                                as_subprocess=not self.run_tasks_in_process,
                                wait_for_completion=self.run_sync
                            )
                            tp.run(by_pipeline_runner=True)
                            work_done += 1
                    except AllRunnableTasksCompletedOrInError:
                        running_pipeline_instance.set_stopped()
                        work_done += 1

            if work_done > 0:
                sleep_idx = 0
                continue
            else:
                if sleep_idx < len(self.sleep_schedule) - 1:
                    sleep_idx += 1

            suggested_sleep = self.sleep_schedule[sleep_idx]

            yield suggested_sleep


    def watch(self):
        for suggested_sleep in self.iterate_work():
            if suggested_sleep is None:
                break
            elif suggested_sleep > 0:
                time.sleep(suggested_sleep)

