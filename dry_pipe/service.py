import json
import logging
import os.path
import time
from pathlib import Path

from dry_pipe.state_machine import StateMachine, AllRunnableTasksCompletedOrInError
from dry_pipe.task_process import TaskProcess

logger = logging.getLogger(__name__)

class PipelineInstanceAccessor:

    def __init__(self, pipeline, pipeline_state_file, validator):
        self.pipeline = pipeline
        self.validator = validator
        self.pipeline_state_file = pipeline_state_file
        self.pipeline_instance = pipeline.create_pipeline_instance(Path(pipeline_state_file).parent.parent)
        self.state_machine = StateMachine(
            self.pipeline_instance.state_file_tracker,
            self.pipeline_instance.pipeline.task_generator
        )


    def task_state_by_key(self, task_key):
        return self.state_machine.state_file_tracker.load_task_from_state_file(task_key)

    def instance_dir(self):
        return self.pipeline_instance.state_file_tracker.pipeline_instance_dir

    def state(self):
        return Path(self.pipeline_state_file).name.split(".")[1]

    def _change_state(self, new_state):
        next_state = Path(self.pipeline_instance.state_file_tracker.pipeline_work_dir, new_state)
        os.rename(
            self.pipeline_state_file,
            next_state
        )
        self.pipeline_state_file = next_state

    def start(self):

        error_messages_by_error_code, _ = self.validator(self.instance_dir())

        if bool(error_messages_by_error_code):
            return {"status": "error", "error_messages_by_error_code": error_messages_by_error_code}

        self._change_state("state.ready")

        return {"status": "ok"}

    def set_running(self):
        self._change_state("state.running")

    def set_stopped(self):
        self._change_state("state.stopped")

    def is_running(self):
        return str(self.pipeline_state_file).endswith(".running")

    def load_conf_as_json(self):
        return self.pipeline_instance.state_file_tracker.load_conf_as_json()

    def args_as_json(self):
        return self.pipeline_instance.state_file_tracker.load_args_as_json()

    def update_args(self, key, value):
        args = self.pipeline_instance.state_file_tracker.load_args_as_json()
        if args is None:
            args = {}

        args[key] = value

        self.pipeline_instance.state_file_tracker.save_args_as_json(args)


class PipelineRunner:

    def __init__(self, config_generator, run_sync=False, run_tasks_in_process=False, sleep_schedule = [0, 0, 0, 1, 5]):


        self.instances_dir_to_pipelines = {
            instances_dir: (pipeline, validator)
            for instances_dir, pipeline, validator in config_generator
        }

        self.run_sync = run_sync
        self.run_tasks_in_process = run_tasks_in_process
        self.pipeline_instances = {}
        self.sleep_schedule = sleep_schedule

    def pipeline_instance_exists(self, instances_dir_basename, name):

        for instances_dir, _ in self.instances_dir_to_pipelines.items():
            if instances_dir.endswith(f"/{instances_dir_basename}"):
                if Path(instances_dir, name).exists():
                    return True

        return False

    def create_pipeline_instance(self, instances_dir_basename, name):

        for instances_dir, _ in self.instances_dir_to_pipelines.items():
            if instances_dir.endswith(f"/{instances_dir_basename}"):
                p = Path(instances_dir, name)
                if p.exists():
                    raise Exception(f"pipeline already exists: {instances_dir}/{name}")
                else:
                    p.mkdir(exist_ok=False, parents=True)
                    wd = Path(p, ".drypipe")
                    wd.mkdir()
                    Path(wd, "state.not-ready").touch()
                    return {"pid": str(p)}

        raise Exception(f"unknown instances dir basename {instances_dir_basename}")


    def iterate_pipelines_state_pids(self):
        for instances_dir, pipeline_and_validator in self.instances_dir_to_pipelines.items():

            pipeline, validator = pipeline_and_validator

            for state_file_path in Path(instances_dir).glob("*/.drypipe/state.*"):
                state_file_path = Path(state_file_path).absolute()

                bn = os.path.basename(state_file_path)
                state = bn[6:]
                pid = str(state_file_path.parent.parent.absolute())

                yield pipeline, state, pid, state_file_path, validator


    def iterate_work(self):
        sleep_idx = 0

        while True:

            work_done = 0

            for pipeline, state, pid, state_file_path, validator in self.iterate_pipelines_state_pids():

                if state not in ["ready", "running"]: # "stopped", "not-ready"
                    continue

                if pid not in self.pipeline_instances:

                    rpi = PipelineInstanceAccessor(pipeline, state_file_path, validator)
                    self.pipeline_instances[pid] = rpi
                    rpi.set_running()
                    rpi.pipeline_instance.prepare_instance_dir()
                    work_done += 1

            for pid, running_pipeline_instance in self.pipeline_instances.items():

                if running_pipeline_instance.is_running():
                    try:
                        for state_file in running_pipeline_instance.state_machine.iterate_tasks_to_launch():
                            control_dir = state_file.control_dir()
                            tp = TaskProcess(
                                control_dir,
                                as_subprocess=not self.run_tasks_in_process,
                                wait_for_completion=self.run_sync
                            )
                            logger.info("will launch %", tp.task_key)
                            tp.run(by_pipeline_runner=True)
                            work_done += 1
                    except AllRunnableTasksCompletedOrInError:
                        running_pipeline_instance.set_stopped()
                        work_done += 1
                    except Exception as ex:
                        logger.error("Error in pipeline instance %s", pid, exc_info=ex)

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
                logger.debug("will sleep %s", suggested_sleep)
                time.sleep(suggested_sleep)

