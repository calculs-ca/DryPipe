import json
import logging
import os.path
import shutil
import time
from pathlib import Path

from dry_pipe.state_machine import StateMachine, AllRunnableTasksCompletedOrInError
from dry_pipe.task_process import TaskProcess

logger = logging.getLogger(__name__)

def _pipeline_state_and_pid_from_pipeline_state_file(pipeline_state_file):

    state_file_path = Path(pipeline_state_file).absolute()

    bn = os.path.basename(state_file_path)
    state = bn[6:]
    pid = str(state_file_path.parent.parent.absolute())

    return state, pid


class PipelineInstanceAccessor:

    def __init__(self, pipeline_type, pipeline_state_file, logger=None):
        self.pipeline_type= pipeline_type
        self.pipeline_state_file = pipeline_state_file
        self.pipeline_instance = pipeline_type.pipeline.create_pipeline_instance(
            Path(pipeline_state_file).parent.parent, logger
        )
        self.state_machine = StateMachine(
            self.pipeline_instance.state_file_tracker,
            self.pipeline_instance.pipeline.task_generator,
            instance_logger=self.pipeline_instance.instance_logger
        )


    def task_state_by_key(self, task_key):
        return self.state_machine.state_file_tracker.load_task_from_state_file(task_key)

    def reset_task(self, task_key, preserve_output=False):
        dirz = [
            Path(self.state_machine.state_file_tracker.pipeline_work_dir, task_key)
        ]

        if not preserve_output:
            dirz.append(Path(self.state_machine.state_file_tracker.pipeline_output_dir, task_key))

        for d in dirz:
            if d.exists():
                shutil.rmtree(d)

    def instance_dir(self):
        return self.pipeline_instance.state_file_tracker.pipeline_instance_dir

    def state(self):
        return Path(self.pipeline_state_file).name.split(".")[1]

    def latest_state(self):

        state, pid = _pipeline_state_and_pid_from_pipeline_state_file(
            self.pipeline_instance.state_file_tracker.load_pipeline_state_file()
        )

        return state

    def _change_state(self, new_state):
        next_state = Path(self.pipeline_instance.state_file_tracker.pipeline_work_dir, new_state)
        os.rename(
            self.pipeline_state_file,
            next_state
        )
        self.pipeline_state_file = next_state

    def start(self):

        error_messages_by_error_code, _ = self.pipeline_type.validator(self.instance_dir())

        if bool(error_messages_by_error_code):
            return {"status": "error", "error_messages_by_error_code": error_messages_by_error_code}

        self._change_state("state.ready")

        return {"status": "ok"}

    def set_running(self):
        self._change_state("state.running")

    def set_stopped(self):
        self._change_state("state.stopped")

    def set_completed(self):
        self._change_state("state.completed")

    def is_running(self):
        return str(self.pipeline_state_file).endswith(".running")

    def is_completed(self):
        return str(self.pipeline_state_file).endswith(".completed")

    def load_conf_as_json(self):
        return self.pipeline_instance.state_file_tracker.load_conf_as_json()

    def args_as_json(self):
        return self.pipeline_instance.state_file_tracker.load_args_as_json()

    def update_args(self, json_args):

        #self.pipeline_type.validator(json_args, self.pipeline_instance.pipeline_instance_dir())

        self.pipeline_instance.state_file_tracker.save_args_as_json(json_args)

    def completed_files(self):
        if self.pipeline_type.complete_func is None:
            return []

        for res, it in self.pipeline_type.complete_func(
            self.pipeline_instance.pipeline_instance_dir()
        ):
            return list(it)

    def check_if_completed(self):
        if self.pipeline_type.complete_func is None:
            return None

        for res, it in self.pipeline_type.complete_func(
            self.pipeline_instance.pipeline_instance_dir()
        ):
            return res

        return False


class PipelineRunner:

    def __init__(self, config_generator, run_sync=False, run_tasks_in_process=False, sleep_schedule = [0, 0, 0, 1, 5]):


        self.instances_dir_to_pipeline_types = {
            instances_dir: pipeline_type
            for instances_dir, pipeline_type in config_generator
        }

        self.run_sync = run_sync
        self.run_tasks_in_process = run_tasks_in_process
        self.pipeline_instances = {}
        self.sleep_schedule = sleep_schedule

    def pipeline_instance_exists(self, instances_dir_basename, name):

        for instances_dir, _ in self.instances_dir_to_pipeline_types.items():
            if instances_dir.endswith(f"/{instances_dir_basename}"):
                if Path(instances_dir, name).exists():
                    return True

        return False

    def create_pipeline_instance(self, instances_dir_basename, name, args):
        for instances_dir, pipeline_type in self.instances_dir_to_pipeline_types.items():
            if instances_dir.endswith(f"/{instances_dir_basename}"):
                pipeline_instance_dir = Path(instances_dir, name)
                if pipeline_instance_dir.exists():
                    raise Exception(f"pipeline already exists: {instances_dir}/{name}")
                else:
                    pipeline_instance_dir.mkdir(exist_ok=False, parents=True)
                    wd = Path(pipeline_instance_dir, ".drypipe")
                    wd.mkdir()
                    Path(wd, "state.not-ready").touch()
                    with open(Path(pipeline_instance_dir, "args.json"), "w") as f:
                        json.dump(args, f, indent=4, sort_keys=True)

                    if pipeline_type.init_func is not None:
                        pipeline_type.init_func(pipeline_instance_dir)

                    return {"pid": str(pipeline_instance_dir)}

        raise Exception(f"unknown instances dir basename {instances_dir_basename}")

    def get_pipeline_types(self):

        def g():
            for instances_dir, pipeline_type in self.instances_dir_to_pipeline_types.items():
                yield {
                    **pipeline_type.as_dict(),
                    "instances_dir": instances_dir,
                    "instances_dir_basename": Path(instances_dir).name,
                }

        return list(g())

    def get_parent_instances_dir(self, path):
        for instances_dir, pipeline_and_validator in self.instances_dir_to_pipeline_types.items():
            if Path(path).is_relative_to(instances_dir):
                return Path(instances_dir, Path(path).name)

        return None

    def iterate_pipelines_state_pids(self):
        for instances_dir, pipeline_type in self.instances_dir_to_pipeline_types.items():

            for state_file_path in Path(instances_dir).glob("*/.drypipe/state.*"):

                state, pid = _pipeline_state_and_pid_from_pipeline_state_file(state_file_path)
                yield pipeline_type, state, pid, state_file_path


    def iterate_work(self):
        sleep_idx = 0

        while True:

            work_done = 0

            for pipeline_type, state, pid, state_file_path in self.iterate_pipelines_state_pids():

                if state not in ["ready", "running"]: # "stopped", "not-ready", "completed"
                    continue

                if pid not in self.pipeline_instances:

                    rpi = PipelineInstanceAccessor(pipeline_type, state_file_path)
                    self.pipeline_instances[pid] = rpi
                    rpi.set_running()

                    rpi.pipeline_instance.prepare_instance_dir()
                    work_done += 1

            for pid, running_pipeline_instance in self.pipeline_instances.items():

                instance_logger = running_pipeline_instance.pipeline_instance.instance_logger

                def check_completed():
                    if running_pipeline_instance.check_if_completed():
                        running_pipeline_instance.set_completed()
                        instance_logger.info("pipeline instance completed")
                        return True

                if running_pipeline_instance.is_running():

                    instance_logger.debug("running instance, start round")

                    try:
                        for state_file in running_pipeline_instance.state_machine.iterate_tasks_to_launch():
                            control_dir = state_file.control_dir()
                            tp = TaskProcess(
                                control_dir,
                                as_subprocess=not self.run_tasks_in_process,
                                wait_for_completion=self.run_sync
                            )
                            instance_logger.info("will launch %s", tp.task_key)
                            tp.run(by_pipeline_runner=True)
                            work_done += 1
                            check_completed()
                    except AllRunnableTasksCompletedOrInError:
                        if not check_completed():
                            running_pipeline_instance.set_stopped()
                            instance_logger.info("pipeline instance stopped")
                        work_done += 1
                    except Exception as ex:
                        logger.error("Error in pipeline instance %s", pid, exc_info=ex)
                        instance_logger.error(
                            "unhandled exception %s", exc_info=ex
                        )

                else:
                    if not running_pipeline_instance.is_completed():
                        check_completed()

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

