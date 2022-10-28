import glob
import logging
import os
import pathlib

from dry_pipe.script_lib import PortablePopen

TASK_ACTIONS = {

    "restart": ["failed", "timed-out", "completed"],
    "pause": [],
    "kill": []
}

PIPELINE_ACTIONS = {
    "kill-all-and-pause": [],
    "pause-all": [],
    "reset-all": []
}


ALL_TASK_ACTIONS = [a for a, l in TASK_ACTIONS.items()]


logger = logging.getLogger(__name__)

class TaskAction:

    @staticmethod
    def fetch_all_actions(pipeline_instance_dir):
        for action_file in glob.glob(os.path.join(pipeline_instance_dir, ".drypipe", "*", "action.*")):
            yield TaskAction.load_from_file(action_file)

    @staticmethod
    def load_from_task_control_dir(d):

        action_files = list(glob.glob(os.path.join(d, "action.*")))
        c = len(action_files)

        if c > 1:
            raise Exception(f"{c} action files found in {d}")
        elif c == 0:
            return None

        return TaskAction.load_from_file(action_files[0])

    @staticmethod
    def load_from_file(action_file):

        action, action_name = os.path.basename(action_file).split(".")

        if action_name.startswith("restart"):
            step = 0
            if "-" in action_name:
                action_name, step = action_name.split("-")

            return TaskAction(action_name, action_file, int(step))

        return TaskAction(action_name, action_file)

    @staticmethod
    def load_from_task_state(task_state):
        return TaskAction.load_from_task_control_dir(task_state.control_dir())

    @staticmethod
    def submit(pipeline_instance_dir, task_key, action_name, step=None):

        TaskAction.validate_action_name(action_name)

        pipeline_work_dir = os.path.join(pipeline_instance_dir, ".drypipe")

        control_dir = os.path.join(pipeline_work_dir, task_key)

        existing_action = TaskAction.load_from_task_control_dir(control_dir)

        if existing_action is not None:
            raise Exception(
                f"can't submit action to {control_dir}, action '{existing_action.action_name}' already exists."
            )

        if action_name == "restart":
            if step is not None:
                action_name = f"{action_name}-{step}"

        p = os.path.join(control_dir, f"action.{action_name}")

        pathlib.Path(p).touch(exist_ok=False)

        pathlib.Path(os.path.join(pipeline_work_dir, "sync")).touch(exist_ok=True)

    @staticmethod
    def validate_action_name(action_name):
        if action_name not in ALL_TASK_ACTIONS:
            raise Exception(f"invalid action_name: {action_name}")

    def __init__(self, action_name, file, step=None):

        TaskAction.validate_action_name(action_name)

        self.action_name = action_name
        self.file = file
        task_control_dir = os.path.dirname(file)
        self.task_key = os.path.basename(task_control_dir)
        self.step = step
        self.pipeline_instance_dir = os.path.dirname(os.path.dirname(task_control_dir))

    def delete(self):
        os.remove(self.file)

    def is_restart(self):
        return self.action_name == "restart"

    def is_pause(self):
        return self.action_name == "pause"

    def is_kill(self):
        return self.action_name == "kill"

    def is_deactivated(self):
        return self.is_pause()

    def unpause(self):
        self.delete()

    def do_it(self, task, executor):

        if self.action_name == "pause":
            return

        task_state = task.get_state()
        #executor = executer_getter.get_executer(task.task_conf)

        if self.is_restart():
            self._do_restart(task, task_state, executor)
        elif self.is_kill():
            self._do_kill(task, task_state, executor)

        self.delete()

    def _do_pause(self):
        pathlib.Path(os.path.join(
            os.path.dirname(self.file),
            "action.pause"
        )).touch()

    def _do_restart(self, task, task_state, executor):

        if task_state.is_failed() or task_state.is_completed() or \
                task_state.is_timed_out() or task_state.is_completed_unsigned() or task_state.is_killed():

            if task.is_remote():
                logger.info("will clear state of remote task %s", task.key)
                executor.clear_remote_task_state(task)


            logger.info("will requeue task %s", task.key)
            task_state.transition_to_queued(increment_retry=False, step=self.step, force=True)
            task.reset_logs()
        else:
            raise Exception(f"restart not yet implemented for tasks in state {task_state.state_name}")

    def _do_kill(self, task, task_state, executor):

        if task_state.is_failed() or task_state.is_completed() or \
                task_state.is_timed_out() or task_state.is_completed_unsigned():
            return

        if task.is_remote():
            if executor.slurm is not None:
                executor.kill_slurm_task(task)
                executor.clear_remote_task_state(task)
                task_state.transition_to_killed()
            else:
                task.log_control_error(
                    f"Warning: remote killing of non slurm tasks not yet implemented, use scancel or kill -9")
        else:

            from dry_pipe.internals import Local
            from dry_pipe.internals import Slurm

            if isinstance(executor, Local):
                executor.kill_task(task.key)
                task_state.transition_to_killed()
            elif isinstance(executor, Slurm):

                slurm_job_id_file = os.path.join(
                    task_state.v_abs_control_dir(),
                    "slurm_job_id"
                )

                if os.path.exists(slurm_job_id_file):
                    with open(slurm_job_id_file) as f:
                        slurm_job_id = f.read().strip()

                        call_scancel(slurm_job_id)
                else:
                    task.log_control_error(
                        f"Warning: can't find slurm job_id, file {slurm_job_id_file} missing.")

        self._do_pause()


def call_scancel(job_id):

    with PortablePopen(
        ["scancel", job_id]
    ) as p:
        p.wait_and_raise_if_non_zero()
