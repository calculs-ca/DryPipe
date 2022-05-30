import os
import time


def poll_and_wait_until_true(func, max_seconds=10, sleep_between_trials=1, is_slurm=False):

    if is_slurm:
        sleep_between_trials = 5
        max_seconds = 100

    for i in range(0, max_seconds):
        if not func():
            time.sleep(sleep_between_trials)
        else:
            return False

    return True


def wait(the_task, is_slurm=False):
    max_seconds = 10 if not is_slurm else 100
    sleep_between_trials = 1 if not is_slurm else 3

    poll_and_wait_until_true(lambda: the_task.get_state().is_completed(), max_seconds, sleep_between_trials)

    if the_task.has_failed():
        raise Exception(f"{the_task} has failed")


def wait_for_step(the_task, step_number, is_slurm=False):
    max_seconds = 10 if not is_slurm else 100
    sleep_between_trials = 1 if not is_slurm else 3

    def check():
        if the_task.has_failed():
            raise Exception(f"{the_task} has failed")
        if the_task.next_step_number() == step_number:
            print("")
            return True

        return False

    exhausted_time = poll_and_wait_until_true(check, max_seconds, sleep_between_trials)

    if exhausted_time:
        raise Exception(f"exhausted time")


def load_file_as_string(f):
    with open(f) as f:
        return f.read()


def rewrite_file(file, content):
    with open(file, "w") as f:
        f.write(content)


def is_ip29():

    return os.path.exists("/nfs3_ib/ip29-ib")


def slurm_conf(dsl):
    return dsl.slurm(
        account="def-xroucou_cpu",
        sbatch_options=[
            "--time=0:5:00"
        ]
    )


def count(pipeline, pred):
    return sum([
        1 if pred(task) else 0
        for task in pipeline.tasks
    ])

