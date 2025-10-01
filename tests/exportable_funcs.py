import glob
import json
import time
from pathlib import Path

import dry_pipe


@dry_pipe.DryPipe.python_call()
def test_func(r, i, f):

    with open(f) as _f:
        f_int = int(_f.read().strip())

        return {
            "var_result": r + i + f_int
        }


def load_crash_plan_and_crash_count(pipeline_instance_dir, i, step_idx):
    with open(Path(pipeline_instance_dir, "crash-plan.json")) as f:
        crash_plan = json.load(f)
        cc = Path(pipeline_instance_dir, f"crash_count_{i}_{step_idx}")
        if not cc.exists():
            return crash_plan, 0
        else:
            with open(cc) as f2:
                return crash_plan, int(f2.read().strip())

def save_crash_count(pipeline_instance_dir, i, c, step_idx):
    cc = Path(pipeline_instance_dir, f"crash_count_{i}_{step_idx}")
    with open(cc, "w") as f:
        f.write(str(c))

def crash_if(i, pipeline_instance_dir, step_idx):
    crash_plan, c = load_crash_plan_and_crash_count(pipeline_instance_dir, i, step_idx)

    planned_crashes = crash_plan[step_idx][i]
    should_crash = planned_crashes - c

    print(f"i: {i}, step_idx: {step_idx}, planned_crashes: {planned_crashes}, crashes so far: {c},  should_crash {should_crash}")

    if should_crash > 0:
        save_crash_count(pipeline_instance_dir, i, c + 1, step_idx)
        raise Exception("!!!")

@dry_pipe.DryPipe.python_call()
def test_step0(i, __pipeline_instance_dir):
    step_idx = 0
    crash_if(i, __pipeline_instance_dir, step_idx)


@dry_pipe.DryPipe.python_call()
def test_step1(i, __pipeline_instance_dir):
    step_idx = 1
    crash_if(i, __pipeline_instance_dir, step_idx)


@dry_pipe.DryPipe.python_call()
def test_step2(i, __pipeline_instance_dir):
    step_idx = 2
    crash_if(i, __pipeline_instance_dir, step_idx)

@dry_pipe.DryPipe.python_call()
def test_step3(i, __pipeline_instance_dir):
    step_idx = 3
    crash_if(i, __pipeline_instance_dir, step_idx)


@dry_pipe.DryPipe.python_call()
def digest_all(__pipeline_work_dir):

    c = 0

    for t in glob.glob(str(Path(__pipeline_work_dir, "t_*", "state.completed"))):
        c += 1

    print(f"-->{c}")


def append_into(s, f):
    with open(Path(f), "a") as _f:
        _f.write(f"{s}\n")


@dry_pipe.DryPipe.python_call()
def test_step0_append_to_file(i, results_file, __pipeline_instance_dir):
    step_idx = 0
    crash_if(i, __pipeline_instance_dir, step_idx)
    append_into("s0", results_file)


@dry_pipe.DryPipe.python_call()
def test_step1_append_to_file(i, results_file, __pipeline_instance_dir):
    step_idx = 1
    crash_if(i, __pipeline_instance_dir, step_idx)
    append_into("s1", results_file)


@dry_pipe.DryPipe.python_call()
def test_step2_append_to_file(i, results_file, __pipeline_instance_dir):
    step_idx = 2
    crash_if(i, __pipeline_instance_dir, step_idx)
    append_into("s2", results_file)

