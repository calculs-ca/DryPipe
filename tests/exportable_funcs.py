import json
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

def save_crash_count(pipeline_instance_dir, i, c):
    cc = Path(pipeline_instance_dir, f"crash_count_{i}")
    with open(cc, "w") as f:
        f.write(str(c))

def crash_if(i, pipeline_instance_dir, step_idx):
    cp, c = load_crash_plan_and_crash_count(pipeline_instance_dir, i, step_idx)

    should_crash = cp[step_idx][i] - c

    print(f"i: {i}, step_idx: {step_idx}, c: {c},  should_crash {should_crash}")

    if should_crash > 0:
        save_crash_count(pipeline_instance_dir, i, c + 1)
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

