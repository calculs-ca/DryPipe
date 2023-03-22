import json
import os

from dry_pipe import DryPipe
from test_utils import file_in_test_suite


@DryPipe.python_call()
def add_all(x, r1, __task_key, **kwargs):

    y = kwargs["y"]
    z = kwargs["z"]
    f = kwargs["f"]

    with open(f) as _f:
        o = json.loads(_f.read())
        if o["x"] != 123:
            raise Exception(f"unexpected value in {f}")

    if __task_key != "t1":
        raise Exception(f"__task_key lost !")

    cmd = f"cp {f} {r1}"
    print(cmd)
    os.system(cmd)

    return {
        "result": x + y + z
    }

@DryPipe.python_call()
def f1(r, **kwargs):

    if r != 11.1:
        raise Exception(f"expected 110.01, got {r}")

    r1 = kwargs.get("r1")
    if r1 is None:
        raise Exception(f"expected 'r1' in **kwargs")

    if not os.path.exists(r1):
        raise Exception(f"file {r1} should exist")

    with open(r1) as _f:
        o = json.loads(_f.read())
        if o["x"] != 123:
            raise Exception(f"unexpected value in {r1}")

    return {
        "r2": 543
    }

def gen_dag(dsl):

    t1 = dsl.task(
        key="t1"
    ).consumes(
        x=dsl.val(1),
        ** {
            "y": dsl.val(0.1),
            "z": dsl.val(10),
            "f": dsl.file(file_in_test_suite("test-data.json"))
        }
    ).produces(
        r1=dsl.file("r1.json"),
        result=dsl.var(float)
    ).calls(add_all)()

    yield t1

    yield dsl.task(
        key="t2"
    ).consumes(
        x=dsl.val(5),
        r1=t1.out.r1,
        r=t1.out.result
    ).produces(
        r2=dsl.var(int)
    ).calls(
        f1
    )()


def validate(test_case, pipeline_instance):
    t1 = pipeline_instance.tasks["t1"]

    test_case.assertEqual(t1.get_state().state_name, "completed")

    res = t1.out.result.fetch()

    if res != 11.1:
        raise Exception(f"expected 110.01, got {res}")

    t2 = pipeline_instance.tasks["t2"]

    test_case.assertEqual(t2.get_state().state_name, "completed")
