import json

from dry_pipe import DryPipe
from test_utils import file_in_test_suite


@DryPipe.python_call()
def add_all(x, __task_key, **kwargs):

    y = kwargs["y"]
    z = kwargs["z"]
    f = kwargs["f"]

    with open(f) as _f:
        o = json.loads(_f.read())
        if o["x"] != 123:
            raise Exception(f"unexpected value in {f}")

    if __task_key != "t1":
        raise Exception(f"__task_key lost !")

    return {
        "result": x + y + z
    }


def gen_dag(dsl):

    yield dsl.task(
        key="t1"
    ).consumes(
        x=dsl.val(1),
        ** {
            "y": dsl.val(0.1),
            "z": dsl.val(10),
            "f": dsl.file(file_in_test_suite("test-data.json"))
        }
    ).produces(
        result=dsl.var(float)
    ).calls(add_all)()


def validate(test_case, pipeline_instance):
    t1 = pipeline_instance.tasks["t1"]

    test_case.assertEqual(t1.get_state().state_name, "completed")

    res = t1.out.result.fetch()

    if res != 11.1:
        raise Exception(f"expected 110.01, got {res}")
