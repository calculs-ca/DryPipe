from dry_pipe import DryPipe

test_sum_args_python = {
    "args": [1, 2, 3],
    "init_func": lambda: 321,
    "expects": {
        "res": 6
    }
}


@DryPipe.python_call(tests=[test_sum_args_python])
def sum_args_python(a1, a2, a3, test=None):

    return {
        "res": int(a1) + int(a2) + int(a3)
    }


def test_a_palooza(dsl):
    yield dsl.task(
        key="sum_args_python"
    ).calls(sum_args_python)


def pipeline():
    return DryPipe.create_pipeline(test_a_palooza)
