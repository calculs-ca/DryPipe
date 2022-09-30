
from dry_pipe import DryPipe


@DryPipe.python_call()
def multiply_by_x(x, y):
    return {
        "result": int(x) * int(y)
    }


def pipeline(dsl):
    yield dsl.task(
        key="multiply_x_by_y"
    ).consumes(
        x=dsl.val(3),
        y=dsl.val(5)
    ).produces(
        result=dsl.var(int)
    ).calls(
        multiply_by_x
    )()


