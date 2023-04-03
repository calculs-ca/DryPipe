
from dry_pipe import DryPipe


@DryPipe.python_call()
def multiply_by_x(x, y):
    return {
        "result": x * y
    }


def pipeline(dsl):
    yield dsl.task(
        key="multiply_x_by_y"
    ).inputs(
        x=3, y=4
    ).outputs(
        result=int
    ).calls(
        multiply_by_x
    )()


def validate_single_task_pipeline(pipeline_instance):
    multiply_x_by_y_task = pipeline_instance.tasks["multiply_x_by_y"]

    task_state = multiply_x_by_y_task.get_state()

    if not task_state.is_completed():
        raise Exception(f"expected completed, got {task_state.state_name}")

    res = multiply_x_by_y_task.out.result.fetch()
    if res != 15:
        raise Exception(f"expected 15, got {res}")
