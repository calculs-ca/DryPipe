from dry_pipe import DryPipe


#TODO :  error when CONSUME VARS are arguments:ex:
# def func(i, a, x, f):
@DryPipe.python_call()
def func(i, f):

    with open(f, "w") as _f:
        _f.write("THE_FILE_CONTENT_123")

    return {
        "x": 123,
        "a": "abc"
    }


def dag_gen(dsl):

    yield dsl.task(
        key="t1"
    ).consumes(
        i=dsl.val(123)
    ).produces(
        x=dsl.var(int),
        a=dsl.var(str),
        f=dsl.file("f.txt")
    ).calls(func)()


def validate(test_case, task):

    task_state = task.get_state()

    if not task_state.is_completed():
        raise Exception(f"expected completed, got {task_state.state_name}")


    test_case.assertEquals(task.out.f.load_as_string(), "THE_FILE_CONTENT_123")