
from dry_pipe import DryPipe


@DryPipe.python_call()
def multiply_by_x(x, y, f):

    with open(f) as _f:
        _f.write("magic")

    return {
        "result": x * y
    }


def pipeline(dsl):

    t1 = dsl.task(
        key="t1"
    ).produces(
        x=dsl.var(int),
        y=dsl.var(int),
        f=dsl.file("magic-file.txt")
    ).calls("""
        #!/usr/bin/env bash
        export x=12
        export y=34
        echo "magic" > $f
    """)()

    yield t1

    yield dsl.task(
        key="t2"
    ).consumes(
        t1.out.x, #equivalent to: x=t1.out.x:
        z=t1.out.y,
        f_magic=t1.out.f
    ).produces(
        r=dsl.var(int)
    ).calls("""
        #!/usr/bin/env bash
        echo "x: $x"
        echo "z: $z"
        export r=$(( $x + $z ))    
    """)()


def validate_two_task_pipeline(pipeline_instance):
    t2 = pipeline_instance.tasks["t2"]

    task_state = t2.get_state()

    if not task_state.is_completed():
        raise Exception(f"expected completed, got {task_state.state_name}")

    res = t2.out.r.fetch()
    if res != 46:
        raise Exception(f"expected 46, got {res}")
