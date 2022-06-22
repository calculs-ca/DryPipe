import datetime
from time import sleep

from dry_pipe import DryPipe, cli


def pipeline_consuming_upstream_vars_with_same_name(dsl):

    def define_t(i):
        return dsl.task(
            key=f"t.{i}"
        ).consumes(
            v=dsl.val(i)
        ).produces(
            a=dsl.var(int)
        ).calls(
            produce_a
        )()

    t1 = define_t(1)
    yield t1

    t2 = define_t(2)
    yield t2

    yield dsl.task(
        key=f"consume_from_t1_and_t2"
    ).consumes(
        a_t1=t1.out.a,
        a_t2=t2.out.a
    ).produces(
        result=dsl.var(int)
    ).calls(
        validate_output_from_t1_and_t2
    )()


@DryPipe.python_call()
def produce_a(v):
    return {
        "a": int(v)
    }

@DryPipe.python_call()
def validate_output_from_t1_and_t2(a_t1, a_t2):

    if int(a_t1) != 1:
        raise Exception(f"expected 1, got {a_t1}")

    if int(a_t2) != 2:
        raise Exception(f"expected 2, got {a_t2}")


def __sleep(i):

    start_t = datetime.datetime.now()
    sleep(1)
    end_t = datetime.datetime.now()

    return {
        f"start_t_{i}": start_t.strftime('%Y-%m-%dT%H:%M:%S.%f'),
        f"end_t_{i}": end_t.strftime('%Y-%m-%dT%H:%M:%S.%f')
    }

@DryPipe.python_call()
def do_sleep1():
    return __sleep(1)

@DryPipe.python_call()
def do_sleep2():
    return __sleep(2)

@DryPipe.python_call()
def do_sleep3():
    return __sleep(3)

def correct_timestamps_in_task_history(dsl):

    s1 = dsl.task(
        "sleep1"
    ).produces(
        start_t_1=dsl.var(int),
        end_t_1=dsl.var(int)
    ).calls(
        do_sleep1
    )()

    yield s1

    for _ in dsl.with_completed_tasks(s1):

        yield dsl.task(
            "sleep2_3"
        ).produces(
            start_t_2=dsl.var(int),
            end_t_2=dsl.var(int),
            start_t_3=dsl.var(int),
            end_t_3=dsl.var(int)
        ).calls(
            do_sleep2
        ).calls(
            do_sleep3
        )()
