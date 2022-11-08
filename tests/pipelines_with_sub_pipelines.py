
from dry_pipe import DryPipe


@DryPipe.python_call()
def parse_and_compute_sum(series):
    return {
        "result": sum([int(n) for n in series.split(",")])
    }


@DryPipe.python_call()
def multiply_by_x(x, n):
    return {
        "result": n * x
    }


@DryPipe.python_call()
def add_n(r, n):
    return {
        "final_result": r + n
    }


def sum_of_series_multiplied_by_x(list_of_ints, x):

    def tasks(dsl):

        parse_and_compute_sum_task = dsl.task(
            key="parse_and_compute_sum"
        ).consumes(
            series=dsl.val(",".join(map(str, list_of_ints)))
        ).produces(
            result=dsl.var(int)
        ).calls(
            parse_and_compute_sum
        )()

        yield parse_and_compute_sum_task

        multiply_by_x_task = dsl.task(
            key="multiply_by_x"
        ).consumes(
            x=dsl.val(x),
            n=parse_and_compute_sum_task.out.result
        ).produces(
            result=dsl.var(int)
        ).calls(
            multiply_by_x
        )()

        yield multiply_by_x_task

    return tasks


def sum_and_multiply_by_x_and_add_y(list_of_int, x, y):

    sum_of_series_multiplied_by_x_pipeline = \
        DryPipe.create_pipeline(sum_of_series_multiplied_by_x(list_of_int, x))

    def tasks(dsl):

        sub_pipeline = dsl.sub_pipeline(sum_of_series_multiplied_by_x_pipeline, "s_")

        yield sub_pipeline

        for multiply_by_x_task in sub_pipeline.with_completed_tasks("multiply_by_x"):
            t = dsl.task(
                key="add_y"
            ).consumes(
                r=multiply_by_x_task.out.result,
                n=dsl.val(y)
            ).produces(
                final_result=dsl.var(int)
            ).calls(
                add_n
            )()

            yield t

    return tasks


def sum_and_multiply_by_x_and_add_y_and_z(list_of_int, x, y, z):

    sum_and_multiply_by_x_and_add_y_pipeline = \
        DryPipe.create_pipeline(sum_and_multiply_by_x_and_add_y(list_of_int, x, y))

    def tasks(dsl):

        sub_pipeline = dsl.sub_pipeline(sum_and_multiply_by_x_and_add_y_pipeline, "q_")

        yield sub_pipeline

        for add_y_task in sub_pipeline.with_completed_tasks("add_y"):
            yield dsl.task(
                key="add_z"
            ).consumes(
                r=add_y_task.out.final_result,
                n=dsl.val(z)
            ).produces(
                final_result=dsl.var(int)
            ).calls(
                add_n
            )()

    return tasks
