from dry_pipe import DryPipe


@DryPipe.python_call()
def square(x):
    return {
        "s": x ^ 2,
        "i": x
    }


@DryPipe.python_call()
def multiply(x, y):
    return {
        "r": x * y
    }

@DryPipe.python_call()
def sum_all(p):
    raise Exception("not implemented")


def create_dag_generator(list_of_numbers):

    def dag_gen(dsl):

        for i in list_of_numbers:
            if i % 2 == 0:
                yield dsl.task(
                    key=f"py-square.{i}"
                ).consumes(
                    x=dsl.val(i)
                ).produces(
                    s=dsl.var(int),
                    i=dsl.var(int)
                ).calls(
                    square
                )()
            else:
                yield dsl.task(
                    key=f"bash-square.{i}"
                ).consumes(
                    x=dsl.val(i)
                ).produces(
                    s=dsl.var(int),
                    i=dsl.var(int)
                ).calls("""
                    #!/usr/bin/env bash                                        
                    export s=$(( $x * $x))
                    export i=$x
                """)()

        for matcher in dsl.wait_for_matching_tasks("*-square.*"):
            for t in matcher.tasks:
                yield dsl.task(
                    key=f"py-multiply-by-i.{t.out.i.fetch()}"
                ).consumes(
                    x=t.out.i,
                    y=t.out.s
                ).produces(
                    s=dsl.var(int)
                ).calls(
                    multiply
                )()

            for m2 in dsl.wait_for_matching_tasks("py-multiply*"):
                yield dsl.task(
                    key="finale"
                ).consumes(
                    p=m2.all.task_out()
                ).calls(
                    sum_all
                )

    return dag_gen
