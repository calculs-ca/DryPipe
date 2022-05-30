import os

from dry_pipe import DryPipe, cli

dsl = DryPipe.dsl(pipeline_code_dir=os.path.dirname(__file__))



def gen_tasks(dsl):

    task1 = dsl.task(
        key="task1"
    ).consumes(
        file_with_number_inside=dsl.file("f.txt")
    ).produces(
        task1_out=dsl.file("task1_out.txt")
    ).calls(
        "task1.sh"
    )()

    yield task1

    task2 = dsl.task(
        key="task2"
    ).consumes(
        task1_out=task1.out.task1_out
    ).produces(
        task2_out=dsl.file("task2_out.txt")
    ).calls(
        "task2.sh"
    )()

    yield task2
