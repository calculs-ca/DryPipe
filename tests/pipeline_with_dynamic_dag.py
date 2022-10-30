import os

from dry_pipe import DryPipe


@DryPipe.python_call()
def prepare_tasks(__work_dir):

    for very_special_number in [1, 2, 3, 4]:
        work_chunk_file = os.path.join(__work_dir, f"work_chunk.{very_special_number}.txt")

        print(f"--->{work_chunk_file}")

        with open(work_chunk_file, "w") as w:
            w.write(str(very_special_number))


@DryPipe.python_call()
def work_chunk_func(work_file):

    with open(work_file) as f:
        i = int(f.read())

        return {
            "inflated_number": i * 2,
            "insane_string": f"abc{i}"
        }


@DryPipe.python_call()
def aggregate_func_2(inflated_numbers, insane_strings):

    aggregate_inflated_number = sum([
         int(n) for n in inflated_numbers.split(",")
    ])

    return {
        "aggregate_inflated_number": aggregate_inflated_number,
        "insane_strings_passed_through": insane_strings
    }


def pipeline():
    return DryPipe.create_pipeline(pipeline_task_generator)


def pipeline_task_generator(dsl):

    preparation_task = dsl.task(
        key="preparation_task"
    ).produces(
        work_files=dsl.fileset("work_chunk.*.txt")
    ).calls(prepare_tasks)()

    yield preparation_task

    for pt in dsl.with_completed_tasks(preparation_task):
        for work_file_handle in pt.out.work_files.fetch():
            chunk_number = work_file_handle.file_path.split(".")[-2]

            yield dsl.task(
                key=f"work_chunk.{chunk_number}"
            ).consumes(
                work_file=work_file_handle
            ).produces(
                inflated_number=dsl.var(int),
                insane_string=dsl.var(str)
            ).calls(
                work_chunk_func
            )()

        for work_chunk_task_matcher in dsl.with_completed_matching_tasks("work_chunk.*"):

            yield dsl.task(
                key="aggregate_all"
            ).consumes(
                inflated_numbers=dsl.val(",".join(
                    map(str, work_chunk_task_matcher.out.inflated_number.fetch())
                )),
                insane_strings=dsl.val(",".join(work_chunk_task_matcher.out.insane_string.fetch()))
            ).produces(
                aggregate_inflated_number=dsl.var(int),
                insane_strings_passed_through=dsl.var(str)
            ).calls(
                aggregate_func_2
            )()
