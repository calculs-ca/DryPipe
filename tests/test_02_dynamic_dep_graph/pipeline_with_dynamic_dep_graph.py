import glob
import os

from dry_pipe import DryPipe, cli


@DryPipe.python_call()
def prepare_tasks(__work_dir):

    list_of_tasks_file = os.path.join(
        os.path.dirname(__file__),
        "list_of_tasks.tsv"
    )

    with open(list_of_tasks_file) as f:
        for line in f.readlines():
            line = line.strip()

            if line == "":
                continue

            very_special_number = int(line)

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
def aggregate_func(__work_dir, grandiose_report):

    d = os.path.dirname(os.path.dirname(__work_dir))

    print(f"s1:{__work_dir}")
    print(f"s2:{d}")

    def results():
        for var_file in glob.glob(f"{d}/.drypipe/work_chunk.*/output_vars"):
            print("s2")
            with open(var_file) as f:
                def var_dict():
                    print("s3")
                    for line in f.readlines():
                        line = line.strip()
                        if line != "":
                            k, v = line.split("=")
                            if k == "inflated_number":
                                v = int(v)
                            else:
                                v = v[1:-1]
                            yield k, v
                yield dict(var_dict())

    r = list(results())

    aggregate_inflated_number = sum([
        dic["inflated_number"] for dic in r
    ])

    aggregate_insane_string = ",".join([
        dic["insane_string"] for dic in r
    ])

    with open(grandiose_report, "w") as f:
        f.write("a truly grandiose report !\n")
        f.write(f"{aggregate_inflated_number}")

    return {
        "aggregate_inflated_number": aggregate_inflated_number,
        "aggregate_insane_string": aggregate_insane_string
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


def all_pipeline_tasks(dsl):

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

    aggregate_task = dsl.task(
        key="aggregate_all"
    ).consumes(
        all_work_chunk_tasks_outputs=dsl.matching_tasks("work_chunk.*")
    ).produces(
        grandiose_report=dsl.file("grandiose_report.txt")
    ).calls(
        aggregate_func
    )()

    yield aggregate_task


def all_pipeline_tasks_with_wait_for_completion(dsl):

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



def get_expected_agg_result(agg_task):
    def gen():
        with open(agg_task.v_abs_output_var_file()) as f:
            for line in f.readlines():
                k, v = line.split("=")
                if k == "aggregate_inflated_number":
                    v = int(v)
                    yield v

    return sum(gen())
