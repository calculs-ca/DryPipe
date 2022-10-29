from dry_pipe import DryPipe


def pipeline():

    def display_grouper(task_key):
        task_type, suffix = task_key.split(".")
        return f"group-{task_type[-1]}"

    return DryPipe.create_pipeline(pipeline_task_generator, display_grouper=display_grouper)


@DryPipe.python_call()
def mult_by_2(n):
    return {
        "n2": int(n) * 2
    }


@DryPipe.python_call()
def grand_total_func(all_n_x_2):

    grand_total = sum([
         int(n) for n in all_n_x_2.split(",")
    ])

    return {
        "grand_total": grand_total
    }


def pipeline_task_generator(dsl):

    for task_key in [
        "group_a.1",
        "group_a.2",
        "group_a.3",
        "group_b.1",
        "group_b.2",
        "group_c.111"
    ]:

        n = int(task_key.split(".")[1])

        yield dsl.task(
            key=task_key
        ).consumes(
            n=dsl.val(n)
        ).produces(
            n2=dsl.var(int)
        ).calls(
            mult_by_2
        )()

    for task_matcher in dsl.with_completed_matching_tasks("group_*"):

        yield dsl.task(
            key="grand_total.end"
        ).consumes(
            all_n_x_2=dsl.val(",".join(
                map(str, task_matcher.out.n2.fetch())
            ))
        ).produces(
            grand_total=dsl.var(int)
        ).calls(
            grand_total_func
        )()
