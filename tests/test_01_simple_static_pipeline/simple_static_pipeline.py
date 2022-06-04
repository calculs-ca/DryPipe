
from dry_pipe import DryPipe


def simple_static_pipeline(dsl):

    blast = dsl.task(
        key="blast.1"
    ).consumes(
        subject=dsl.file("human.fasta"),
        query=dsl.file("chimp.fasta"),
        fake_blast_output=dsl.file("human_chimp_blast-fake.tsv")
    ).produces(
        blast_out=dsl.file("human_chimp_blast.tsv"),
        v1=dsl.var(int),
        v2=dsl.var(float)
    ).calls("call_blast.sh")()

    yield blast

    report_task = dsl.task(
        key="report",
    ).consumes(
        blast_result=blast.out.blast_out,
        v1_from_blast=blast.out.v1
    ).produces(
        fancy_report=dsl.file("fancy_report.txt"),
        x=dsl.var(int),
        s1=dsl.var(str),
        s2=dsl.var(type=str, may_be_none=True)
    ).calls("report.sh")()

    yield report_task

    # should not blow up:
    if report_task.out.s2.producing_task is None:
        raise Exception("report_task.out.fancy_vars.s2 should not be None !")

    yield dsl.task(
        key="python_much_fancier_report.1"
    ).consumes(
        fancy_report=report_task.out.fancy_report,
        fancy_int=dsl.val(1),
        vx=report_task.out.x,
        vs1=report_task.out.s1,
        vs2=report_task.out.s2,
        v1=blast.out.v1,
        v2=blast.out.v2
    ).produces(
        much_fancier_report=dsl.file("fancier_report1.txt")
    ).calls(
        much_fancier_report_func
    )()

    yield dsl.task(
        key="python_much_much_fancier_report.2"
    ).consumes(
        fancy_report=report_task.out.fancy_report,
        fancy_int=dsl.val(1000),
        vx=report_task.out.x,
        vs1=report_task.out.s1,
        vs2=report_task.out.s2
    ).produces(
        much_fancier_report=dsl.file("fancier_report2.txt")
    ).calls(
        much_fancier_super_report_func
    )()


def run_and_validate_pipeline_execution(pipeline, test_case):

    blast, report_task, python_much_fancier_report, python_much_much_fancier_report = pipeline.tasks

    d = list(blast.iterate_unsatisfied_deps())
    test_case.assertEqual(len(d), 0)

    missing_deps = list(report_task.iterate_unsatisfied_deps())

    test_case.assertEqual(len(missing_deps), 1)

    (msg, code, task, missing_file_deps, missing_var_deps) = missing_deps[0]

    test_case.assertEqual(task, blast)
    test_case.assertEqual(len(missing_file_deps), 1)
    test_case.assertEqual(len(missing_var_deps), 1)

    pipeline.run_sync()

    validate_pipeline_execution(pipeline, test_case)


def validate_pipeline_execution(pipeline, test_case):

    blast, report_task, python_much_fancier_report, python_much_much_fancier_report = pipeline.tasks

    for t in [blast, report_task, python_much_fancier_report, python_much_much_fancier_report]:
        state = t.get_state()
        if not state.is_completed():
            raise Exception(f"expected {t} to be completed, but is {state}")

    test_case.assertEqual(
        "9a380a5deaa3a091368d3a07b722d5362328a07b",
        blast.output_signature()
    )

    test_case.assertEqual(
        "a336aef80f2358c60f16e1e5a10839b59a5e2e42",
        report_task.output_signature()
    )

    test_case.assertEqual(
        "e31ead308b82c9ccbf67791517f1bb864bf29840",
        python_much_fancier_report.output_signature()
    )

    test_case.assertEqual(
        "44a6b33b025d65f155ca1c3431b4478924de77eb",
        python_much_much_fancier_report.output_signature()
    )


@DryPipe.python_call()
def much_fancier_report_func(much_fancier_report, fancy_int):

    print("got args:")
    print(f"much_fancier_report={much_fancier_report}")
    print(f"fancy_int={fancy_int}")

    with open(much_fancier_report, "w") as out:
        out.write("yooozzzz\n")
        out.write(f"--->{fancy_int}\n")

    return {
        "super_duper_int": fancy_int,
        "and_another_var": "wow..."
    }


@DryPipe.python_call()
def much_fancier_super_report_func(much_fancier_report):

    with open(much_fancier_report, "w") as f:
        f.write("ultra fancy report...")


    return {
        "a": 321
    }


def pipeline():
    return DryPipe.create_pipeline(simple_static_pipeline)
