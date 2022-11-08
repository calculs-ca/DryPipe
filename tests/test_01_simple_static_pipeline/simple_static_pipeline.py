import os

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
        v1_from_blast=blast.out.v1,
        v2_from_blast=blast.out.v2,
    ).produces(
        fancy_report=dsl.file("fancy_report.txt"),
        var_dump=dsl.file("var_dump.txt"),
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


def run_and_validate_pipeline_execution(pipeline_instance, test_case):

    blast, report_task, python_much_fancier_report, python_much_much_fancier_report = pipeline_instance.tasks

    d = list(blast.iterate_unsatisfied_deps())
    test_case.assertEqual(len(d), 0)

    missing_deps = list(report_task.iterate_unsatisfied_deps())

    test_case.assertEqual(len(missing_deps), 1)

    (msg, code, task, missing_file_deps, missing_var_deps) = missing_deps[0]

    test_case.assertEqual(task, blast)
    test_case.assertEqual(len(missing_file_deps), 1)
    test_case.assertEqual(len(missing_var_deps), 2)

    pipeline_instance.run_sync()

    validate_pipeline_execution(pipeline_instance, test_case)


def validate_pipeline_execution(pipeline_instance, test_case):

    blast, report_task, python_much_fancier_report, python_much_much_fancier_report = pipeline_instance.tasks

    if not pipeline_instance.get_state().is_completed():
        raise Exception(f"expected pipeline state to be completed")

    for t in [blast, report_task, python_much_fancier_report, python_much_much_fancier_report]:
        state = t.get_state()
        if not state.is_completed():
            raise Exception(f"expected {t} to be completed, but is {state}")

    var_dump = os.path.join(
        pipeline_instance.pipeline_instance_dir, report_task.out.var_dump.absolute_path(report_task)
    )
    with open(var_dump) as f:
        for line in f.read().split("\n"):
            if line.startswith("v1_for_validation:"):
                v1 = line.split(":")[1]
            if line.startswith("v2_for_validation:"):
                v2 = line.split(":")[1]

        if v1 == "":
            raise Exception(f"variable v1_for_validation was not set by task {report_task} in {var_dump}")
        if v2 == "":
            raise Exception(f"variable v2_for_validation was not set by task {report_task} in {var_dump}")

        test_case.assertEqual(int(v1), 1111)
        test_case.assertEqual(float(v2), 3.14)

    test_case.assertEqual(
        "9a380a5deaa3a091368d3a07b722d5362328a07b",
        blast.output_signature()
    )

    if False:
        test_case.assertEqual(
            "e90c0ae0e6cb2d66a00fce87646625d32f9d614d",
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
