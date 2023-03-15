


def pipeline_with_external_deps_dag_gen(
        dsl,
        pipeline_with_variable_passing_pid,
        pipeline_with_multistep_tasks_with_shared_vars_pid
):

    external_tasks1 = dsl.query("t2", external_pipeline_instance_dir=pipeline_with_variable_passing_pid).tasks

    external_tasks2 = dsl.query(
        "t1", external_pipeline_instance_dir=pipeline_with_multistep_tasks_with_shared_vars_pid
    ).tasks

    if len(external_tasks1) != 1:
        raise Exception(f"expected 1 task, got {len(external_tasks1)}")

    if len(external_tasks2) != 1:
        raise Exception(f"expected 1 task, got {len(external_tasks2)}")
    a = external_tasks2[0].out.a
    f = external_tasks2[0].out.f

    for t in external_tasks1:
        yield dsl.task(
            key="t2"
        ).consumes(
            r_upstream=t.out.r,
            a_upstream=a,
            f_upstream=f
        ).produces(
            r=dsl.var(int),
            f=dsl.file("f.txt")
        ).calls("""
        #!/usr/bin/env bash        
        export r=$r_upstream
        export a=$a_upstream
        cat $f_upstream > $f
        """
        )()