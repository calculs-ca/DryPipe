
def create_pipeline_generator_two_remote_sites(remote_task_conf_1, remote_task_conf_2):

    def pipeline_generator(dsl):

        t1 = dsl.task(
            key=f"t1",
            task_conf=remote_task_conf_1
        ).produces(
            out_file=dsl.file("f1.txt")
        ).calls(f"""
            #!/usr/bin/env bash
            echo "1" > $out_file                           
        """)()

        t2 = dsl.task(
            key=f"t2",
            task_conf=remote_task_conf_2
        ).produces(
            out_file=dsl.file("f1.txt")
        ).calls(f"""
            #!/usr/bin/env bash
            echo "2" > $out_file                           
        """)()

        yield t1
        yield t2

        for _ in dsl.wait_for_tasks(t1, t2):
            yield dsl.task(
                key=f"t3"
            ).consumes(
                f1=t1.out.out_file,
                f2=t2.out.out_file
            ).produces(
                out_file=dsl.file("f3.txt")
            ).calls(f"""
                #!/usr/bin/env bash
                cat $f1 >  $out_file
                cat $f2 >> $out_file                           
            """)()

    return pipeline_generator