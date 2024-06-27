import os



def dag_gen(remote_task_conf, test_case):

    d = os.path.abspath(os.path.dirname(__file__))

    external_dep1 = os.path.join(d,"test-external-dependency1.sh")
    external_dep2 = os.path.join(d,"test-external-dependency2.sh")

    def g(dsl):

        yield dsl.task(
            key=f"t1",
            task_conf=remote_task_conf
        ).consumes(
            external_file1=dsl.file(external_dep1),
            external_file2=dsl.file(external_dep2, remote_cache_bucket=None)
        ).produces(
            the_value=dsl.var(str),
            the_other_value=dsl.var(str)
        ).calls(f"""
            #!/usr/bin/env bash            
            set -e
            
            echo $external_file1
            echo $external_file2
             
            . $external_file1
            . $external_file2
            
            export the_value=$THE_VALUE
            export the_other_value=$THE_OTHER_VALUE            
        """)()

    def validate(pipeline_instance):
        remote_task = pipeline_instance.tasks["t1"]

        test_case.assertEqual(remote_task.out.the_value.fetch(), 'abc-cba')
        test_case.assertEqual(remote_task.out.the_other_value.fetch(), 'xyz-zyx')

    return g, validate
