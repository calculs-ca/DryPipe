import os

from dry_pipe import DryPipe, TaskConf


def dag_gen(test_case, container=None):

    tc32 = TaskConf(
        executer_type="process",
        ssh_specs=f"maxl@ip32.ccs.usherbrooke.ca:~/.ssh/id_rsa",
        remote_base_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests",
        remote_pipeline_code_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests/code-dir",
        remote_containers_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests/containers",
        container=container
    )

    tc = TaskConf(
        executer_type="process",
        ssh_specs=f"maxl@ip29.ccs.usherbrooke.ca:~/.ssh/id_rsa",
        remote_base_dir="/home/maxl/drypipe-tests",
        remote_pipeline_code_dir="/home/maxl/drypipe-tests/code-dir",
        remote_containers_dir="/home/maxl/drypipe-tests/containers",
        container=container
    )

    def g(dsl):
        yield dsl.task(
            key=f"t1",
            task_conf=tc
        ).produces(
            the_value=dsl.var(str)
        ).calls(f"""
            #!/usr/bin/env bash            
            set -e    
            . $__pipeline_code_dir/tests/test-external-dependency1.sh
            export the_value=$THE_VALUE
        """)()

    def validate(pipeline_instance):
        remote_task = pipeline_instance.tasks["t1"]

        test_case.assertEqual(remote_task.out.the_value.fetch(), 'abc-cba')

    return g, validate, tc


pipeline_code_dir = os.path.dirname(os.path.dirname(__file__))

def pipeline(container):

    gen, validator, tc = dag_gen(None, container)

    return DryPipe.create_pipeline(
        gen,
        remote_task_confs=[tc],
        pipeline_code_dir=pipeline_code_dir
    )

def pipeline1():
    return pipeline(None)

def pipeline2():
    return pipeline(os.path.join(pipeline_code_dir, "tests", "containers", "singularity-test-container.sif"))
