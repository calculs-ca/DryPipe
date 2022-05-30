import os

from dry_pipe import DryPipe, cli, DynamicTaskConf

dsl = DryPipe.dsl(pipeline_code_dir=os.path.dirname(__file__))


def gen_task(dsl):

    executer_conf = DynamicTaskConf(
        executer_type="slurm",
        ssh_specs=
        "maxl@ip29.ccs.usherbrooke.ca:/home/maxl/drypipe_tests:~/.ssh/id_rsa",
        slurm_account="def-rodrigu1",
        sbatch_options=[
            "--time=0:20:00"
        ]
    )

    yield dsl.task(
        key="slurm-task-on-ip29",
        executer=executer_conf
    ).consumes(
        loop_count=dsl.val(10000)
    ).produces(
        result=dsl.var(int)
    ).calls("loop-n-times.sh")()

    def z():
        yield dsl.task(
            key="local-task-on-ip29",
            executer=executer_conf
        ).consumes(
            loop_count=dsl.val(10000)
        ).produces(
            result=dsl.var(int)
        ).calls("loop-n-times.sh")()

        yield dsl.task(
            key="local-task"
        ).consumes(
            loop_count=dsl.val(10000)
        ).produces(
            result=dsl.var(int)
        ).calls("loop-n-times.sh")()
