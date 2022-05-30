from dry_pipe import DryPipe, TaskConf

_INFINITE_LOOP_SCRIPT = """
    #!/usr/bin/env bash

    for i in $(seq 1000000); do
        echo "--:>$i"
        sleep 2
    done            
"""


def never_ending_pipeline(task_conf):

    def tasks(dsl):

        yield dsl.task(
            key="loop-n-times",
            task_conf=task_conf
        ).consumes(
            count=dsl.val(10000)
        ).produces(
            result=dsl.var(int)
        ).calls(_INFINITE_LOOP_SCRIPT)()

    return tasks


def pipeline():
    return DryPipe.create_pipeline(never_ending_pipeline(TaskConf.default()))
