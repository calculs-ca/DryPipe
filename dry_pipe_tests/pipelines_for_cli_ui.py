import time

from dry_pipe import DryPipe


def dag_echo_debug(dsl):

    dsl.logger.debug("debug 1")

    dsl.logger.info("info x")

    yield dsl.task(
        key="t1"
    ).inputs(
        x=123
    ).outputs(
        y=int
    ).calls(
        """
        #!/usr/bin/bash
        
        export y=456
        sleep 10000000
        """
    )()

    time.sleep(2)



def pipline_dag_echo_debug():
    return DryPipe.create_pipeline(dag_echo_debug)