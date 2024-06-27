
import dry_pipe
from base_pipeline_test import BasePipelineTest
from dry_pipe import TaskConf

class PipelineWithMultiCallSlurmArrayForRealSlurmTest(BasePipelineTest):

    def launches_tasks_in_process(self):
        return True

    def is_fail_test(self):
        return True

    def dag_gen(self, dsl):

        for i in range(1, 4):
            yield dsl.task(
                key=f"t{i}",
                is_slurm_array_child=True,
                task_conf=TaskConf(
                    executer_type="slurm",
                    slurm_account="dummy"
                )
            ).inputs(
                x=i
            ).outputs(
                r=int
            ).calls(
                array_test_crash
            ).calls("""
            #!/usr/bin/env bash
            echo "..."
            """)()

        tc = self.task_conf()

        for match in dsl.query_all_or_nothing("t*", state="ready"):
            yield dsl.task(
                key="array-parent",
                task_conf=tc
            ).slurm_array_parent(
                children_tasks=match.tasks
            )()

    def validate(self, tasks_by_keys):

        t = tasks_by_keys.get("t1")

        self.assertIsNotNone(t)
        if not t.is_failed():
            raise Exception(f"expected failure, got {t.state_name()}")

        array_task = tasks_by_keys.get("array-parent")

        self.assertIsNotNone(array_task)
        if not array_task.is_failed():
            raise Exception(f"expected failure of array-parent, got {array_task.state_name()}")


@dry_pipe.DryPipe.python_call()
def array_test_crash(r):
    raise Exception(f"crash test")


@dry_pipe.DryPipe.python_call()
def array_test_1(r):
    pass




all_tests = [
    PipelineWithMultiCallSlurmArrayForRealSlurmTest
]