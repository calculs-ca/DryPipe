from base_pipeline_test import BasePipelineTest
from dry_pipe import DryPipe, TaskConf


class PipelineWithVariablePassing(BasePipelineTest):

    def dag_gen(self, dsl):
        t1 = dsl.task(
            key="produce_a_var"
        ).outputs(
            v=int
        ).calls(
            """
            #!/usr/bin/bash                
            export v=1234        
            """
        )()

        yield t1

        yield dsl.task(
            key="consume_and_produce_a_var"
        ).inputs(
            v=t1.outputs.v
        ).outputs(
            result=int
        ).calls(
            """
            #!/usr/bin/bash
            export static_result=abc
            export result=$((v * 2))
            """
        )()


    def validate(self, tasks_by_keys):

        consume_and_produce_a_var = tasks_by_keys["consume_and_produce_a_var"]
        produce_a_var = tasks_by_keys["produce_a_var"]

        self.assertEqual(int(produce_a_var.outputs.v), 1234)
        self.assertEqual(int(consume_and_produce_a_var.outputs.result), 2468)



@DryPipe.python_call()
def multiply_by_x(x, y, f):

    with open(f) as _f:
        _f.write("magic")

    return {
        "result": x * y
    }

class PipelineWithTwoBashTasks(BasePipelineTest):

    def dag_gen(self, dsl):

        t1 = dsl.task(
            key="t1"
        ).outputs(
            x=int,
            y=int,
            f=dsl.file("magic-file.txt")
        ).calls("""
            #!/usr/bin/env bash
            export x=12
            export y=34
            echo "f: $f"
            echo "magic" > $f
        """)()

        yield t1

        yield dsl.task(
            key="t2",
            task_conf=self.task_conf()
        ).inputs(
            t1.outputs.x, #equivalent to: x=t1.out.x:
            z=t1.outputs.y,
            f_magic=t1.outputs.f
        ).outputs(
            r=int,
            f_magic2=dsl.file("magic2")
        ).calls("""
            #!/usr/bin/env bash
            set -exv 
            
            echo "x: $x"
            echo "z: $z"
            export r=$(( $x + $z ))
            cp $f_magic $f_magic2            
        """)()

    def validate(self, tasks_by_keys):
        t2 = tasks_by_keys["t2"]

        if not t2.is_completed():
            raise Exception(f"expected completed, got {t2.state_name()}")

        res = int(t2.outputs.r)
        if res != 46:
            raise Exception(f"expected 46, got {res}")

        self.assert_file_content_equals(str(t2.outputs.f_magic2), "magic")


class PipelineWithTwoBashTasksWorkOnLocalCopy(PipelineWithTwoBashTasks):

    def task_conf(self):
        tc = TaskConf.default()
        tc.work_on_local_file_copies = True
        return tc


def all_basic_tests():
    return [
        PipelineWithVariablePassing,
        PipelineWithTwoBashTasks,
        PipelineWithTwoBashTasksWorkOnLocalCopy
    ]