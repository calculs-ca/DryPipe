from base_pipeline_test import BasePipelineTest
from dry_pipe import DryPipe


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


    def test_validate(self):

        consume_and_produce_a_var = self.tasks_by_keys["consume_and_produce_a_var"]
        produce_a_var = self.tasks_by_keys["produce_a_var"]

        self.assertEqual(int(produce_a_var.outputs.v), 1234)
        self.assertEqual(int(consume_and_produce_a_var.outputs.result), 2468)



@DryPipe.python_call()
def multiply_by_x(x, y, f):

    with open(f) as _f:
        _f.write("magic")

    return {
        "result": x * y
    }

class PipelineWithTwoPythonTasks(BasePipelineTest):

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
            echo "magic" > $f
        """)()

        yield t1

        yield dsl.task(
            key="t2"
        ).inputs(
            t1.outputs.x, #equivalent to: x=t1.out.x:
            z=t1.outputs.y,
            f_magic=t1.outputs.f
        ).outputs(
            r=int
        ).calls("""
            #!/usr/bin/env bash
            echo "x: $x"
            echo "z: $z"
            export r=$(( $x + $z ))    
        """)()


    def test_validate(self):
        t2 = self.tasks_by_keys["t2"]

        if not t2.is_completed():
            raise Exception(f"expected completed, got {t2.state_name()}")

        res = int(t2.outputs.r)
        if res != 46:
            raise Exception(f"expected 46, got {res}")
