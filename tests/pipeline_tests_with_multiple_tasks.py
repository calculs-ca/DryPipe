from base_pipeline_test import BasePipelineTest



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