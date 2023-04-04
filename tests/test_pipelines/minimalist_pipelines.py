from dry_pipe import DryPipe
from dry_pipe.pipeline_runner import PipelineRunner
from dry_pipe.state_machine import StateFileTracker, StateMachine


class PipelineTester:

    def __init__(self, test_case, sandbox, task_conf=None):
        self.test_case = test_case
        self.sandbox = sandbox
        self.task_conf = task_conf
        self.tasks_by_keys = {}

        t = StateFileTracker(self.sandbox.sandbox_dir)
        state_machine = StateMachine(t, lambda dsl: self.dag_gen(dsl))
        pr = PipelineRunner(state_machine)
        pr.run_sync()

        self.tasks_by_keys = {
            t.key: t
            for t in t.load_completed_tasks_for_query()
        }
        self.validate()

    def dag_gen(self, dsl):
        pass

    def validate(self):
        pass



class PipelineWithSingleBashTask(PipelineTester):

    def dag_gen(self, dsl):
        yield dsl.task(
            key="multiply_x_by_y",
            task_conf=self.task_conf
        ).inputs(
            x=3,
            y=5
        ).outputs(
            result=int
        ).calls(
            """
            #!/usr/bin/bash        
            echo "Z1"        
            export result=$((x * y))
            echo "--->$result"
            echo "Z2"
            """
        )()

    def validate(self):
        multiply_x_by_y_task = self.tasks_by_keys["multiply_x_by_y"]
        self.test_case.assertEqual(3, multiply_x_by_y_task.inputs.x)
        self.test_case.assertEqual(5, multiply_x_by_y_task.inputs.y)
        self.test_case.assertEqual(15, int(multiply_x_by_y_task.outputs.result))


@DryPipe.python_call()
def multiply_by_x(x, y):
    return {
        "result": x * y
    }


class PipelineWithSinglePythonTask(PipelineTester):

    def dag_gen(self, dsl):
        yield dsl.task(
            key="multiply_x_by_y",
            task_conf=self.task_conf
        ).inputs(
            x=3, y=4
        ).outputs(
            result=int
        ).calls(
            multiply_by_x
        )()


    def validate(self):
        multiply_x_by_y_task = self.tasks_by_keys["multiply_x_by_y"]

        if not multiply_x_by_y_task.is_completed():
            raise Exception(f"expected completed, got {multiply_x_by_y_task.state_name()}")

        x = multiply_x_by_y_task.inputs.x
        if x != 3:
            raise Exception(f"expected 3, got {x}")

        res = int(multiply_x_by_y_task.outputs.result)
        if res != 12:
            raise Exception(f"expected 12, got {res}")
