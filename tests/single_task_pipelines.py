from pathlib import Path

from base_pipeline_test import BasePipelineTest
from dry_pipe import DryPipe, TaskConf


@DryPipe.python_call()
def multiply_by_x(x, y):
    return {
        "result": x * y
    }

class PipelineWithSingleBashTask(BasePipelineTest):

    def dag_gen(self, dsl):
        yield dsl.task(
            key="multiply_x_by_y",
            task_conf=self.task_conf()
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


    def test_validate(self):
        multiply_x_by_y_task = self.tasks_by_keys["multiply_x_by_y"]
        self.assertEqual(3, multiply_x_by_y_task.inputs.x)
        self.assertEqual(5, multiply_x_by_y_task.inputs.y)
        self.assertEqual(15, int(multiply_x_by_y_task.outputs.result))


class PipelineWithSinglePythonTask(BasePipelineTest):

    def dag_gen(self, dsl):
        yield dsl.task(
            key="multiply_x_by_y",
            task_conf=self.task_conf()
        ).inputs(
            x=3, y=4
        ).outputs(
            result=int
        ).calls(
            multiply_by_x
        )()


    def test_validate(self):
        multiply_x_by_y_task = self.tasks_by_keys["multiply_x_by_y"]

        if not multiply_x_by_y_task.is_completed():
            raise Exception(f"expected completed, got {multiply_x_by_y_task.state_name()}")

        x = multiply_x_by_y_task.inputs.x
        if x != 3:
            raise Exception(f"expected 3, got {x}")

        res = int(multiply_x_by_y_task.outputs.result)
        if res != 12:
            raise Exception(f"expected 12, got {res}")


#TODO :  error when CONSUME VARS are arguments:ex:
# def func(i, a, x, f):
@DryPipe.python_call()
def func(i, f):

    with open(f, "w") as _f:
        _f.write("THE_FILE_CONTENT_123")

    return {
        "x": 123,
        "a": "abc"
    }


class PipelineWithVarAndFileOutput(BasePipelineTest):

    def dag_gen(self, dsl):

        yield dsl.task(
            key="t1",
            task_conf=self.task_conf()
        ).inputs(
            i=123
        ).outputs(
            x=int,
            a=str,
            f=Path("f.txt")
        ).calls(func)()


    def test_validate(self):
        task = self.tasks_by_keys["t1"]

        if not task.is_completed():
            raise Exception(f"expected completed, got {task.state_name()}")

        with open(task.outputs.f) as f:
            s = f.read()
            self.assertEqual(s, "THE_FILE_CONTENT_123")


# Same pipelines with container

task_conf_with_test_container = TaskConf(
    executer_type="process",
    container="singularity-test-container.sif"
)

class PipelineWithSingleBashTaskInContainer(PipelineWithSingleBashTask):
    def task_conf(self):
        return task_conf_with_test_container

class PipelineWithSinglePythonTaskInContainer(PipelineWithSinglePythonTask):
    def task_conf(self):
        return task_conf_with_test_container

class PipelineWithVarAndFileOutputInContainer(PipelineWithVarAndFileOutput):
    def task_conf(self):
        return task_conf_with_test_container