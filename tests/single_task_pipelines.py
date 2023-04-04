import os.path
from pathlib import Path

from base_pipeline_test import BasePipelineTest
from dry_pipe import DryPipe, TaskConf
from dry_pipe.pipeline import Pipeline


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

@DryPipe.python_call()
def f3(x1, x2):

    print("f3")

    return {
        "x3": x1 + x2
    }


class PipelineWithVarSharingBetweenSteps(BasePipelineTest):

    def dag_gen(self, dsl):
        yield dsl.task(
            key="t"
        ).outputs(
            x1=int,
            x2=int,
            x3=int
        ).calls("""
            #!/usr/bin/env bash        
            export x1=7
        """).calls("""
            #!/usr/bin/env bash                
            export x2=$(( $x1 * 2 ))    
        """).calls(f3)()


    def test_validate(self):

        t = self.tasks_by_keys["t"]

        self.assertTrue(t.is_completed())

        x1 = int(t.outputs.x1)
        x2 = int(t.outputs.x2)
        x3 = int(t.outputs.x3)

        self.assertEqual(x1, 7)
        self.assertEqual(x2, 14)
        self.assertEqual(x3, 21)




class PipelineWith3StepsNoCrash(BasePipelineTest):

    def dag_gen(self, dsl):
        three_phase_task = dsl.task(
            key="three_phase_task",
            task_conf=self.task_conf()
        ).outputs(
            out_file=dsl.file("out_file.txt")
        ).calls("""
            #!/usr/bin/env bash
            
            echo "---> $CRASH_STEP_1"

            if [[ "${CRASH_STEP_1}" ]]; then
              echo "boom in step 1" >&2
              exit 1
            fi

            echo "s1" >> $out_file    
        """).calls("""
            #!/usr/bin/env bash

            if [[ "${CRASH_STEP_2}" ]]; then
              echo "boom in step 2" >&2
              exit 1
            fi


            echo "s2" >> $out_file    
        """).calls("""
            #!/usr/bin/env bash

            if [[ "${CRASH_STEP_3}" ]]; then
              echo "boom in step 3" >&2
              exit 1
            fi

            echo "s3" >> $out_file    
        """)()

        yield three_phase_task

    def output_as_string(self):
        three_phase_task = self.tasks_by_keys["three_phase_task"]
        with open(three_phase_task.outputs.out_file) as f:
            return f.read()

    def test_validate(self):
        self.assertEqual(self.output_as_string(), "s1\ns2\ns3\n")

class PipelineWith3StepsCrash1(PipelineWith3StepsNoCrash):

    def task_conf(self):
        return TaskConf(
            executer_type="process",
            extra_env={
                "CRASH_STEP_1": "TRUE"
            }
        )

    def run_pipeline(self):
        p = Pipeline(lambda dsl: self.dag_gen(dsl), pipeline_code_dir=self.pipeline_code_dir)
        pi = p.create_pipeline_instance(self.pipeline_instance_dir)
        pi.run_sync(fail_silently=True)
        self.tasks_by_keys = {
            t.key: t
            for t in pi.query("*", include_incomplete_tasks=True)
        }

    def test_validate(self):
        three_phase_task = self.tasks_by_keys["three_phase_task"]
        self.assertTrue(three_phase_task.is_failed())
        self.assertFalse(os.path.exists(three_phase_task.outputs.out_file))

class PipelineWith3StepsCrash2(PipelineWith3StepsCrash1):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            extra_env={
                "CRASH_STEP_2": "TRUE"
            }
        )

    def test_validate(self):
        three_phase_task = self.tasks_by_keys["three_phase_task"]
        self.assertTrue(three_phase_task.is_failed())
        self.assertTrue(os.path.exists(three_phase_task.outputs.out_file))
        self.assertEqual(self.output_as_string(), "s1\n")


class PipelineWith3StepsCrash3(PipelineWith3StepsCrash1):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            extra_env={
                "CRASH_STEP_3": "TRUE"
            }
        )

    def test_validate(self):
        three_phase_task = self.tasks_by_keys["three_phase_task"]
        self.assertTrue(three_phase_task.is_failed())
        self.assertTrue(os.path.exists(three_phase_task.outputs.out_file))
        self.assertEqual(self.output_as_string(), "s1\ns2\n")


@DryPipe.python_call()
def step2_in_python(out_file):
    with open(out_file, "a") as f:
        f.write("s2\n")


@DryPipe.python_call()
def step4_in_python(out_file):
    with open(out_file, "a") as f:
        f.write("s4\n")


class PipelineWith4MixedStepsNoCrash(BasePipelineTest):

    def dag_gen(self, dsl):
        three_phase_task = dsl.task(
            key="three_phase_task",
            task_conf=self.task_conf()
        ).outputs(
            out_file=dsl.file("out_file.txt")
        ).calls(
            """
                #!/usr/bin/env bash
                echo "zaz -> $CRASH_STEP_3"
    
                if [[ "${CRASH_STEP_1}" ]]; then
                  exit 1
                fi
    
                echo "s1" >> $out_file        
            """
        ).calls(
            step2_in_python
        ).calls("""
            #!/usr/bin/env bash                        
    
            if [[ "${CRASH_STEP_3}" ]]; then
              exit 1
            fi
    
            echo "s3" >> $out_file    
        """).calls(
            step4_in_python
        )()

        yield three_phase_task


    def output_as_string(self):
        three_phase_task = self.tasks_by_keys["three_phase_task"]
        with open(three_phase_task.outputs.out_file) as f:
            return f.read()

    def test_validate(self):
        self.assertEqual(self.output_as_string(), "s1\ns2\ns3\ns4\n")


class PipelineWith4MixedStepsCrash(PipelineWith4MixedStepsNoCrash):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            extra_env={
                "CRASH_STEP_3": "TRUE"
            }
        )

    def run_pipeline(self):
        p = Pipeline(lambda dsl: self.dag_gen(dsl), pipeline_code_dir=self.pipeline_code_dir)
        pi = p.create_pipeline_instance(self.pipeline_instance_dir)
        pi.run_sync(fail_silently=True)
        self.tasks_by_keys = {
            t.key: t
            for t in pi.query("*", include_incomplete_tasks=True)
        }

    def test_validate(self):
        three_phase_task = self.tasks_by_keys["three_phase_task"]
        self.assertTrue(three_phase_task.is_failed())
        self.assertTrue(os.path.exists(three_phase_task.outputs.out_file))
        self.assertEqual(self.output_as_string(), "s1\ns2\n")


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

class PipelineWithVarSharingBetweenStepsInContainer(PipelineWithVarSharingBetweenSteps):
    def task_conf(self):
        return task_conf_with_test_container

class PipelineWith3StepsNoCrashInContainer(PipelineWith3StepsNoCrash):
    def task_conf(self):
        return task_conf_with_test_container

class PipelineWith3StepsCrash3InContainer(PipelineWith3StepsCrash3):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            container="singularity-test-container.sif",
            extra_env={
                "CRASH_STEP_3": "TRUE"
            }
        )