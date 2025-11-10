import os.path
from pathlib import Path

from dry_pipe_tests.base_pipeline_test import BasePipelineTest
from dry_pipe_tests.cli_tests import test_cli
from dry_pipe.cli import Cli, cli_in_sub_process
from dry_pipe import DryPipe, TaskConf
from dry_pipe_tests import exportable_funcs


@DryPipe.python_call()
def multiply_by_x(x, y, PYTHONPATH):

    if not PYTHONPATH.endswith("/x/y"):
        raise Exception(f"PYTHONPATH not right")

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

    def validate(self, tasks_by_keys):
        multiply_x_by_y_task = tasks_by_keys["multiply_x_by_y"]
        self.assertEqual(3, int(multiply_x_by_y_task.inputs.x))
        self.assertEqual(5, int(multiply_x_by_y_task.inputs.y))
        self.assertEqual(15, int(multiply_x_by_y_task.outputs.result))



class PipelineWithSingleBashTaskExternalReset(PipelineWithSingleBashTask):

    def test_run_pipeline(self):
        self.run_pipeline()
        tasks_by_keys = self.pipeline_instance.query_all_tasks_by_key()
        self.validate(tasks_by_keys)

        multiply_x_by_y_task = tasks_by_keys["multiply_x_by_y"]
        self.assertTrue(multiply_x_by_y_task.is_completed())

        test_cli(
            self,
            '--pipeline-instance-dir', self.pipeline_instance_dir,
            'reset',
            '--task-key', 'multiply_x_by_y'
        )

        self.pipeline_instance.run_sync(sleep_schedule=self.custom_sleep_schedule_parsed())

        multiply_x_by_y_task = tasks_by_keys["multiply_x_by_y"]

        self.assertTrue(multiply_x_by_y_task.is_waiting())




class TestExtraEnvResolution(BasePipelineTest):

    def dag_gen(self, dsl):
        yield dsl.task(
            key="t1",
            task_conf=TaskConf(
                executer_type="process",
                extra_env={
                    "aaa": "z123",
                    "bbb": "$aaa",
                    "ccc": "$__pipeline_instance_dir/$bbb"
                }
            )
        ).inputs(
            x=3
        ).outputs(
            result=str
        ).calls(
            """
            #!/usr/bin/bash        
            export result=$ccc
            """
        )()

    def validate(self, tasks_by_keys):

        expected_result = os.path.join(self.pipeline_instance_dir, "z123")

        self.assertEqual(
            str(tasks_by_keys["t1"].outputs.result),
            expected_result
        )


class PipelineWithSinglePythonTask(BasePipelineTest):

    def dag_gen(self, dsl):
        tc = self.task_conf()

        if tc.extra_env is None:
            tc.extra_env = {"PYTHONPATH": "/x/y"}
        else:
            pp = tc.extra_env["PYTHONPATH"]
            tc.extra_env["PYTHONPATH"] = f"{pp}:/x/y"

        yield dsl.task(
            key="multiply_x_by_y",
            task_conf=tc
        ).inputs(
            x=3, y=4
        ).outputs(
            result=int
        ).calls(
            multiply_by_x
        )()


    def validate(self, tasks_by_keys):

        if not "multiply_x_by_y" in tasks_by_keys:
            raise Exception("multiply_x_by_y did not complete")

        multiply_x_by_y_task = tasks_by_keys["multiply_x_by_y"]

        if not multiply_x_by_y_task.is_completed():
            raise Exception(f"expected completed, got {multiply_x_by_y_task.state_name()}")

        x = int(multiply_x_by_y_task.inputs.x)
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

    def validate(self, tasks_by_keys):
        task = tasks_by_keys["t1"]

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
            key="t",
            task_conf=self.task_conf()
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



    def validate(self, tasks_by_keys):
        t = tasks_by_keys["t"]

        self.assertTrue(t.is_completed())

        x1 = int(t.outputs.x1)
        x2 = int(t.outputs.x2)
        x3 = int(t.outputs.x3)

        self.assertEqual(x1, 7)
        self.assertEqual(x2, 14)
        self.assertEqual(x3, 21)


@DryPipe.python_call()
def crash_on_first_run_then_succeed(__task_control_dir, x1):

    f = Path(__task_control_dir, "f")
    if not f.exists():
        f.touch()
        raise Exception("expected f")

    return {
        "x2": x1 * 2
    }

class PipelineWithCrashOnFirstRun(BasePipelineTest):

    def dag_gen(self, dsl):
        yield dsl.task(
            key="t",
            task_conf=self.task_conf()
        ).outputs(
            x1=int,
            x2=int,
            x3=int
        ).calls("""
            #!/usr/bin/env bash        
            export x1=7
        """).calls(
            crash_on_first_run_then_succeed
        ).calls(f3)()

    def run_pipeline(self, until_patterns=None):

        pipeline_instance = self.create_pipeline_instance()
        pipeline_instance.monitor=self.create_monitor()

        pipeline_instance.run_sync(
            until_patterns=until_patterns,
            run_tasks_in_process=self.launches_tasks_in_process(),
            sleep_schedule=self.custom_sleep_schedule_parsed()
        )

        tasks_by_keys = pipeline_instance.query_all_tasks_by_key()

        self.assertTrue(tasks_by_keys["t"].is_failed())

        Cli([
            "task",
            f"--pipeline-instance-dir={self.pipeline_instance_dir}",
            "--task-key=t"
        ], test_mode=True).invoke()

        tasks_by_keys = {
            t.key: t
            for t in pipeline_instance.query("*", include_incomplete_tasks=False)
        }

        t = tasks_by_keys["t"]

        self.assertTrue(t.is_completed())

        x1 = int(t.outputs.x1)
        x2 = int(t.outputs.x2)
        x3 = int(t.outputs.x3)

        self.assertEqual(x1, 7)
        self.assertEqual(x2, 14)
        self.assertEqual(x3, 21)

        return pipeline_instance





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

    def output_as_string(self, tasks_by_keys):
        three_phase_task = tasks_by_keys["three_phase_task"]
        with open(three_phase_task.outputs.out_file) as f:
            return f.read()

    def validate(self, tasks_by_keys):
        self.assertEqual(self.output_as_string(tasks_by_keys), "s1\ns2\ns3\n")

class PipelineWith3StepsCrash1(PipelineWith3StepsNoCrash):

    def task_conf(self):
        return TaskConf(
            executer_type="process",
            extra_env={
                "CRASH_STEP_1": "TRUE"
            }
        )

    def is_fail_test(self):
        return True

    def validate(self, tasks_by_keys):
        three_phase_task = tasks_by_keys["three_phase_task"]
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

    def is_fail_test(self):
        return True

    def validate(self, tasks_by_keys):
        three_phase_task = tasks_by_keys["three_phase_task"]
        self.assertTrue(three_phase_task.is_failed())
        self.assertTrue(os.path.exists(three_phase_task.outputs.out_file))
        self.assertEqual(self.output_as_string(tasks_by_keys), "s1\n")


class PipelineWith3StepsCrash3(PipelineWith3StepsCrash1):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            extra_env={
                "CRASH_STEP_3": "TRUE"
            }
        )

    def is_fail_test(self):
        return True

    def validate(self, tasks_by_keys):
        three_phase_task = tasks_by_keys["three_phase_task"]
        self.assertTrue(three_phase_task.is_failed())
        self.assertTrue(os.path.exists(three_phase_task.outputs.out_file))
        self.assertEqual(self.output_as_string(tasks_by_keys), "s1\ns2\n")


@DryPipe.python_call()
def step2_in_python(out_file):

    if os.environ.get("CRASH_STEP_2") == "TRUE":
        raise Exception(f"prescribed crash at CRASH_STEP_2 !")

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
        """,
        container="singularity-test-container.sif"
        ).calls(
            step4_in_python
        )()

        yield three_phase_task


    def output_as_string(self, tasks_by_keys):
        three_phase_task = tasks_by_keys["three_phase_task"]
        with open(three_phase_task.outputs.out_file) as f:
            return f.read()

    def validate(self, tasks_by_keys):
        self.assertEqual(self.output_as_string(tasks_by_keys), "s1\ns2\ns3\ns4\n")


class PipelineWith4MixedStepsCrash(PipelineWith4MixedStepsNoCrash):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            extra_env={
                "CRASH_STEP_3": "TRUE"
            }
        )

    def is_fail_test(self):
        return True

    def validate(self, tasks_by_keys):
        three_phase_task = tasks_by_keys["three_phase_task"]
        self.assertTrue(three_phase_task.is_failed())
        self.assertTrue(os.path.exists(three_phase_task.outputs.out_file))
        self.assertEqual(self.output_as_string(tasks_by_keys), "s1\ns2\n")


class PipelineWith4MixedStepsPythonCrash(PipelineWith4MixedStepsNoCrash):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            extra_env={
                "CRASH_STEP_2": "TRUE"
            }
        )

    def is_fail_test(self):
        return True

    def validate(self, tasks_by_keys):
        three_phase_task = tasks_by_keys["three_phase_task"]
        self.assertTrue(three_phase_task.is_failed())


# Same pipelines with container

task_conf_with_test_container = TaskConf(
    executer_type="process",
    container="singularity-test-container.sif",
    python_bin="python3"
)


class PipelineWithSingleBashTaskInMissingContainer(PipelineWithSingleBashTask):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            container="not-existing-container-file.sif"
        )

    def is_fail_test(self):
        return True

    def validate(self, tasks_by_keys):
        multiply_x_by_y = tasks_by_keys["multiply_x_by_y"]

        self.assertTrue(multiply_x_by_y.is_failed())




class PipelineWithSingleBashTaskInContainer(PipelineWithSingleBashTask):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            container="singularity-test-container.sif",
            apptainer_exec_args=["--nv", "--no-home"]
        )

class PipelineWithSinglePythonTaskInContainer(PipelineWithSinglePythonTask):
    def task_conf(self):
        return TaskConf(
            executer_type="process",
            container="singularity-test-container.sif",
            apptainer_exec_args="--nv",
            python_bin="python3",
            extra_env={
                "PYTHONPATH": Path(__file__).parent.parent.__str__()
            }
        )

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


class TestFileSet(BasePipelineTest):

    def init_pipeline_instance(self, pipeline_instance):

        pid = pipeline_instance.state_file_tracker.pipeline_instance_dir

        data_dir = Path(pid, "data-dir")
        data_dir.mkdir(exist_ok=True)

        def dump_in_file(file_name, str_content):
            with open(Path(data_dir, file_name), "w") as f:
                f.write(str_content)

        dump_in_file("palindrome.txt", "123454321")

    def dag_gen(self, dsl):

        tc = self.task_conf()
        yield dsl.task(
            key="t",
            task_conf=tc
        ).inputs(
            x=3,
            y=5,
            palindrome_file=Path("data-dir/palindrome.txt")
        ).outputs(
            random_files=dsl.file_set("**/*", "*.no"),
            palindrome=int,
            round_trip=dsl.file("rt.txt")
        ).calls(
            """
            #!/usr/bin/bash
                        
            export palindrome=`cat $palindrome_file`            
            cp $palindrome_file $__task_output_dir            
            
            mkdir -p $__task_output_dir/a1/b        
            mkdir -p $__task_output_dir/a2/c/x/y/z            
            
            echo "z" > $__task_output_dir/a1/a.yes
            touch $__task_output_dir/a2/c/x/y/z/pop
            
            echo "z" > $__task_output_dir/z.no                    
            echo "z" > $__task_output_dir/a2/b.txt
            
            touch $round_trip
            """
        )()

    def validate(self, tasks_by_keys):
        self.assertEqual(
            {
                "t/a2/b.txt",
                "t/a1/a.yes",
                "t/a2/c/x/y/z/pop",
                "t/palindrome.txt",
                "t/rt.txt"
            },
            {
                str(Path(f).relative_to(Path(self.pipeline_instance_dir, "output")))
                for f in tasks_by_keys["t"].outputs.random_files
            }
        )

        self.assertEqual(str(tasks_by_keys["t"].outputs.palindrome), "123454321")

        self.assertEqual(int(tasks_by_keys["t"].outputs.palindrome), 123454321)



@DryPipe.python_call()
def test_python_path_env_var(__pipeline_instance_dir):
    from a.b import c
    res = c()
    print(f"--->{res}")
    return {
        "r": res
    }


class TestPythonPathInExtraEnv(BasePipelineTest):

    def init_pipeline_instance(self, pipeline_instance):

        pid = pipeline_instance.state_file_tracker.pipeline_instance_dir

        pp = Path(pid, "tmp")
        pp.mkdir(exist_ok=False)
        a_dir = Path(pp, "a")
        a_dir.mkdir()
        Path(a_dir, "__init__.py").touch()
        with open(Path(a_dir, "b.py"), "w") as b:
            b.write("\n")
            b.write("def c():\n")
            b.write("   return 4321\n")


    def task_conf(self):

        pp = super().task_conf().extra_env["PYTHONPATH"]
        return TaskConf(
            executer_type="process",
            command_before_task="export PYTHONPATH=/x/y",
            extra_env={
                "PYTHONPATH": ":".join([
                    os.path.join(self.pipeline_instance_dir, "tmp"),
                    pp
                ])
            }
        )

    def dag_gen(self, dsl):

        yield dsl.task(
            key="t1",
            task_conf=self.task_conf()
        ).outputs(
            r=int
        ).calls(
            test_python_path_env_var
        )()


    def validate(self, tasks_by_keys):
        if "t1" not in tasks_by_keys:
            raise Exception(f"task t1 did not succeed")
        self.assertEqual(int(tasks_by_keys["t1"].outputs.r), 4321)



class TestPythonPathInExtraEnv2(TestPythonPathInExtraEnv):

    def task_conf(self):
        return TaskConf(
            executer_type="process",
            command_before_task="export PYTHONPATH=",
            extra_env={
                "PYTHONPATH": super().task_conf().extra_env["PYTHONPATH"]
            }
        )


@DryPipe.python_call()
def step0(x):

    print(f"step0: {x}")
    return {
        "result": x * 2
    }

@DryPipe.python_call()
def step1(result):
    print(f"step1: {result}")
    return {
        "result": result * 2
    }

class PipelineWithMultiStepVarPassTrough(BasePipelineTest):

    def dag_gen(self, dsl):

        yield dsl.task(
            key="t",
            task_conf=self.task_conf()
        ).inputs(
            x=3
        ).outputs(
            result=int
        ).calls(
            step0
        ).calls(
            step1
        )()

    def validate(self, tasks_by_keys):
        self.assertEqual(int(tasks_by_keys["t"].outputs.result), 12)


class PipelineWithMultiStepsForRestartTests(BasePipelineTest):

    def dag_gen(self, dsl):

        yield dsl.task(
            key="t"
        ).inputs(
            i=0
        ).outputs(
            results_file=dsl.file("results.txt")
        ).calls(
            exportable_funcs.test_step0_append_to_file
        ).calls(
            exportable_funcs.test_step1_append_to_file
        ).calls(
            exportable_funcs.test_step2_append_to_file
        )()

    def launches_tasks_in_process(self):
        return False

    def initial_run(self, pipeline_instance):

        pipeline_instance.monitor=self.create_monitor()
        pipeline_instance.run_sync(
            until_patterns=None,
            run_tasks_in_process=self.launches_tasks_in_process(),
            sleep_schedule=self.custom_sleep_schedule_parsed()
        )

        tasks_by_keys = pipeline_instance.query_all_tasks_by_key()

        return tasks_by_keys["t"]

    def validate(self, tasks_by_keys):
        pass


class RestartTest(PipelineWithMultiStepsForRestartTests):


    def test_run_pipeline(self):
        pipeline_instance = self.create_pipeline_instance()

        self.save_crash_plan([
         # i: 0
             [0], # step 0
             [1], # step 1
             [0], # step 2
        ])

        t = self.initial_run(pipeline_instance)

        self.assertTrue(t.is_failed())
        self.assertEqual(t.step_idx(), 1)

        def simple_restart():
            with cli_in_sub_process([
                '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
                'restart',
                '--task-key=t',
                '--wait'
            ]) as p:
                p.wait_and_raise_if_non_zero()

            t.refresh_state()

        simple_restart()

        self.assertTrue(t.is_completed())

        self.save_crash_plan([
         # i: 0
             [1], # step 0
             [0], # step 1
             [1], # step 2
        ])

        with cli_in_sub_process([
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'restart',
            '--task-key=t',
            '--reset',
            '--wait'
        ]) as p:
            p.wait_and_raise_if_non_zero()

        t.refresh_state()

        self.assertTrue(t.is_failed())
        self.assertEqual(t.step_idx(), 0)

        simple_restart()

        self.assertTrue(t.is_failed())
        self.assertEqual(t.step_idx(), 2)

        simple_restart()

        self.assertTrue(t.is_completed())


        self.save_crash_plan([
         # i: 0
             [1], # step 0
             [0], # step 1
             [1], # step 2
        ])

        with cli_in_sub_process([
            '--pipeline-instance-dir', pipeline_instance.state_file_tracker.pipeline_instance_dir,
            'restart',
            '--task-key=t',
            '--at-step=1',
            '--wait'
        ]) as p:
            p.wait_and_raise_if_non_zero()

        t.refresh_state()

        self.assertTrue(t.is_failed())
        self.assertEqual(t.step_idx(), 2)


unimplemented = [
    PipelineWithSingleBashTaskExternalReset
]


def all_basic_tests():
    return [
        TestExtraEnvResolution,
        TestFileSet,
        PipelineWith3StepsNoCrash,
        PipelineWith3StepsCrash1,
        PipelineWith3StepsCrash2,
        PipelineWith3StepsCrash3,
        PipelineWith4MixedStepsCrash,
        PipelineWithSinglePythonTask,
        PipelineWithSingleBashTask,
        PipelineWithVarAndFileOutput,
        PipelineWithVarSharingBetweenSteps,
        PipelineWith4MixedStepsPythonCrash,
        TestPythonPathInExtraEnv,
        TestPythonPathInExtraEnv2,
        PipelineWithCrashOnFirstRun,
        PipelineWithMultiStepVarPassTrough
    ]

def all_tests_in_containers():
    return [
        PipelineWith3StepsCrash3InContainer,
        PipelineWithSinglePythonTaskInContainer,
        PipelineWithSingleBashTaskInContainer,
        PipelineWithVarAndFileOutputInContainer,
        PipelineWithVarSharingBetweenStepsInContainer,
        PipelineWithSingleBashTaskInMissingContainer
    ]


def all_tests():
    return all_basic_tests() + all_tests_in_containers()
