import os

import dry_pipe
from base_pipeline_test import BasePipelineTest
from dry_pipe import TaskConf
from tests.exportable_funcs import test_func


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



class PipelineWithSlurmArray(BasePipelineTest):

    def task_conf(self):
        return TaskConf(
            executer_type="slurm", slurm_account="dummy",
            extra_env={"DRYPIPE_TASK_DEBUG": "True", "PYTHONPATH": os.environ.get("PYTHONPATH")}
        )

    def dag_gen(self, dsl):

        t0 = dsl.task(
            key="z"
        ).outputs(
            r=int,
            f=dsl.file('f.txt')
        ).calls("""
            #!/usr/bin/env bash            
            export r=10        
            echo 25 > $f            
        """)()

        yield t0

        exportable_funcs_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "exportable_funcs.py"
        )

        for i in [1, 2]:
            for c in ["a", "b"]:
                yield dsl.task(
                    key=f"t_{c}_{i}",
                    is_slurm_array_child=True,
                    task_conf=self.task_conf()
                ).inputs(
                    r=t0.outputs.r,
                    i=i,
                    f=t0.outputs.f,
                    code_dep=dsl.file(exportable_funcs_file)
                ).outputs(
                    slurm_result=int,
                    slurm_result_in_file=dsl.file('slurm_result.txt'),
                    var_result=int
                ).calls("""
                    #!/usr/bin/env bash
                    echo "$r $i"  
                    value_f=$(<$f)          
                    export slurm_result=$(( $r + $i + $value_f))
                    echo "__task_output_dir=$__task_output_dir"
                    echo "slurm_result_in_file=$slurm_result_in_file"                    
                    echo "$slurm_result"
                    mkdir -p $__task_output_dir
                    echo "$slurm_result" > $slurm_result_in_file
                """).calls(
                    test_func
                )()

        for match in dsl.query_all_or_nothing("t_*", state="ready"):
            yield dsl.task(
                key=f"array_parent",
                task_conf=self.task_conf()
            ).slurm_array_parent(
                children_tasks=match.tasks
            )()


        for _ in dsl.query_all_or_nothing("t_a_*"):
            yield dsl.task(
                key=f"a-digest"
            )()

        for _ in dsl.query_all_or_nothing("t_b_*"):
            yield dsl.task(
                key=f"b-digest"
            )()

    def validate(self, tasks_by_keys):

        for k, t in tasks_by_keys.items():
            self.assertEqual(t.state_name(), "state.completed", f"unexpected state for {k}")

        self.assertEqual(
            int(tasks_by_keys["t_a_1"].outputs.slurm_result),
            11+25
        )

        self.assertEqual(
            int(tasks_by_keys["t_a_2"].outputs.slurm_result),
            12+25
        )

        self.assertEqual(
            int(tasks_by_keys["t_b_1"].outputs.slurm_result),
            11+25
        )

        self.assertEqual(
            int(tasks_by_keys["t_b_2"].outputs.slurm_result),
            12+25
        )

        for k, t in tasks_by_keys.items():
            if k.startswith("t_"):
                with open(t.outputs.slurm_result_in_file) as f:
                    r = int(f.read().strip())
                    expected = int(t.outputs.slurm_result)
                    self.assertEqual(expected, r, "slurm_result_in_file does not match expected result")
                    self.assertEqual(expected, int(t.outputs.var_result), "var_result does not match expected result")


class PipelineWithSlurmArrayWithUntil(PipelineWithSlurmArray):

    def launches_tasks_in_process(self):
        return True

    def test(self):
        #TODO: make --until=x apply to array children
        pipeline_instance = self.run_pipeline(["a-dige*"])

        self.assertTrue(
            pipeline_instance.lookup_single_task("a-digest", include_incomplete_tasks=True).is_ready()
        )

        self.assertTrue(
            pipeline_instance.lookup_single_task("b-digest").is_completed()
        )

        pipeline_instance.run_sync(
            run_tasks_in_process=True
        )

        for task in pipeline_instance.query("*"):
            if not task.is_completed():
                raise Exception(f"expected {task.key} to be completed, got {task.state_name()}")


class PipelineWithSlurmArrayForRealSlurmTest(BasePipelineTest):

    def launches_tasks_in_process(self):
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
            ).calls("""
            #!/usr/bin/env bash
            export r=$(($x * $x))
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
        res = 0
        for task_key, task in tasks_by_keys.items():
            if task_key.startswith("t"):
                res += int(task.outputs.r)

        self.assertEqual(res, 14)



@dry_pipe.DryPipe.python_call()
def array_test_1(r):
    pass

@dry_pipe.DryPipe.python_call()
def array_test_2(i, __task_output_dir):

    flag = os.path.join(__task_output_dir, "ok")
    if not os.path.exists(flag):
        raise Exception(f"expected failure, {flag} is not found")

    return {
        "r": i * i
    }

@dry_pipe.DryPipe.python_call()
def array_test_3(i, r):

    print(f"3--->{i}, {r}")

    return {
        "r2": r + i
    }


class PipelineWithSlurmArrayForRestarts(BasePipelineTest):

    def task_conf(self):
        return TaskConf(executer_type="slurm", slurm_account="dummy", extra_env={"DRYPIPE_TASK_DEBUG": "True"})

    def dag_gen(self, dsl):

        for i in [1, 2]:
            yield dsl.task(
                key=f"t_{i}",
                is_slurm_array_child=True,
                task_conf=self.task_conf()
            ).inputs(
                i=i,
                r=0
            ).outputs(
                r=int,
                r2=int
            ).calls(
                array_test_1
            ).calls(
                array_test_2
            ).calls(
                array_test_3
            )()

        for match in dsl.query_all_or_nothing("t_*", state="ready"):
            yield dsl.task(
                key=f"array_parent",
                task_conf=self.task_conf()
            ).slurm_array_parent(
                children_tasks=match.tasks
            )()

    def validate(self, tasks_by_keys):

        t1 = tasks_by_keys["t_1"]
        t2 = tasks_by_keys["t_2"]

        self.assertEqual(int(t1.outputs.r), 1)
        self.assertEqual(int(t1.outputs.r2), 2)

        self.assertEqual(int(t2.outputs.r), 3)
        self.assertEqual(int(t2.outputs.r2), 5)


all_tests = [
    PipelineWithMultiCallSlurmArrayForRealSlurmTest,
    PipelineWithSlurmArrayWithUntil
]