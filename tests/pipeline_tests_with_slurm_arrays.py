import os
from pathlib import Path

import dry_pipe
from base_pipeline_test import BasePipelineTest
from dry_pipe import TaskConf
from dry_pipe.pipeline_instance import Monitor
from dry_pipe.state_machine import AllRunnableTasksCompletedOrInError
from dry_pipe.slurm_array_task import SlurmArrayParentTask
from tests.exportable_funcs import test_func, test_step0, test_step1, test_step2, test_step3, digest_all

python_path_for_tests = str(Path(__file__).resolve().parent.parent)

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


exportable_funcs_file = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "exportable_funcs.py"
)


class PipelineWithSlurmArray(BasePipelineTest):

    def task_conf(self):
        return TaskConf(
            executer_type="slurm",
            #slurm_account="dummy",
            extra_env={"DRYPIPE_TASK_DEBUG": "True", "PYTHONPATH": os.environ.get("PYTHONPATH")}
        )

    def create_monitor(self):

        class M(Monitor):
            def on_task_fail(self, state_file):
                if state_file.task_key == "array_parent":
                    raise AllRunnableTasksCompletedOrInError()

        return M()

    def sbatch_step2(self, dsl):
        return None

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

        for i in [1, 2]:
            for c in ["a", "b"]:
                yield dsl.task(
                    key=f"t_{c}_{i}",
                    is_slurm_array_child=True,
                    task_conf=TaskConf(
                        python_bin="python3",
                        extra_env=self.task_conf().extra_env
                    )
                ).inputs(
                    r=t0.outputs.r,
                    i=i,
                    f=t0.outputs.f,
                    code_dep=dsl.file(exportable_funcs_file)
                ).outputs(
                    slurm_result=int,
                    slurm_result_in_file=dsl.file('slurm_result.txt'),
                    var_result=int,
                    random_files=dsl.file_set("**/*", "b.*")
                ).calls("""
                    #!/usr/bin/env bash
                    echo "$r $i"  
                    value_f=$(<$f)          
                    export slurm_result=$(( $r + $i + $value_f))
                    echo "__task_output_dir=$__task_output_dir"
                    echo "slurm_result_in_file=$slurm_result_in_file"                    
                    echo "$slurm_result"
                    echo "$slurm_result" > $slurm_result_in_file
                    
                    mkdir -p $__task_output_dir/sub1/a
                    echo "123" > $__task_output_dir/sub1/a/a.txt
                    mkdir -p $__task_output_dir/sub2/a
                    echo "123" > $__task_output_dir/sub2/a/b.txt                    
                    echo "123" > $__task_output_dir/a.txt
                """).calls(
                    test_func,
                    sbatch_options=self.sbatch_step2(dsl)
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


class PipelineWithSlurmArray2StepsWith2Sbatch(PipelineWithSlurmArray):

    def launches_tasks_in_process(self):
        return True

    def sbatch_step2(self, dsl):
        return ["--time=30:00"]


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
        return TaskConf(
            executer_type="slurm", slurm_account="dummy",
            extra_env={"DRYPIPE_TASK_DEBUG": "True", "PYTHONPATH": python_path_for_tests}
        )

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


class PipelineWithMultiStepSlurmArrayWithMultiSbatchOptionsWithCrashAndRestarts(BasePipelineTest):

    def task_conf(self):
        return TaskConf(
            executer_type="slurm",
            # slurm_account="dummy",
            extra_env={"DRYPIPE_TASK_DEBUG": "True", "PYTHONPATH": os.environ.get("PYTHONPATH")}
        )

    def create_monitor(self):

        class M(Monitor):
            def on_task_fail(self, state_file):
                if state_file.task_key == "array_parent":
                    raise AllRunnableTasksCompletedOrInError()

        return M()

    def dag_gen(self, dsl):

        for i in [0, 1, 2 ,3, 4]:
            yield dsl.task(
                key=f"t_{i}",
                is_slurm_array_child=True,
                task_conf=TaskConf(
                    python_bin="python3",
                    extra_env=self.task_conf().extra_env
                )
            ).inputs(
                i=i,
                code_dep=dsl.file(exportable_funcs_file)
            ).outputs(
                slurm_result=int
            ).calls(
                test_step0
            ).calls(
                test_step1,
                sbatch_options=["--time=30:00"]
            ).calls(
                test_step2
            ).calls(
                test_step3
            )()

        for match in dsl.query_all_or_nothing("t_*", state="ready"):
            yield dsl.task(
                key=f"array_parent",
                task_conf=self.task_conf()
            ).slurm_array_parent(
                children_tasks=match.tasks
            )()


    def test_run_pipeline(self):

        pipeline_instance = self.create_pipeline_instance()

        crash_plan = [
        # i: 0  1  2  3  4
            [1, 0, 0, 0, 0], # step 0
            [0, 1, 1, 0, 0], # step 1
            [0, 0, 2, 0, 0], # step 2
            [0, 0, 0, 1, 0]  # step 3
        ]

        self.save_crash_plan(crash_plan)

        pipeline_instance.monitor=self.create_monitor()

        pipeline_instance.run_sync()

        tasks_by_keys = {
            t.key: t
            for t in pipeline_instance.query("*", include_incomplete_tasks=True)
        }

        t_0 = tasks_by_keys["t_0"]
        t_1 = tasks_by_keys["t_1"]
        t_2 = tasks_by_keys["t_2"]
        t_3 = tasks_by_keys["t_3"]
        t_4 = tasks_by_keys["t_4"]

        self.assertTrue(t_0.is_failed())
        self.assertTrue(t_1.is_failed())
        self.assertTrue(t_2.is_failed())
        self.assertTrue(t_3.is_failed())
        self.assertTrue(t_4.is_completed())

        self.assertEqual(t_0.step_idx(), 0)
        self.assertEqual(t_1.step_idx(), 1)
        self.assertEqual(t_2.step_idx(), 1)
        self.assertEqual(t_3.step_idx(), 3)

        array_parent = tasks_by_keys["array_parent"]
        sapt = SlurmArrayParentTask(array_parent.task_process)

        next_task_state_files = list(sapt.iterate_next_task_state_files(None, True, False, dry_run=True))

        array_batches_for_restart =  list(sapt.split_into_steps_with_sbatch_options(next_task_state_files))

        self.assertIsNone(array_batches_for_restart[0][0])

        self.assertEqual(len(array_batches_for_restart), 2)

        batch_with_no_sbatch_options = [
            state_files
            for sbo, state_files in array_batches_for_restart
            if sbo is None
        ][0]

        self.assertEqual(len(batch_with_no_sbatch_options), 1)

        batch_with_sbatch_options = [
            state_files
            for sbo, state_files in array_batches_for_restart
            if sbo is not None
        ][0]

        self.assertEqual(len(batch_with_sbatch_options), 3)

        self.assertEqual(
            {"/t_0/state.failed.0"},
            {str(s) for s in batch_with_no_sbatch_options}
        )

        self.assertEqual(
            {"/t_1/state.failed.1",
             "/t_2/state.failed.1",
             "/t_3/state.failed.3"},
            {str(s) for s in batch_with_sbatch_options}
        )

        n_submitted = sapt.prepare_and_launch_next_array(None, restart_failed=True)

        self.assertEqual(n_submitted, 4)

        self.spin_until_no_running_jobs()

        self.assertEqual(
            set([k for k, _ in sapt.task_keys_in_i_th_array_file(1)]),
            {"t_0"}
        )

        self.assertEqual(
            set([k for k, _ in sapt.task_keys_in_i_th_array_file(2)]),
            {"t_1", "t_2", "t_3"}
        )

        def refresh_state_files():
            for t in [t_0, t_1, t_2, t_3, t_4]:
                t.refresh_state()

        refresh_state_files()

        self.assertTrue(t_0.is_completed())
        self.assertTrue(t_1.is_completed())
        self.assertTrue(t_2.is_failed())
        self.assertTrue(t_3.is_completed())
        self.assertTrue(t_4.is_completed())

        self.assertEqual(t_2.step_idx(), 2)


class PipelineWithPartialArrayDep(BasePipelineTest):

    def task_conf(self):
        return TaskConf(
            executer_type="slurm",
            extra_env={"DRYPIPE_TASK_DEBUG": "True", "PYTHONPATH": os.environ.get("PYTHONPATH")}
        )

    def create_monitor(self):

        class M(Monitor):
            def on_task_fail(self, state_file):
                if state_file.task_key == "array_parent":
                    raise AllRunnableTasksCompletedOrInError()

        return M()

    def dag_gen(self, dsl):

        for i in [0, 1, 2 ,3, 4]:
            yield dsl.task(
                key=f"t_{i}",
                is_slurm_array_child=True,
                task_conf=TaskConf(
                    python_bin="python3",
                    extra_env=self.task_conf().extra_env
                )
            ).inputs(
                i=i,
                code_dep=dsl.file(exportable_funcs_file)
            ).outputs(
                slurm_result=int
            ).calls(
                test_step0
            ).calls(
                test_step1
            )()

        for match in dsl.query_all_or_nothing("t_*", state="ready"):
            yield dsl.task(
                key=f"array_parent",
                task_conf=self.task_conf()
            ).slurm_array_parent(
                children_tasks=match.tasks
            )()

        for match in dsl.query_all_or_nothing("t_*", state="completed", min_matches=3):
            yield dsl.task(
                key=f"digest",
                task_conf=self.task_conf()
            ).outputs(
                slurm_result=int
            ).calls(
                digest_all
            )()

    def init_instance(self):
        crash_plan = [
        # i: 0  1  2  3  4
            [0, 0, 0, 1, 0], # step 0
            [0, 0, 0, 0, 2], # step 1
        ]

        self.save_crash_plan(crash_plan)

    def test_run_pipeline(self):
        pipeline_instance = self.create_pipeline_instance()

        self.init_instance()
        #pipeline_instance.monitor=self.create_monitor()

        pipeline_instance.run_sync()

        tasks_by_keys = {
            t.key: t
            for t in pipeline_instance.query("*", include_incomplete_tasks=True)
        }



all_tests = [
    PipelineWithMultiCallSlurmArrayForRealSlurmTest,
    PipelineWithSlurmArrayWithUntil,
    PipelineWithSlurmArray2StepsWith2Sbatch,
    PipelineWithMultiStepSlurmArrayWithMultiSbatchOptionsWithCrashAndRestarts
]
