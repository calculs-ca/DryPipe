import os
import shutil
from pathlib import Path
import time

import dry_pipe
from dry_pipe_tests.base_pipeline_test import BasePipelineTest
from dry_pipe.cli import cli_in_sub_process
from dry_pipe import TaskConf
from dry_pipe.pipeline_instance import Monitor
from dry_pipe.state_machine import AllRunnableTasksCompletedOrInError
from dry_pipe_tests.test_utils import TestSandboxDir
from dry_pipe_tests.exportable_funcs import test_func, test_step0, test_step1, test_step2, test_step3, digest_all

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
            extra_env={
                "PYTHONPATH": os.environ.get("PYTHONPATH"),
                "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            },
            use_squeue=False
        )

    def launches_tasks_in_process(self):
        return True


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
                    # sleep 100000
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
        return False

    def sbatch_step2(self, dsl):
        return ["--time=30:00"]


class PipelineWithSlurmArrayWithUntil(PipelineWithSlurmArray):

    def launches_tasks_in_process(self):
        return True

    def _test(self):
        #TODO: make --until=x apply to array children
        pipeline_instance = self.run_pipeline(["a-dige*"])

        self.assertTrue(
            pipeline_instance.lookup_single_task("a-digest", include_incomplete_tasks=True).is_ready()
        )

        self.assertTrue(
            pipeline_instance.lookup_single_task("b-digest").is_completed()
        )

        pipeline_instance.run_sync(
            run_tasks_in_process=True,
            sleep_schedule=self.custom_sleep_schedule_parsed()
        )

        for task in pipeline_instance.query("*"):
            if not task.is_completed():
                raise Exception(f"expected {task.key} to be completed, got {task.state_name()}")


class PipelineWithSlurmArrayForRealSlurmTest(BasePipelineTest):

    def launches_tasks_in_process(self):
        return True

    def dag_gen(self, dsl):

        tc = self.task_conf()
        tc.use_squeue = True

        for i in range(1, 4):
            yield dsl.task(
                key=f"t{i}",
                is_slurm_array_child=True,
                task_conf=TaskConf(
                    executer_type="slurm",
                    slurm_account="dummy",
                    extra_env=tc.extra_env,
                    use_squeue=True
                )
            ).inputs(
                x=i
            ).outputs(
                r=int
            ).calls("""
            #!/usr/bin/env bash
            export r=$(($x * $x))
            """)()



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
            extra_env={
                "PYTHONPATH": python_path_for_tests,
                "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            }
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
            extra_env={
                "PYTHONPATH": os.environ.get("PYTHONPATH"),
                "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            }
        )

    def create_monitor(self):

        class M(Monitor):
            def on_task_fail(self, state_file):
                if state_file.task_key == "array_parent":
                    raise AllRunnableTasksCompletedOrInError()

        return M()

    def extra_env(self, dsl):
        return self.task_conf().extra_env

    def dag_gen(self, dsl):

        for i in [0, 1, 2 ,3, 4]:
            yield dsl.task(
                key=f"t_{i}",
                is_slurm_array_child=True,
                task_conf=TaskConf(
                    python_bin="python3",
                    extra_env=self.extra_env(dsl)
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
                sbatch_options=self.sbatch_options_for_step1()
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


    def sbatch_options_for_step1(self):
        return ["--time=30:00"]

    def is_log_level_debug(self):
        return False


    def init_before_run(self):
        crash_plan = [
        # i: 0  1  2  3  4
            [1, 0, 0, 0, 0], # step 0
            [0, 1, 1, 0, 0], # step 1
            [0, 0, 2, 0, 0], # step 2
            [0, 0, 0, 1, 0]  # step 3
        ]

        self.save_crash_plan(crash_plan)

    def test_run_pipeline(self):

        pipeline_instance = self.create_pipeline_instance()

        self.init_before_run()

        pipeline_instance.monitor=self.create_monitor()

        pipeline_instance.run_sync(sleep_schedule=self.custom_sleep_schedule_parsed())

        tasks_by_keys = pipeline_instance.query_all_tasks_by_key()

        t_0 = tasks_by_keys["t_0"]
        t_1 = tasks_by_keys["t_1"]
        t_2 = tasks_by_keys["t_2"]
        t_3 = tasks_by_keys["t_3"]
        t_4 = tasks_by_keys["t_4"]
        array_parent = tasks_by_keys["array_parent"]

        def refresh_state_files():
            for t in [t_0, t_1, t_2, t_3, t_4]:
                t.refresh_state()
            array_parent.refresh_state()

        self.spin_until_no_running_jobs()
        refresh_state_files()

        self.assertTrue(t_0.is_failed())
        self.assertTrue(t_1.is_failed())
        self.assertTrue(t_2.is_failed())
        self.assertTrue(t_3.is_failed())
        self.assertTrue(t_4.is_completed())

        self.assertEqual(t_0.step_idx(), 0)
        self.assertEqual(t_1.step_idx(), 1)
        self.assertEqual(t_2.step_idx(), 1)
        self.assertEqual(t_3.step_idx(), 3)

        from dry_pipe.slurm_array_task import SlurmArrayParentTask

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
            {"t_0/state.failed.0"},
            {str(s) for s in batch_with_no_sbatch_options}
        )

        self.assertEqual(
            {"t_1/state.failed.1",
             "t_2/state.failed.1",
             "t_3/state.failed.3"},
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

        refresh_state_files()

        self.assertTrue(t_0.is_completed())
        self.assertTrue(t_1.is_completed())
        self.assertTrue(t_2.is_failed())
        self.assertTrue(t_3.is_completed())
        self.assertTrue(t_4.is_completed())

        self.assertEqual(t_2.step_idx(), 2)
        self.assertTrue(array_parent.is_failed())





class PipelineWithAutoRestart1(PipelineWithMultiStepSlurmArrayWithMultiSbatchOptionsWithCrashAndRestarts):

    def task_conf_for_remote_tests(self):
        rts = self.remote_test_site()
        tc = TaskConf(
            executer_type="slurm",
            ssh_remote_dest=rts.ssh_remote_dst(),
            sbatch_options=rts.sbatch_options,
            extra_env={
                "DRYPIPE_TASK_DEBUG": "True" if self.is_log_level_debug() else "False",
                "PYTHONPATH": self.python_path_for_remote_site(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            },
            auto_restart_condition_regexp_per_log_file={
                "out.log": [".*predicted_crash.*"]
            }
        )
        tc.python_bin = None
        return tc

    def launches_tasks_in_process(self):
        return False

    def task_conf(self):
        if self.remote_test_site() is not None:
            return self.task_conf_for_remote_tests()
        else:
            return TaskConf(
                executer_type="slurm",
                extra_env={
                    "PYTHONPATH": os.environ.get("PYTHONPATH"),
                    "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                    "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
                },
                auto_restart_condition_regexp_per_log_file={
                    "out.log": [".*predicted_crash.*"]
                },
                use_squeue=True
            )

    def sbatch_options_for_step1(self):
        return None

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

        pipeline_instance.run_sync(sleep_schedule=self.custom_sleep_schedule_parsed())

        tasks_by_keys = pipeline_instance.query_all_tasks_by_key()

        t_0 = tasks_by_keys["t_0"]
        t_1 = tasks_by_keys["t_1"]
        t_2 = tasks_by_keys["t_2"]
        t_3 = tasks_by_keys["t_3"]
        t_4 = tasks_by_keys["t_4"]
        t_parent = tasks_by_keys["array_parent"]

        self.assertTrue(t_0.is_completed())
        self.assertTrue(t_1.is_completed())
        self.assertTrue(t_2.is_completed())
        self.assertTrue(t_3.is_completed())
        self.assertTrue(t_4.is_completed())
        self.assertTrue(t_parent.is_completed())


class PipelineWithAutoRestart2(PipelineWithAutoRestart1):

    def test_run_pipeline(self):

        pipeline_instance = self.create_pipeline_instance()

        crash_plan = [
        # i: 0  1  2  3  4
            [1, 0, 0, 0, 4], # step 0
            [0, 1, 1, 0, 0], # step 1
            [0, 0, 2, 0, 0], # step 2
            [3, 0, 0, 1, 0]  # step 3
        ]

        self.save_crash_plan(crash_plan)

        pipeline_instance.monitor=self.create_monitor()

        pipeline_instance.run_sync(sleep_schedule=self.custom_sleep_schedule_parsed())

        tasks_by_keys = pipeline_instance.query_all_tasks_by_key()

        t_0 = tasks_by_keys["t_0"]
        t_1 = tasks_by_keys["t_1"]
        t_2 = tasks_by_keys["t_2"]
        t_3 = tasks_by_keys["t_3"]
        t_4 = tasks_by_keys["t_4"]
        t_parent = tasks_by_keys["array_parent"]

        self.assertTrue(t_0.is_failed())
        self.assertTrue(t_1.is_completed())
        self.assertTrue(t_2.is_completed())
        self.assertTrue(t_3.is_completed())
        self.assertTrue(t_4.is_failed())
        self.assertTrue(t_parent.is_failed())



class PipelineWithAutoRestart1Funky(PipelineWithAutoRestart1):
    """
        causes almost twice as many sbatch array restarts, takes longer to run
    """
    def sbatch_options_for_step1(self):
        return ["--time=30:00"]

class PipelineWithAutoRestart2Funky(PipelineWithAutoRestart2):
    def sbatch_options_for_step1(self):
        return ["--time=30:00"]



class PipelineWithPartialArrayMatch(BasePipelineTest):
    """
      Tests for dsl.query_all_or_nothing( ... min_matches=N)
    """

    def task_conf(self):
        return TaskConf(
            executer_type="slurm",
            extra_env={
                "PYTHONPATH": os.environ.get("PYTHONPATH"),
                "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            },
            use_squeue=True
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
                slurm_result=int,
                f=dsl.file("f.txt")
            ).calls(
                test_step0
            ).calls(
                test_step1
            ).calls(
                """
                #!/usr/bin/bash
                echo "$i" > $f
                """
            )()

        for match in dsl.query_all_or_nothing("t_*", state="ready"):
            array_parent = dsl.task(
                key=f"array_parent",
                task_conf=self.task_conf(),
                downstream_resets=["digest"]
            ).slurm_array_parent(
                children_tasks=match.tasks
            )()

            yield array_parent

            if array_parent.has_ended():
                yield dsl.task(
                    key=f"digest",
                    task_conf=TaskConf(
                        python_bin="python3",
                        extra_env=None
                    )
                ).outputs(
                    results=dsl.file("results.txt")
                ).calls(
                    digest_all
                )()

    def init_instance(self):
        crash_plan = [
        # i: 0  1  2  3  4
            [0, 0, 0, 1, 0], # step 0
            [0, 0, 0, 0, 2], # step 1
            [0, 0, 0, 0, 0], # step 2
        ]

        self.save_crash_plan(crash_plan)

    def launches_tasks_in_process(self):
        return False

    def _test_regression_1(self):
        d = TestSandboxDir(self)
        self.pipeline_instance_dir = d.sandbox_dir

        z = Path(__file__).parent.joinpath("regression_data").joinpath("PipelineWithPartialArrayMatchRemote.test_run_pipeline")

        shutil.copytree(str(z), self.pipeline_instance_dir, dirs_exist_ok=True)

        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)

        pipeline_instance.run_sync(sleep_schedule=self.custom_sleep_schedule_parsed())

        for t, task in pipeline_instance.query_all_tasks_by_key().items():
            self.assertTrue(task.is_completed())


    def test_run_pipeline(self):

        d = TestSandboxDir(self)
        self.pipeline_instance_dir = d.sandbox_dir

        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)

        self.init_instance()

        pipeline_instance.run_sync(sleep_schedule=self.custom_sleep_schedule_parsed())

        self.pipeline_instance = pipeline_instance

        tasks_by_keys = pipeline_instance.query_all_tasks_by_key()

        t_0 = tasks_by_keys["t_0"]
        t_1 = tasks_by_keys["t_1"]
        t_2 = tasks_by_keys["t_2"]
        t_3 = tasks_by_keys["t_3"]
        t_4 = tasks_by_keys["t_4"]
        array_parent = tasks_by_keys["array_parent"]
        digest = tasks_by_keys["digest"]

        self.assertTrue(array_parent.is_failed())
        self.assertTrue(t_0.is_completed())
        self.assertTrue(t_1.is_completed())
        self.assertTrue(t_2.is_completed())
        self.assertTrue(t_3.is_failed())
        self.assertTrue(t_4.is_failed())
        self.assertTrue(digest.is_completed())

        self.assertEqual(t_0.outputs.f.content_as_string_if_exists(), "0\n")
        self.assertEqual(t_1.outputs.f.content_as_string_if_exists(), "1\n")
        self.assertEqual(t_2.outputs.f.content_as_string_if_exists(), "2\n")
        self.assertEqual(t_3.outputs.f.content_as_string_if_exists(), None)
        self.assertEqual(t_4.outputs.f.content_as_string_if_exists(), None)
        self.assertEqual(t_4.outputs.f.content_as_string_if_exists(), None)

        self.assertEqual(digest.outputs.results.content_as_string_if_exists(), "t_0,t_1,t_2")


        #test_cli(
        #    self,
        #    '--pipeline-instance-dir', self.pipeline_instance_dir,
        #    'restart',
        #    '--task-key', 'array_parent', '--wait'
        #)
        with cli_in_sub_process([
            'restart',
            '--pipeline-instance-dir', self.pipeline_instance_dir,
            '--task-key', 'array_parent', '--wait'
        ]) as p:
            p.wait_and_raise_if_non_zero()

        self.assertEqual(digest.outputs.results.content_as_string_if_exists(), None)

        self.assertEqual(t_0.outputs.f.content_as_string_if_exists(), "0\n")
        self.assertEqual(t_1.outputs.f.content_as_string_if_exists(), "1\n")
        self.assertEqual(t_2.outputs.f.content_as_string_if_exists(), "2\n")
        self.assertEqual(t_3.outputs.f.content_as_string_if_exists(), "3\n")
        self.assertEqual(t_4.outputs.f.content_as_string_if_exists(), None)

        with cli_in_sub_process([
            'restart',
            '--pipeline-instance-dir', self.pipeline_instance_dir,
            '--task-key', 'array_parent', '--wait'
        ]) as p:
            p.wait_and_raise_if_non_zero()


        self.assertEqual(t_0.outputs.f.content_as_string_if_exists(), "0\n")
        self.assertEqual(t_1.outputs.f.content_as_string_if_exists(), "1\n")
        self.assertEqual(t_2.outputs.f.content_as_string_if_exists(), "2\n")
        self.assertEqual(t_3.outputs.f.content_as_string_if_exists(), "3\n")
        self.assertEqual(t_4.outputs.f.content_as_string_if_exists(), "4\n")

        pipeline_instance.reset_state_tracker()

        pipeline_instance.run_sync(sleep_schedule=self.custom_sleep_schedule_parsed())

        self.assertEqual(digest.outputs.results.content_as_string_if_exists(), "t_0,t_1,t_2,t_3,t_4")

    def is_log_level_debug(self):
        return True



@dry_pipe.DryPipe.python_call()
def simple_array_x(x, __scratch_dir, __task_process):

    #if x == 2 and False:
    #    raise Exception("i == 2 !!")
    
    #if x == 9 and False:
    #    print("will sleep for ever")
    #    time.sleep(60*60*10000)
    __scratch_dir = Path(__scratch_dir)
    zaz = __scratch_dir.joinpath("zaz")
    zaz.mkdir(exist_ok=False)
    __scratch_dir.joinpath("popo").touch(exist_ok=False)

    ls = ",".join([
        f"{e}"
        for e in __scratch_dir.glob("*")
    ])
    __task_process.task_logger.info(f"{ls}")

    return {
        "r": x * 2
    }


def tc_ar():
    return TaskConf(
        executer_type="slurm",
        slurm_account="dummy-account",
        extra_env={
            "PYTHONPATH": python_path_for_tests,
            "DRYPIPE_TASK_DEBUG": "True",
            "DRYPIPE_SLEEP_SCHEDULE": "1"
        }
    ).with_sbatch_options(time="1:00:1")


# drypipe array-submit -pid dry_pipe_tests/sandboxes/ta1 --generator=dry_pipe_tests.pipeline_tests_with_slurm_arrays:dag_simple_array -k ap


# drypipe array-submit -pid dry_pipe_tests/sandboxes/ta1 --generator=dry_pipe_tests.pipeline_tests_with_slurm_arrays:dag_simple_array -k ap --packed-job-size=2

def dag_simple_array(dsl):

    def g():
        for i in range(1, 12):
            yield dsl.task(
                key=f"t{i:02d}",
                is_slurm_array_child=True,
                task_conf=tc_ar()
            ).inputs(
                x=i
            ).outputs(
                r=int
            ).calls(
                simple_array_x
            ).calls("""
            #!/usr/bin/env bash
            echo "..."
            """)()

    tasks = list(g())

    yield from tasks

    yield dsl.task(
        key="ap",
        task_conf=tc_ar()
    ).slurm_array_parent(
        children_tasks=tasks
    )()

all_tests = [
    PipelineWithMultiCallSlurmArrayForRealSlurmTest,
    PipelineWithSlurmArrayWithUntil,
    PipelineWithSlurmArray2StepsWith2Sbatch,
    PipelineWithMultiStepSlurmArrayWithMultiSbatchOptionsWithCrashAndRestarts,
    PipelineWithAutoRestart1,
    PipelineWithAutoRestart2,
    PipelineWithPartialArrayMatch
]

tests_with_funky_corner_cases = [
    PipelineWithAutoRestart1Funky,
    PipelineWithAutoRestart2Funky
]
