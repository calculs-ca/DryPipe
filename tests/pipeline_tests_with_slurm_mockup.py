import glob
import os.path
import shutil
import time
from functools import reduce
from pathlib import Path
from typing import Tuple, List

import dry_pipe
from base_pipeline_test import BasePipelineTest, TestWithDirectorySandbox
from dry_pipe import TaskConf
from dry_pipe.core_lib import StateFileTracker
from dry_pipe.task_process import SlurmArrayParentTask, TaskProcess
from mockups import TaskMockup
from test_state_machine import StateMachineTester


def throttle(share_dir, threshold, task_key):

    def count_in_queue():
        return len(glob.glob(os.path.join(share_dir, "*")))


    while True:
        if count_in_queue() >= threshold:
            time.sleep(5)
            continue
        else:
            break

    Path(os.path.join(share_dir, task_key)).touch()



def format_sbatch_array(array_indexes):

    def is_consecutive(n1, n2):
        return n1 + 1 == n2

    array_indexes = list(sorted(set(array_indexes)))

    def f(res, i):
        match res:
            case None:
                return [[i]]
            case [*o, [m]]:
                if is_consecutive(m, i):
                    return o + [[m, i]]
                else:
                    return o + [[m]] + [[i]]
            case [*o, [m, n]]:
                if is_consecutive(n, i):
                    return o + [[m, i]]
                else:
                    return o + [[m, n]] + [[i]]

    r1 = reduce(f, array_indexes, None)

    def g():
        for t in r1:
            match t:
                case [x]:
                    yield f"{x}"
                case [i, j]:
                    yield f"{i}-{j}"

    r2 = ",".join(g())
    print(f"{r2}")
    return r2


@dry_pipe.DryPipe.python_call()
def test_func(r, i, f):

    with open(f) as _f:
        f_int = int(_f.read().strip())

        return {
            "var_result": r + i + f_int
        }


class PipelineWithSlurmArray(BasePipelineTest):

    def task_conf(self):
        return TaskConf(executer_type="slurm", slurm_account="dummy", extra_env={"DRYPIPE_TASK_DEBUG": "True"})

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
                    task_conf=self.task_conf()
                ).inputs(
                    r=t0.outputs.r,
                    i=i,
                    f=t0.outputs.f
                ).outputs(
                    slurm_result=int,
                    slurm_result_in_file=dsl.file('slurm_result.txt'),
                    var_result=int
                ).calls("""
                    #!/usr/bin/env bash
                    echo "$r $i"  
                    value_f=$(<$f)          
                    export slurm_result=$(( $r + $i + $value_f))
                    echo "$slurm_result"
                    mkdir -p $__task_output_dir
                    echo "$slurm_result" > $slurm_result_in_file
                """).calls(test_func)()

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



class SlurmArrayHandingStateMachineTest(TestWithDirectorySandbox):

    def test_slurm_array_state_machine(self):

        t = PipelineWithSlurmArray()

        tester = StateMachineTester(self, lambda dsl: t.dag_gen(dsl), StateFileTracker(self.pipeline_instance_dir))

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready('z', 'array_parent')

        tester.set_completed_on_disk("t_a_1")
        tester.set_completed_on_disk("t_b_1")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready()

        tester.set_completed_on_disk("t_a_2")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready("a-digest")



class BaseSlurmArrayScenarioWithSlurmMockup(TestWithDirectorySandbox):

    def setUp(self):

        d = Path(self.pipeline_instance_dir)
        if d.exists():
            shutil.rmtree(d)

        self.parent_task_key, self.children_task_keys = self.get_slurm_array_task_keys()

        self.task_control_dir = os.path.join(
            self.pipeline_instance_dir,
            ".drypipe",
            self.parent_task_key
        )

        self.current_step = 0
        self.current_squeue_call = 0

        self.tracker = StateFileTracker(pipeline_instance_dir=self.pipeline_instance_dir)

        self.parent_task_state_file = self.tracker.create_true_state_if_new_else_fetch_from_memory(
            TaskMockup(self.parent_task_key)
        )

        self.parent_task = SlurmArrayParentTask(TaskProcess(self.task_control_dir))

        for k in self.children_task_keys:
            self.tracker.create_true_state_if_new_else_fetch_from_memory(TaskMockup(k))
            self.tracker.set_ready_on_disk_and_in_memory(k)

        with open(self.parent_task.task_keys_file(), "w") as f:
            for k in self.children_task_keys:
                f.write(f"{k}\n")

    def load_task_keys(self, f):
        def gen():
            with open(f) as _f:
                for k in _f:
                    yield k.strip()
        return set(gen())

    def assert_task_keys_in_array_file(self, array_number: int, task_keys: set[str]):
        self.assertEqual(
            self.load_task_keys(self.parent_task.i_th_array_file(array_number)),
            task_keys
        )

    def set_states_on_disc(self, task_key_to_state_base_name):
        for k, state_base_name in task_key_to_state_base_name.items():
            self.tracker.set_step_state_on_disk_and_in_memory(k, state_base_name)

    def assert_task_state_file_states(self, task_key_to_drypipe_state: dict[str, str]):

        errors = []
        for task_key, expected_state in task_key_to_drypipe_state.items():
            _, state_file = self.tracker.fetch_true_state_and_update_memory_if_changed(task_key)
            state_on_disc = state_file.state_as_string()
            if state_on_disc != expected_state:
                errors.append(f"expected {task_key} to be {expected_state}, got {state_on_disc}")

        if len(errors) > 0:
            raise Exception(f"\n".join(errors))

    def get_task_conf(self):
        return TaskConf(
            executer_type="slurm",
            slurm_account="zaz"
        )

    def get_slurm_array_task_keys(self)-> Tuple[str, List[str]]:
        """
        :return: parent_task_key->str, children_task_keys: List[str]
        """
        raise NotImplementedError()


class SlurmArrayNormalScenario1(BaseSlurmArrayScenarioWithSlurmMockup):

    def get_slurm_array_task_keys(self):
        return "parent_task", ["t1", "t2", "t3", "t4"]

    def get_task_conf(self):
        return TaskConf.default()

    def test(self):

        size_of_array_launched = self.parent_task.prepare_and_launch_next_array(
            call_sbatch_mockup=lambda: 1232
        )

        self.assertEqual(size_of_array_launched, 4)

        self.assertTrue(
            os.path.exists(self.parent_task.i_th_array_file(0))
        )

        self.assertTrue(
            os.path.exists(self.parent_task.i_th_submitted_array_file(0, 1232))
        )

        self.assert_task_keys_in_array_file(0, {"t1", "t2", "t3", "t4"})

        size_of_array_launched = self.parent_task.prepare_and_launch_next_array(
            call_sbatch_mockup=lambda: 234324234
        )

        self.assertEqual(size_of_array_launched, 0)

        self.assertEqual(
            dict(self.parent_task.task_keys_in_i_th_array_file(0)),
            {"t1": 0, "t2": 1, "t3": 2, "t4": 3}
        )

        self.assert_task_state_file_states({
            "t1": "state._step-started",
            "t2": "state._step-started",
            "t3": "state._step-started",
            "t4": "state._step-started"
        })

        self.set_states_on_disc({
            "t2": "state.step-started.0",
            "t4": "state.step-started.0"
        })

        dict_unexpected_states = self.parent_task.mock_compare_and_reconcile_squeue_with_state_files([
            "1232_0 PD",
            "1232_1 R",
            "1232_2 PD",
            "1232_3 R"
        ])
        self.assertEqual(dict_unexpected_states, {})

        self.set_states_on_disc({
            "t2": "state.failed.0"
        })

        dict_unexpected_states = self.parent_task.mock_compare_and_reconcile_squeue_with_state_files([
            "1232_0 R",
            "1232_2 R",
            "1232_3 R"
        ])
        self.assertEqual(dict_unexpected_states, {})

        self.set_states_on_disc({
            "t1": "state.completed",
            "t2": "state.failed.0",
            "t3": "state.completed",
            "t4": "state.timed-out.0"
        })

        dict_unexpected_states = self.parent_task.mock_compare_and_reconcile_squeue_with_state_files([])
        self.assertEqual(dict_unexpected_states, {})

        # should launch nothing
        size_of_array_launched = self.parent_task.prepare_and_launch_next_array()
        self.assertEqual(size_of_array_launched, 0)


        size_of_array_launched = self.parent_task.prepare_and_launch_next_array(
            restart_failed=True,
            call_sbatch_mockup=lambda: 5345
        )
        self.assertEqual(size_of_array_launched, 2)

        self.assertTrue(
            os.path.exists(self.parent_task.i_th_array_file(1))
        )

        self.assert_task_keys_in_array_file(1, {"t2","t4"})

        self.assertTrue(
            os.path.exists(self.parent_task.i_th_submitted_array_file(1, 5345))
        )

        self.assert_task_state_file_states({
            "t1": "state.completed",
            "t2": "state._step-started",
            "t3": "state.completed",
            "t4": "state._step-started"
        })

        dict_unexpected_states = self.parent_task.mock_compare_and_reconcile_squeue_with_state_files([
            "5345_0 R",
            "5345_1 R"
        ])
        self.assertEqual(dict_unexpected_states, {})



class SlurmArrayCrashScenario(BaseSlurmArrayScenarioWithSlurmMockup):

    def get_slurm_array_task_keys(self):
        return "parent_task", ["t1", "t2"]

    def get_task_conf(self):
        return TaskConf.default()

    def test(self):

        size_of_array_launched = self.parent_task.prepare_and_launch_next_array(
            call_sbatch_mockup=lambda: 1232
        )

        submitted_arrays_files_with_job_status = list(
            self.parent_task.submitted_arrays_files_with_job_is_running_status()
        )

        for array_n, job_id, file, is_terminated in submitted_arrays_files_with_job_status:
            self.assertEqual(array_n, 0)
            self.assertEqual(job_id, "1232")
            self.assertFalse(is_terminated)

        self.assertEqual(len(submitted_arrays_files_with_job_status), 1)

        self.assertEqual(2, size_of_array_launched)

        self.assert_task_state_file_states({
            "t1": "state._step-started",
            "t2": "state._step-started"
        })

        dict_unexpected_states = self.parent_task.mock_compare_and_reconcile_squeue_with_state_files([])
        self.assertEqual(dict_unexpected_states, {
            't1': ('_step-started', 'R,PD', None),
            't2': ('_step-started', 'R,PD', None)
        })

        self.assert_task_state_file_states({
            "t1": "state.crashed",
            "t2": "state.crashed"
        })



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


def all_low_level_tests_with_mockup_slurm():
    return [
        SlurmArrayHandingStateMachineTest,
        SlurmArrayNormalScenario1,
        SlurmArrayCrashScenario,
        PipelineWithSlurmArrayWithUntil
    ]