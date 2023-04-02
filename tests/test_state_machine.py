import json
import os
import unittest
from pathlib import Path

from dry_pipe.state_machine import StateMachine, StateFile, StateFileTracker
from test_utils import TestSandboxDir


class TaskMockup:
    def __init__(self, key, upstream_dep_keys=[]):
        self.key = key
        self.upstream_dep_keys = upstream_dep_keys
        self.inputs = [1, 5, 4]

    def cause_change_of_hash_code(self, data):
        self.inputs.append(data)

    def compute_hash_code(self):
        return ",".join(str(i) for i in self.inputs)

    def save(self, pipeline_instance_dir, hash_code):
        task_control_dir = Path(pipeline_instance_dir, ".drypipe", self.key)
        task_control_dir.mkdir(exist_ok=True, parents=True)
        inputs = [
            {
                "upstream_task_key": k
            }
            for k in self.upstream_dep_keys
        ]
        with open(os.path.join(task_control_dir, "task-conf.json"), "w") as tc:
            tc.write(json.dumps({
                "hash_code": hash_code,
                "inputs": inputs
            }))


class StateFileTrackerMockup:

    def __init__(self):
        self.state_files_in_memory: dict[str, StateFile] = {}
        self.task_keys_to_task_states_on_mockup_disk: dict[str, str] = {}
        self.pipeline_work_dir = "/"

    def set_completed_on_disk(self, task_key):
        self.task_keys_to_task_states_on_mockup_disk[task_key] = "state.completed"

    def state_file_in_memory(self, task_key):
        return self.state_files_in_memory[task_key]

    def completed_task_keys(self):
        for k, state_file in self.state_files_in_memory.items():
            if state_file.is_completed():
                yield k

    def fetch_true_state_and_update_memory_if_changed(self, task_key):

        true_task_state = self.task_keys_to_task_states_on_mockup_disk.get(task_key)
        state_file_in_memory = self.state_files_in_memory.get(task_key)

        if state_file_in_memory is None:
            raise Exception(f"{task_key} should be in memory by now")

        if str(state_file_in_memory) == f"/{task_key}/{true_task_state}":
            return None
        else:
            state_file_in_memory.refresh(f"/{task_key}/{true_task_state}")
            return state_file_in_memory

    def create_true_state_if_new_else_fetch_from_memory(self, task):
        """
        :param task:
        :return: (task_is_new, state_file)
        """
        true_task_state = self.task_keys_to_task_states_on_mockup_disk.get(task.key)

        if true_task_state is None:
            s = StateFile(task.key, task.compute_hash_code(), self)
            self.task_keys_to_task_states_on_mockup_disk[task.key] = s.state_as_string()
            self.state_files_in_memory[task.key] = s
            return True, s
        else:
            return False, self.state_files_in_memory[task.key]


class StateMachineTester:

    def __init__(self, test_case, dag_gen, state_file_tracker=None):
        self.test_case = test_case
        if state_file_tracker is not None:
            self.state_file_tracker = state_file_tracker
        else:
            self.state_file_tracker = StateFileTrackerMockup()
        self.state_machine = StateMachine(self.state_file_tracker, dag_gen)
        self.dag_gen = dag_gen
        self.list_of_ready_state_files = []

    def set_completed_on_disk(self, task_key):
        self.state_file_tracker.set_completed_on_disk(task_key)

    def new_iterator(self):
        return iter(self.state_machine.iterate_tasks_to_launch())

    def iterate_once_and_mutate_set_of_next_state_files_ready(self):
        self.list_of_ready_state_files = list(self.state_machine.iterate_tasks_to_launch())

    def assert_set_of_next_tasks_ready(self, *expected_task_keys):
        expected_task_keys = set(expected_task_keys)
        computed_ready_task_keys = {sf.task_key for sf in self.list_of_ready_state_files}
        if expected_task_keys != computed_ready_task_keys:
            raise Exception(
                f"expected ready tasks {expected_task_keys}, computed {computed_ready_task_keys or '{}'}"
            )

    def asser_end_of_iterator(self, i):
        self.test_case.assertRaises(StopIteration, lambda: next(i))

    def assert_no_new_task_to_launch(self):
        c = len(self.list_of_ready_state_files)
        if c > 0:
            raise Exception(f"expected no new tasks to launch, got {c}")

    def assert_tasks_to_launch(self, *task_keys):
        keys_to_launch = {
            state_file.task_key
            for state_file in self.list_of_ready_state_files
        }
        self.test_case.assertEqual(keys_to_launch, set(task_keys))

    def assert_wait_graph(self, waiting_task_keys_to_upstream_task_keys):
        for waiting_task_key, upstream_task_keys in waiting_task_keys_to_upstream_task_keys.items():
            upstream_task_keys = set(upstream_task_keys)
            computed_waiting_task_keys = \
                self.state_machine.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys.get(waiting_task_key)

            if computed_waiting_task_keys is None:
                computed_waiting_task_keys = set()

            if upstream_task_keys != computed_waiting_task_keys:
                raise Exception(
                    f"expected {waiting_task_key} to wait for {upstream_task_keys}, "+
                    f"instead computed {computed_waiting_task_keys}"
                )

    def assert_completed_task_keys(self, *task_keys):
        self.test_case.assertEqual(self.state_machine.set_of_completed_task_keys(), set(task_keys))


    @staticmethod
    def create_scenario(test_case, dependency_graph, state_file_tracker=None, save_dag_and_restart=False):

        def dag_gen():
            for task_key, upstream_task_keys in dependency_graph.items():
                yield TaskMockup(task_key, upstream_task_keys)

        if not save_dag_and_restart:
            return [StateMachineTester(test_case, dag_gen, state_file_tracker)] + list(dag_gen())
        else:
            if isinstance(state_file_tracker, StateFileTrackerMockup):
                raise Exception(f"can't have save_dag_and_restart with mockup tracker")

            tester = StateMachineTester(test_case, dag_gen, state_file_tracker)
            tester.iterate_once_and_mutate_set_of_next_state_files_ready()

            tester = StateMachineTester(test_case, None, state_file_tracker)
            tester.state_machine.prepare_for_run_without_generator()
            return [tester] + list(dag_gen())


class StateFileTrackerTester:

    def __init__(self, test_case: unittest.TestCase, state_file_tracker):
        self.test_case = test_case
        self.state_file_tracker = state_file_tracker

    def test_01(self):
        t1, t2, t3 = [
            TaskMockup(f"t{i}")
            for i in [1, 2, 3]
        ]

        is_sf1_new, sf1_a = self.state_file_tracker.create_true_state_if_new_else_fetch_from_memory(t1)

        self.test_case.assertIsNotNone(self.state_file_tracker.state_file_in_memory(t1.key))

        self.test_case.assertTrue(is_sf1_new)

        is_sf1_new, sf1_b = self.state_file_tracker.create_true_state_if_new_else_fetch_from_memory(t1)
        self.test_case.assertFalse(is_sf1_new)

        self.test_case.assertEqual(sf1_a.path, sf1_b.path)

        self.state_file_tracker.set_completed_on_disk("t1")

        true_state_file = self.state_file_tracker.fetch_true_state_and_update_memory_if_changed("t1")

        self.test_case.assertEqual("/t1/state.completed", str(true_state_file))

        # ensure only fetched once:
        true_state_file = self.state_file_tracker.fetch_true_state_and_update_memory_if_changed("t1")
        self.test_case.assertIsNone(true_state_file)


class StateFileTrackerTests(unittest.TestCase):

    def test_mockup_state_file_tracker(self):
        tester = StateFileTrackerTester(self, StateFileTrackerMockup())
        tester.test_01()

    def test_real_state_file_tracker(self):
        d = TestSandboxDir(self)
        tester = StateFileTrackerTester(self, StateFileTracker(d.sandbox_dir))
        tester.test_01()

    def test_real_state_file_tracker_change_tracking(self):
        d = TestSandboxDir(self)
        tracker = StateFileTracker(d.sandbox_dir)

        t1 = TaskMockup("t1")

        is_new, state_file = tracker.create_true_state_if_new_else_fetch_from_memory(t1)

        self.assertEqual(tracker.new_save_count, 1)
        tracker.create_true_state_if_new_else_fetch_from_memory(t1)
        self.assertEqual(tracker.new_save_count, 1)

        state_file2 = tracker.load_from_existing_file_on_disc_and_resave_if_required(t1, state_file.path)

        self.assertEqual(tracker.load_from_disk_count, 1)
        self.assertEqual(state_file.path, state_file2.path)
        self.assertEqual(state_file.hash_code, state_file2.hash_code)

        tracker.load_from_existing_file_on_disc_and_resave_if_required(t1, state_file.path)
        self.assertEqual(tracker.load_from_disk_count, 2)
        self.assertEqual(tracker.resave_count, 0)

        t1.cause_change_of_hash_code("dfg")

        state_file3 = tracker.load_from_existing_file_on_disc_and_resave_if_required(t1, state_file.path)

        self.assertEqual(tracker.load_from_disk_count, 3)
        self.assertEqual(tracker.resave_count, 1)
        self.assertEqual(state_file.path, state_file3.path)
        self.assertNotEqual(state_file.hash_code, state_file3.hash_code)


class StateMachineTests(unittest.TestCase):

    def _test_state_machine_00(self, state_file_tracker, save_dag_and_restart=False):
        tester = StateMachineTester.create_scenario(self, {
            "t1": [],
            "t2": ["t1"],
            "t3": ["t2"]
        }, state_file_tracker, save_dag_and_restart)[0]

        # mutate
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        # verify
        tester.assert_set_of_next_tasks_ready("t1")

        # mutate
        tester.set_completed_on_disk("t1")
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        # verify
        tester.assert_set_of_next_tasks_ready("t2")

        # mutate
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        # verify
        tester.assert_set_of_next_tasks_ready(*[])

        # mutate
        tester.set_completed_on_disk("t2")
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        # verify
        tester.assert_set_of_next_tasks_ready("t3")

        # mutate
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        # verify
        tester.assert_set_of_next_tasks_ready(*[])

        # mutate
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        # verify
        tester.assert_set_of_next_tasks_ready(*[])

        # mutate
        tester.set_completed_on_disk("t3")
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        # verify
        tester.assert_set_of_next_tasks_ready(*[])


    def _test_state_machine_01(self, state_file_tracker, save_dag_and_restart=False):

        tester = StateMachineTester.create_scenario(self, {
            "t1": [],
            "t2": ["t1"],
            "t3": ["t2"]
        }, state_file_tracker, save_dag_and_restart)[0]

        # mutate
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        # verify
        tester.assert_wait_graph({
            "t2": ["t1"],
            "t3": ["t2"]
        })
        tester.assert_completed_task_keys(*[])

        # mutate
        tester.set_completed_on_disk("t1")
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        # verify
        tester.assert_completed_task_keys("t1")
        tester.assert_wait_graph({
            "t2": [],
            "t3": ["t2"]
        })

        # mutate
        tester.set_completed_on_disk("t2")
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        # verify
        tester.assert_tasks_to_launch("t3")
        tester.assert_completed_task_keys("t1", "t2")

        # mutate
        tester.set_completed_on_disk("t3")
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        # verify
        tester.assert_no_new_task_to_launch()
        tester.assert_completed_task_keys("t1", "t2", "t3")
        tester.assert_wait_graph({
            "t1": [],
            "t2": [],
            "t3": []
        })

    def _test_state_machine_02(self, state_file_tracker, save_dag_and_restart=False):

        tester = StateMachineTester.create_scenario(self, {
            "t1": [],
            "t2": ["t1"],
            "t3": ["t1", "t2"],
            "t4": ["t3"],
            "t5": []
        }, state_file_tracker, save_dag_and_restart)[0]

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_wait_graph({
            "t2": ["t1"],
            "t3": ["t1", "t2"],
            "t4": ["t3"]
        })

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_no_new_task_to_launch()
        tester.assert_completed_task_keys(*[])

        tester.set_completed_on_disk("t1")

    # state machine 00

    def test_state_machine_00_with_mockup_tracker(self):
        self._test_state_machine_00(StateFileTrackerMockup())

    def test_state_machine_00_with_real_tracker(self):
        d = TestSandboxDir(self)
        self._test_state_machine_00(StateFileTracker(d.sandbox_dir))

    def test_state_machine_00_with_real_tracker_and_restart(self):
        d = TestSandboxDir(self)
        self._test_state_machine_00(StateFileTracker(d.sandbox_dir), save_dag_and_restart=True)

    # state machine 01

    def test_state_machine_01_with_mockup_tracker(self):
        self._test_state_machine_01(StateFileTrackerMockup())

    def test_state_machine_01_with_real_tracker(self):
        d = TestSandboxDir(self)
        self._test_state_machine_01(StateFileTracker(d.sandbox_dir))

    def test_state_machine_01_with_real_tracker_with_restart(self):
        d = TestSandboxDir(self)
        self._test_state_machine_01(StateFileTracker(d.sandbox_dir), save_dag_and_restart=True)

    # state machine 02

    def test_state_machine_02_with_mockup_tracker(self):
        self._test_state_machine_02(StateFileTrackerMockup())

    def test_state_machine_02_with_real_tracker(self):
        d = TestSandboxDir(self)
        self._test_state_machine_02(StateFileTracker(d.sandbox_dir))

    def test_state_machine_02_with_real_tracker_with_restart(self):
        d = TestSandboxDir(self)
        self._test_state_machine_02(StateFileTracker(d.sandbox_dir), save_dag_and_restart=True)
