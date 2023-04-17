import os
import shutil
import unittest
from pathlib import Path

from dry_pipe.state_machine import StateMachine, StateFileTracker, AllRunnableTasksCompletedOrInError, \
    InvalidQueryInTaskGenerator
from mockups import TaskMockup, StateFileTrackerMockup
from test_utils import TestSandboxDir

class Counter:

    def __init__(self):
        self.value = 0

    def inc(self):
        self.value += 1


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
        return self.list_of_ready_state_files

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
                self.state_machine._keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys.get(waiting_task_key)

            if computed_waiting_task_keys is None:
                computed_waiting_task_keys = set()

            if upstream_task_keys != computed_waiting_task_keys:
                raise Exception(
                    f"expected {waiting_task_key} to wait for {upstream_task_keys}, "+
                    f"instead computed {computed_waiting_task_keys}"
                )

    def assert_completed_task_keys(self, *task_keys):
        self.test_case.assertEqual(self.state_machine.set_of_completed_task_keys(), set(task_keys))

    def assert_task_keys_to_states(self, task_keys_to_states):
        self.state_file_tracker.all_tasks()

    @staticmethod
    def create_scenario(test_case, dependency_graph, state_file_tracker=None, save_dag_and_restart=False):

        def dag_gen(dsl):
            for task_key, upstream_task_keys in dependency_graph.items():
                yield TaskMockup(task_key, upstream_task_keys)

        if not save_dag_and_restart:
            return [StateMachineTester(test_case, dag_gen, state_file_tracker)] + list(dag_gen(None))
        else:
            if isinstance(state_file_tracker, StateFileTrackerMockup):
                raise Exception(f"can't have save_dag_and_restart with mockup tracker")

            tester = StateMachineTester(test_case, dag_gen, state_file_tracker)
            tester.iterate_once_and_mutate_set_of_next_state_files_ready()

            tester = StateMachineTester(test_case, None, state_file_tracker)
            tester.state_machine.prepare_for_run_without_generator()
            return [tester] + list(dag_gen(None))


class BaseStateFileTrackerTester(unittest.TestCase):

    def state_file_tracker(self):
        raise NotImplementedError()

    def test_task_state_tracking(self):
        t1, t2, t3 = [
            TaskMockup(f"t{i}")
            for i in [1, 2, 3]
        ]

        state_file_tracker = self.state_file_tracker()

        is_sf1_new, sf1_a = state_file_tracker.create_true_state_if_new_else_fetch_from_memory(t1)

        self.assertIsNotNone(state_file_tracker.lookup_state_file_from_memory(t1.key))

        self.assertTrue(is_sf1_new)

        is_sf1_new, sf1_b = state_file_tracker.create_true_state_if_new_else_fetch_from_memory(t1)
        self.assertFalse(is_sf1_new)

        self.assertEqual(sf1_a.path, sf1_b.path)

        state_file_tracker.set_completed_on_disk("t1")

        true_state_file, _ = state_file_tracker.fetch_true_state_and_update_memory_if_changed("t1")

        self.assertEqual("/t1/state.completed", str(true_state_file))

        # ensure only fetched once:
        true_state_file, _ = state_file_tracker.fetch_true_state_and_update_memory_if_changed("t1")
        self.assertIsNone(true_state_file)


class MockupStateFileTrackerTest(BaseStateFileTrackerTester):
    def state_file_tracker(self):
        return StateFileTrackerMockup()

class StateFileTrackerTest(BaseStateFileTrackerTester):

    def setUp(self) -> None:
        all_sandbox_dirs = os.path.join(
            os.path.dirname(__file__),
            "sandboxes"
        )

        self.pipeline_instance_dir = os.path.join(all_sandbox_dirs, self.__class__.__name__)

        d = Path(self.pipeline_instance_dir)
        if d.exists():
            shutil.rmtree(d)

    def state_file_tracker(self):
        return StateFileTracker(self.pipeline_instance_dir)


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

        self.assertRaises(
            AllRunnableTasksCompletedOrInError,
            lambda: tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        )

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

        self.assertRaises(
            AllRunnableTasksCompletedOrInError,
            lambda: tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        )

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


    def test_query_all_completed_01(self):

        def dag_gen(dsl):
            yield TaskMockup("t1")
            yield TaskMockup("t2")
            for _ in dsl.query_all_or_nothing("t*"):
                yield TaskMockup("A1")

        d = TestSandboxDir(self)
        state_file_tracker = StateFileTracker(d.sandbox_dir)

        tester = StateMachineTester(self, dag_gen, state_file_tracker)

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready("t1", "t2")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        tester.assert_set_of_next_tasks_ready(*[])
        tester.assert_wait_graph({})

        state_file_tracker.set_completed_on_disk("t1")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        tester.assert_set_of_next_tasks_ready(*[])
        tester.assert_wait_graph({})

        state_file_tracker.set_completed_on_disk("t2")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready("A1")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready(*[])

        state_file_tracker.set_completed_on_disk("A1")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        self.assertTrue(tester.state_machine._no_runnable_tasks_left)

        self.assertRaises(
            AllRunnableTasksCompletedOrInError,
            lambda: tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        )


    def test_invalid_dag_with_query_all_completed(self):

        counter = Counter()

        def invalid_dag_gen(dsl):
            yield TaskMockup("t1")
            if counter.value > 1:
                yield TaskMockup("t2")
            for _ in dsl.query_all_or_nothing("t*"):
                yield TaskMockup("A")
            counter.inc()


        d = TestSandboxDir(self)
        state_file_tracker = StateFileTracker(d.sandbox_dir)

        tester = StateMachineTester(self, invalid_dag_gen, state_file_tracker)

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready("t1")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        self.assertRaises(
            InvalidQueryInTaskGenerator,
            lambda: tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        )

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

    def test_invalid_constructors(self):

        self.assertRaises(Exception, lambda : StateMachine(None))
        sm = StateMachine(StateFileTrackerMockup())
        self.assertRaises(Exception, lambda: list(sm.iterate_tasks_to_launch()))