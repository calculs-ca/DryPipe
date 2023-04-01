import unittest

from dry_pipe.state_machine import StateMachine, StateFile


class TaskMockup:
    def __init__(self, key, upstream_dep_keys=[]):
        self.key = key
        self.upstream_dep_keys = upstream_dep_keys
        self._hash_code = "123"

    def hash_code(self):
        return self._hash_code

    def set_hash_code(self, v):
        self._hash_code = v


class StateFileTrackerMockup:

    def __init__(self):
        self.state_files_in_memory: dict[str, StateFile] = {}
        self.task_keys_to_task_states_on_mockup_disk: dict[str, str] = {}

    def set_completed_on_disk(self, task_key):
        self.task_keys_to_task_states_on_mockup_disk[task_key] = "state.completed"

    def state_file_in_memory(self, task_key):
        return self.state_files_in_memory[task_key]

    def fetch_true_state_and_update_memory_if_changed(self, task_key):

        true_task_state = self.task_keys_to_task_states_on_mockup_disk.get(task_key)
        state_file_in_memory = self.state_files_in_memory.get(task_key)

        if state_file_in_memory is None:
            raise Exception(f"{task_key} should be in memory by now")

        if str(state_file_in_memory) == f"/{task_key}/{true_task_state}":
            return None
        else:
            state_file_in_memory.update_in_memory(f"/{task_key}/{true_task_state}")
            return state_file_in_memory

    def create_true_state_if_new_else_fetch_from_memory(self, task):
        """
        :param task:
        :return: (task_is_new, state_file)
        """
        true_task_state = self.task_keys_to_task_states_on_mockup_disk.get(task.key)

        if true_task_state is None:
            s = StateFile("/", task.key, task.hash_code(), self)
            self.task_keys_to_task_states_on_mockup_disk[task.key] = s.state_as_string()
            self.state_files_in_memory[task.key] = s
            return True, s
        else:
            return False, self.state_files_in_memory[task.key]


class StateMachineTester:

    def __init__(self, test_case, dag_gen):
        self.test_case = test_case
        self.state_file_tracker = StateFileTrackerMockup()
        self.janitor = StateMachine(self.state_file_tracker)
        self.dag_gen = dag_gen
        self.list_of_ready_state_files = []

    def set_completed_on_disk(self, task_key):
        self.state_file_tracker.set_completed_on_disk(task_key)

    def new_iterator(self):
        return iter(self.janitor.iterate_tasks_to_launch(self.dag_gen))

    def iterate_once_and_mutate_set_of_next_state_files_ready(self):
        self.list_of_ready_state_files = list(self.janitor.iterate_tasks_to_launch(self.dag_gen))

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
                self.janitor.keys_of_waiting_tasks_to_set_of_incomplete_upstream_task_keys.get(waiting_task_key)

            if computed_waiting_task_keys is None:
                computed_waiting_task_keys = set()

            if upstream_task_keys != computed_waiting_task_keys:
                raise Exception(
                    f"expected {waiting_task_key} to wait for {upstream_task_keys}, "+
                    f"instead computed {computed_waiting_task_keys}"
                )

    def assert_completed_task_keys(self, *task_keys):
        self.test_case.assertEqual(self.janitor.set_of_completed_task_keys(), set(task_keys))


    @staticmethod
    def create_scenario(test_case, dependency_graph):

        def dag_gen():
            for task_key, upstream_task_keys in dependency_graph.items():
                yield TaskMockup(task_key, upstream_task_keys)

        return [StateMachineTester(test_case, dag_gen)] + list(dag_gen())

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
        self.test_case.assertTrue(is_sf1_new)

        is_sf1_new, sf1_b = self.state_file_tracker.create_true_state_if_new_else_fetch_from_memory(t1)
        self.test_case.assertFalse(is_sf1_new)

        self.test_case.assertEqual(sf1_a, sf1_b)

        self.state_file_tracker.set_completed_on_disk("t1")

        next_state_file = self.state_file_tracker.fetch_true_state_and_update_memory_if_changed("t1")

        self.test_case.assertEqual("/t1/state.completed", str(next_state_file))

        self.test_case.assertIsNone(self.state_file_tracker.fetch_true_state_and_update_memory_if_changed("t1"))


class StateMachineTests(unittest.TestCase):

    def test_state_mockup_file_tracker(self):
        tester = StateFileTrackerTester(self, StateFileTrackerMockup())
        tester.test_01()


    def test_state_machine_00(self):

        tester = StateMachineTester.create_scenario(self, {
            "t1": [],
            "t2": ["t1"],
            "t3": ["t2"]
        })[0]

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


    def test_state_machine_01(self):

        tester = StateMachineTester.create_scenario(self, {
            "t1": [],
            "t2": ["t1"],
            "t3": ["t2"]
        })[0]

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



    def test_state_machine_02(self):

        tester = StateMachineTester.create_scenario(self, {
            "t1": [],
            "t2": ["t1"],
            "t3": ["t1", "t2"],
            "t4": ["t3"],
            "t5": []
        })[0]

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
