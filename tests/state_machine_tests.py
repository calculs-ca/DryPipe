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
        self.state_files: dict[str, StateFile] = {}
        self.pending_changes: dict[str, str] = {}

    def set_completed(self, task_key):
        self.pending_changes[task_key] = "state.completed"

    def state_file_in_memory(self, task_key):
        return self.state_files[task_key]

    def next_state_file_if_changed(self, task_key, last_state_file):
        next_state = self.pending_changes.pop(task_key, None)
        if next_state is not None:
            s = self.state_files[task_key]
            s.update_in_memory(f"/{task_key}/{next_state}")
            return s
        else:
            return None

    def get_state_file_and_save_if_required(self, task):
        """
        :param task:
        :return: (task_is_new, state_file)
        """
        s = self.state_files.get(task.key)
        if s is None:
            s = StateFile("/", task.key, task.hash_code(), self)
            self.state_files[task.key] = s
            return True, s
        else:
            return False, s


class JanitorTester:

    def __init__(self, test_case, dag_gen):
        self.test_case = test_case
        self.state_file_tracker = StateFileTrackerMockup()
        self.janitor = StateMachine(self.state_file_tracker)
        self.dag_gen = dag_gen

    def set_completed(self, task_key):
        self.state_file_tracker.set_completed(task_key)

    def new_iterator(self):
        return iter(self.janitor.iterate_tasks_to_launch(self.dag_gen))

    def asser_end_of_iterator(self, i):
        self.test_case.assertRaises(StopIteration, lambda: next(i))

    def assert_no_new_task_to_launch(self):
        i = iter(self.janitor.iterate_tasks_to_launch(self.dag_gen))
        self.test_case.assertRaises(StopIteration, lambda: next(i))

    def assert_no_new_task_to_launch(self, *task_keys):
        keys_to_launch = {
            state_file.task_key
            for state_file in self.janitor.iterate_tasks_to_launch(self.dag_gen)
        }
        self.test_case.assertEqual(keys_to_launch, set(task_keys))

    def assert_wait_relationships(self, task_key, set_of_wait_relationships):
        self.test_case.assertEqual(
            self.janitor.set_of_wait_relationships_of(task_key),
            set_of_wait_relationships
        )

    def assert_next_tasks_to_launch_are(self, *task_keys):
        tasks_to_launch = list(self.janitor.iterate_tasks_to_launch(self.dag_gen))
        for t in tasks_to_launch:
            t.path.endswith("state.waiting")
        self.test_case.assertEqual(
            {task_state.task_key for task_state in tasks_to_launch},
            set(task_keys)
        )

    def assert_completed_task_keys(self, *task_keys):
        self.test_case.assertEqual(self.janitor.set_of_completed_task_keys(), set(task_keys))


class StateMachineTests(unittest.TestCase):


    def test_state_machine_01(self):

        t1 = TaskMockup("t1")
        t2 = TaskMockup("t2", ["t1"])
        t3 = TaskMockup("t3", ["t2"])

        def dag_gen():
            yield from [t1, t2, t3]

        tester = JanitorTester(self, dag_gen)

        i = tester.new_iterator()
        ts1 = next(i)
        self.assertEqual(ts1.task_key, "t1")
        tester.asser_end_of_iterator(i)
        tester.assert_wait_relationships(t2.key, {"t1->waiting"})
        tester.assert_wait_relationships(t3.key, {"t2->waiting"})
        tester.assert_no_new_task_to_launch()
        tester.assert_completed_task_keys(*[])

        tester.set_completed(t1.key)

        i = tester.new_iterator()
        ts2 = next(i)
        self.assertEqual(ts2.task_key, "t2")
        tester.asser_end_of_iterator(i)
        tester.assert_completed_task_keys("t1")
        tester.assert_wait_relationships(t2.key, set())
        tester.assert_wait_relationships(t3.key, {"t2->waiting"})
        tester.assert_no_new_task_to_launch()
        tester.assert_no_new_task_to_launch()

        tester.set_completed(t2.key)

        tester.assert_next_tasks_to_launch_are("t3")
        tester.assert_completed_task_keys("t1", "t2")

        tester.set_completed(t3.key)

        tester.assert_no_new_task_to_launch()
        tester.asser_end_of_iterator(i)
        tester.assert_completed_task_keys("t1", "t2", "t3")
        tester.assert_wait_relationships(t1.key, set())
        tester.assert_wait_relationships(t2.key, set())
        tester.assert_wait_relationships(t3.key, set())


    def test_state_machine_02(self):

        t1 = TaskMockup("t1")
        t2 = TaskMockup("t2", ["t1"])
        t3 = TaskMockup("t3", ["t1", "t2"])
        t4 = TaskMockup("t4", ["t3"])
        t5 = TaskMockup("t5")

        def dag_gen():
            yield from [t1, t2, t3, t4, t5]

        tester = JanitorTester(self, dag_gen)

        i = tester.new_iterator()
        ts1 = next(i)
        self.assertEqual(ts1.task_key, "t1")
        ts5 = next(i)
        self.assertEqual(ts5.task_key, "t5")
        tester.asser_end_of_iterator(i)
        tester.assert_wait_relationships(t2.key, {"t1->waiting"})
        tester.assert_wait_relationships(t3.key, {"t1->waiting", "t2->waiting"})
        tester.assert_wait_relationships(t4.key, {"t3->waiting"})
        tester.assert_no_new_task_to_launch()
        tester.assert_completed_task_keys(*[])

        tester.set_completed("t1")
        tester.assert_no_new_task_to_launch("t2")
