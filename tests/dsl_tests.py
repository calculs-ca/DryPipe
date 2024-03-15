import unittest


from dry_pipe import TaskBuilder, TaskConf
from dry_pipe.state_file_tracker import StateFileTracker
from dry_pipe.state_machine import InvalidTaskDefinition
from tests.test_state_machine import StateMachineTester
from tests.test_utils import TestSandboxDir


class DslErrorsTests(unittest.TestCase):

    def test_bad_task_definitions(self):

        d = TestSandboxDir(self)

        def dag_gen(dsl):

            yield dsl.task(key="t1").calls("""
                #!/usr/bin/bash
                echo "z"
            """)# missing ()

        tester = StateMachineTester(self, dag_gen, StateFileTracker(d.sandbox_dir))

        self.assertRaises(
            InvalidTaskDefinition,
            lambda: tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        )




class TaskChangeTrackingTests(unittest.TestCase):


    def test_task_digest(self):

        def snippet(s):
            return f"""
            #!/usr/bin/bash
            echo {s}
            """

        task_conf = TaskConf.default()

        t1 = TaskBuilder("t1", task_conf=task_conf).inputs(x=234).outputs(z=int).calls(snippet("a"))()
        t2 = TaskBuilder("t1", task_conf=task_conf).inputs(x=234).outputs(z=int).calls(snippet("a"))()

        self.assertEqual(t1.compute_hash_code(), t2.compute_hash_code())

        self.assertNotEqual(
            t2.compute_hash_code(),
            TaskBuilder(
                "t1",
                task_conf=task_conf
            ).inputs(x=234).outputs(z=int).calls(snippet("a1"))().compute_hash_code()
        )

        self.assertNotEqual(
            t2.compute_hash_code(),
            TaskBuilder(
                "t1",
                task_conf=task_conf
            ).inputs(x=234).outputs(z=float).calls(snippet("a"))().compute_hash_code()
        )

        self.assertNotEqual(
            t2.compute_hash_code(),
            TaskBuilder(
                "t1",
                task_conf=task_conf
            ).inputs(x=234).outputs(z=int,c=str).calls(snippet("a"))().compute_hash_code()
        )
