import unittest


from dry_pipe import TaskBuilder, TaskConf


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
