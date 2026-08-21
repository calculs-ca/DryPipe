import threading
from pathlib import Path

from dry_pipe.task_process import TaskProcess
from dry_pipe_tests.base_pipeline_test import BasePipelineTest


def elapsed_labels(logged):
    return [
        line.split("TIME_ELAPSED_FOR:")[1].split(":")[0]
        for line in logged if "TIME_ELAPSED_FOR:" in line
    ]


class TaskTimersTest(BasePipelineTest):
    """
    Timers must survive a task being timed out or killed.

    The task runs in a worker thread (TaskProcess.launch_task), signals are delivered to the
    main thread, and the handlers end the process with os._exit(): a handler can neither unwind
    the "with" blocks the timers live in, nor rely on any cleanup on the way out. The two timers
    a task runs are therefore held on the TaskProcess, and closed explicitly by the handlers.
    """

    def dag_gen(self, dsl):
        yield dsl.task(
            key="t1"
        ).calls(
            """
            #!/usr/bin/bash
            echo hello
            """
        )()

    def task_process(self):
        return TaskProcess(
            str(Path(self.pipeline_instance_dir, ".drypipe", "t1")), no_logger=True
        )

    def validate(self, tasks_by_keys):
        self.check_timers_are_closed_from_another_thread()
        self.check_an_ended_timer_is_not_logged_twice()

    def check_timers_are_closed_from_another_thread(self):

        task_process = self.task_process()
        logged = []

        inside_the_timers = threading.Event()
        let_the_task_end = threading.Event()

        def task_func_wrapper():
            task_process.task_timer = task_process.create_time_logger("TASK", logged.append)
            with task_process.task_timer:
                task_process.step_timer = task_process.create_time_logger("STEP-0", logged.append)
                with task_process.step_timer:
                    inside_the_timers.set()
                    let_the_task_end.wait(timeout=30)

        worker = threading.Thread(target=task_func_wrapper)
        worker.start()
        self.assertTrue(inside_the_timers.wait(timeout=30))

        # this is what the SIGUSR1/SIGTERM handlers do, from the main thread, while the worker
        # sits inside both "with" blocks
        task_process._log_elapsed_for_active_time_loggers()

        # the step timer first, the order they would have unwound in
        self.assertEqual(elapsed_labels(logged), ["STEP-0", "TASK"])

        # the worker leaving its "with" blocks must not log them a second time. os._exit() means
        # it never gets there in production, but a signal can land anywhere, so closing a timer
        # is idempotent
        let_the_task_end.set()
        worker.join(timeout=30)
        self.assertFalse(worker.is_alive())
        self.assertEqual(elapsed_labels(logged), ["STEP-0", "TASK"])

    def check_an_ended_timer_is_not_logged_twice(self):
        """
        step_timer holds the step that just ended while the task moves to the next one, a signal
        landing in between must not report it again
        """
        task_process = self.task_process()
        logged = []

        task_process.task_timer = task_process.create_time_logger("TASK", logged.append)
        with task_process.task_timer:
            task_process.step_timer = task_process.create_time_logger("STEP-0", logged.append)
            with task_process.step_timer:
                pass

            self.assertEqual(elapsed_labels(logged), ["STEP-0"])

            # in between two steps: the task timer is still running, the step timer is not
            task_process._log_elapsed_for_active_time_loggers()

        self.assertEqual(elapsed_labels(logged), ["STEP-0", "TASK"])


all_tests = [
    TaskTimersTest
]
