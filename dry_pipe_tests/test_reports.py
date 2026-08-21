import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from dry_pipe.reports import parse_timers_in_log, timers_for_tasks


def log_line(hh_mm_ss, message):
    return f"2026-08-20 {hh_mm_ss}-0400 - INFO - {message}\n"


class ParseTimersInLogTests(unittest.TestCase):

    def parse(self, *lines):
        with TemporaryDirectory() as d:
            log = Path(d, "drypipe.log")
            with open(log, "w") as f:
                f.writelines(lines)
            return list(parse_timers_in_log(log))

    def test_timers_are_read_from_the_log(self):
        self.assertEqual(
            self.parse(
                log_line("10:00:00", "START_TIMER_FOR:TASK"),
                log_line("10:00:00", "START_TIMER_FOR:STEP-0"),
                log_line("10:02:03", "TIME_ELAPSED_FOR:STEP-0: 00:02:03, 123.45"),
                log_line("10:02:03", "TIME_ELAPSED_FOR:TASK: 00:02:03, 123.46"),
            ),
            [
                ("STEP-0", "00:02:03", "123.45"),
                ("TASK", "00:02:03", "123.46")
            ]
        )

    def test_a_timed_out_task_is_read_like_any_other(self):
        """
        the signal handlers log TIME_ELAPSED_FOR before dying, see
        TaskProcess._log_elapsed_for_active_time_loggers, so there is nothing special to do here
        """
        self.assertEqual(
            self.parse(
                log_line("10:00:00", "START_TIMER_FOR:TASK"),
                log_line("10:00:00", "START_TIMER_FOR:STEP-0"),
                log_line("12:00:30", "time out signal recieved"),
                log_line("12:00:30", "TIME_ELAPSED_FOR:STEP-0: 02:00:30, 7230.0"),
                log_line("12:00:30", "TIME_ELAPSED_FOR:TASK: 02:00:30, 7230.0"),
                log_line("12:00:30", "will transition to: state.timed-out.0"),
            ),
            [
                ("STEP-0", "02:00:30", "7230.0"),
                ("TASK", "02:00:30", "7230.0")
            ]
        )

    def test_restarts_append_to_the_same_log(self):
        """
        drypipe.log is appended to by every run of a task, each run's timers are reported
        separately, and the wait in between (queue time before the relaunch, often days) is
        nowhere in the report, since each run logs its own elapsed time
        """
        self.assertEqual(
            self.parse(
                "2026-08-17 09:00:00-0400 - INFO - START_TIMER_FOR:TASK\n",
                "2026-08-17 12:00:00-0400 - INFO - TIME_ELAPSED_FOR:TASK: 03:00:00, 10800.0\n",
                "2026-08-17 12:00:00-0400 - INFO - will transition to: state.timed-out.0\n",
                # 3 days later, the task gets its turn in the queue again
                "2026-08-20 09:48:34-0400 - INFO - START_TIMER_FOR:TASK\n",
                "2026-08-20 11:48:34-0400 - INFO - TIME_ELAPSED_FOR:TASK: 02:00:00, 7200.0\n",
            ),
            [
                ("TASK", "03:00:00", "10800.0"),
                ("TASK", "02:00:00", "7200.0")
            ]
        )


class TimersForTasksTests(unittest.TestCase):

    def write_log(self, d, task_key, *lines):
        control_dir = Path(d, ".drypipe", task_key)
        control_dir.mkdir(parents=True)
        with open(Path(control_dir, "drypipe.log"), "w") as f:
            f.writelines(lines)

    def test_tasks_without_a_log_are_skipped(self):
        """
        a selected task that was never launched has no drypipe.log, it must not blow up the
        report, nor hide the tasks that do have one
        """
        with TemporaryDirectory() as d:
            self.write_log(d, "t1", log_line("10:00:00", "TIME_ELAPSED_FOR:TASK: 00:00:10, 10.0"))

            self.assertEqual(
                list(timers_for_tasks(d, ["never-launched", "t1", "also-never-launched"])),
                [("t1", "TASK", "00:00:10", "10.0")]
            )

    def test_steps_are_excluded_unless_asked_for(self):
        with TemporaryDirectory() as d:
            self.write_log(
                d, "t1",
                log_line("10:00:10", "TIME_ELAPSED_FOR:STEP-0: 00:00:04, 4.0"),
                log_line("10:00:10", "TIME_ELAPSED_FOR:TASK: 00:00:10, 10.0"),
            )

            self.assertEqual(
                list(timers_for_tasks(d, ["t1"])),
                [("t1", "TASK", "00:00:10", "10.0")]
            )

            self.assertEqual(
                list(timers_for_tasks(d, ["t1"], include_steps=True)),
                [
                    ("t1", "STEP-0", "00:00:04", "4.0"),
                    ("t1", "TASK", "00:00:10", "10.0")
                ]
            )


all_tests = [
    ParseTimersInLogTests,
    TimersForTasksTests
]


if __name__ == '__main__':
    unittest.main()
