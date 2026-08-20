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

    def test_completed_timers_are_reported_as_logged(self):
        """
        when a task ends on its own terms, the times come from TIME_ELAPSED_FOR, which the
        TimeLogger measured with the wall clock, NOT from the log's timestamps
        """
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

    def test_timer_never_ended_is_reported_as_partial(self):
        """
        a timed-out (or killed, or crashed) task never logs TIME_ELAPSED_FOR, since the signal
        handlers exit with os._exit(). The time it ran is derived from the log's timestamps.
        """
        self.assertEqual(
            self.parse(
                log_line("10:00:00", "START_TIMER_FOR:TASK"),
                log_line("10:00:00", "START_TIMER_FOR:STEP-0"),
                log_line("12:00:30", "time out signal recieved"),
            ),
            [
                ("TASK-PARTIAL", "02:00:30", "7230.0"),
                ("STEP-0-PARTIAL", "02:00:30", "7230.0")
            ]
        )

    def test_partial_and_completed_timers_mixed(self):
        """a task that timed out at step 1, after step 0 had completed"""
        self.assertEqual(
            self.parse(
                log_line("10:00:00", "START_TIMER_FOR:TASK"),
                log_line("10:00:00", "START_TIMER_FOR:STEP-0"),
                log_line("10:00:10", "TIME_ELAPSED_FOR:STEP-0: 00:00:10, 10.0"),
                log_line("10:00:10", "START_TIMER_FOR:STEP-1"),
                log_line("11:00:10", "time out signal recieved"),
            ),
            [
                ("STEP-0", "00:00:10", "10.0"),
                ("TASK-PARTIAL", "01:00:10", "3610.0"),
                ("STEP-1-PARTIAL", "01:00:00", "3600.0")
            ]
        )

    def test_restart_appends_to_the_same_log(self):
        """
        drypipe.log is appended to across restarts, so a second START_TIMER_FOR for a label
        means the previous run of it never ended
        """
        self.assertEqual(
            self.parse(
                log_line("10:00:00", "START_TIMER_FOR:TASK"),
                log_line("11:00:00", "time out signal recieved"),
                log_line("12:00:00", "START_TIMER_FOR:TASK"),
                log_line("12:00:05", "TIME_ELAPSED_FOR:TASK: 00:00:05, 5.0"),
            ),
            [
                ("TASK-PARTIAL", "02:00:00", "7200.0"),
                ("TASK", "00:00:05", "5.0")
            ]
        )

    def test_lines_without_timestamps_dont_end_the_timer(self):
        """multi line messages (stack traces, etc) have continuation lines with no timestamp"""
        self.assertEqual(
            self.parse(
                log_line("10:00:00", "START_TIMER_FOR:TASK"),
                log_line("10:00:30", "Traceback (most recent call last):"),
                "  File \"x.py\", line 1, in <module>\n",
            ),
            [
                ("TASK-PARTIAL", "00:00:30", "30.0")
            ]
        )


class TimersForTasksTests(unittest.TestCase):

    def test_tasks_without_a_log_are_skipped(self):
        """
        a selected task that was never launched has no drypipe.log, it must not blow up the
        report, nor hide the tasks that do have one
        """
        with TemporaryDirectory() as d:
            control_dir = Path(d, ".drypipe", "t1")
            control_dir.mkdir(parents=True)
            with open(Path(control_dir, "drypipe.log"), "w") as f:
                f.write(log_line("10:00:00", "TIME_ELAPSED_FOR:TASK: 00:00:10, 10.0"))

            self.assertEqual(
                list(timers_for_tasks(d, ["never-launched", "t1", "also-never-launched"])),
                [("t1", "TASK", "00:00:10", "10.0")]
            )

    def test_steps_are_excluded_unless_asked_for(self):
        with TemporaryDirectory() as d:
            control_dir = Path(d, ".drypipe", "t1")
            control_dir.mkdir(parents=True)
            with open(Path(control_dir, "drypipe.log"), "w") as f:
                f.writelines([
                    log_line("10:00:00", "START_TIMER_FOR:TASK"),
                    log_line("10:00:00", "START_TIMER_FOR:STEP-0"),
                    log_line("10:00:10", "time out signal recieved"),
                ])

            # both the complete and the partial step labels are filtered out by default
            self.assertEqual(
                list(timers_for_tasks(d, ["t1"])),
                [("t1", "TASK-PARTIAL", "00:00:10", "10.0")]
            )

            self.assertEqual(
                list(timers_for_tasks(d, ["t1"], include_steps=True)),
                [
                    ("t1", "TASK-PARTIAL", "00:00:10", "10.0"),
                    ("t1", "STEP-0-PARTIAL", "00:00:10", "10.0")
                ]
            )


all_tests = [
    ParseTimersInLogTests,
    TimersForTasksTests
]


if __name__ == '__main__':
    unittest.main()
