import unittest
from datetime import datetime

import test_helpers as th
from dry_pipe.monitoring import fetch_task_groups_stats
from dry_pipe.task_state import TaskState
from test_regressions_pipelines.regression_pipeline_defs import pipeline_consuming_upstream_vars_with_same_name, \
    correct_timestamps_in_task_history
from test_utils import TestSandboxDir


class RegressionTests(unittest.TestCase):

    def test_pipeline_consuming_upstream_vars_with_same_name(self):

        d = TestSandboxDir(self)

        pipeline = d.pipeline_instance_from_generator(
            pipeline_consuming_upstream_vars_with_same_name, completed=True
        )

        self.assertEqual(
            th.count(pipeline, lambda task: task.has_completed()),
            3
        )


    def test_correct_timestamps_in_task_history(self):

        d = TestSandboxDir(self)

        pipeline_for_correct_timestamps_in_task_history = \
            d.pipeline_instance_from_generator(correct_timestamps_in_task_history, completed=True)

        sleep1, sleep2_3 = pipeline_for_correct_timestamps_in_task_history.tasks

        v1 = dict(sleep1.iterate_out_vars())
        v2_3 = dict(sleep2_3.iterate_out_vars())

        def p(dt):
            return datetime.strptime(dt[1:-1], '%Y-%m-%dT%H:%M:%S.%f')

        start1 = p(v1["start_t_1"])
        end1 = p(v1["end_t_1"])

        start2 = p(v2_3["start_t_2"])
        end2 = p(v2_3["end_t_2"])

        start3 = p(v2_3["start_t_3"])
        end3 = p(v2_3["end_t_3"])

        def filter_step_start_step_complete(task):
            for row in task.get_state().load_history_rows():
                state_name = row[0]
                timestamp = row[1]
                if state_name in ["step-started", "step-completed"]:
                    yield TaskState.parse_history_timestamp(timestamp)

        h_start1, h_end1 = list(filter_step_start_step_complete(sleep1))

        h_start2, h_end2, h_start3, h_end3 = list(filter_step_start_step_complete(sleep2_3))

        def f(d):
            return datetime.strftime(d, '%S.%f')

        self.assertTrue(
            h_start1 != h_end1,
            f"expected {f(h_end1)} to be at least 1 second later than {f(h_start1)}"
        )

        self.assertTrue(
            start1 != h_end1,
            f"expected {f(h_end1)} to be at least 1 second later than {f(h_start1)}"
        )

        self.assertTrue(
            end1 <= h_end1,
            f"expected {f(end1)} to be earlier or equal to {f(h_end1)}"
        )

        self.assertTrue(
            h_end1 < h_start2,
            f"expected {f(h_end1)} to be at least 1 second later than {f(h_start1)}"
        )

        def ensure1sec(d1, d2):
            self.assertTrue(
                (d2 - d1).seconds >= 1,
                f"expected on second between {f(d1)} and {f(d2)}"
            )

        def sanity_check(h_start, start, end, h_end):
            ensure1sec(h_start, end)
            ensure1sec(start, h_end)

        sanity_check(h_start1, start1, end1, h_end1)
        sanity_check(h_start2, start2, end2, h_end2)
        sanity_check(h_start3, start3, end3, h_end3)

        expected_timestamp_ordering = [
            h_start1, start1, end1, h_end1,
            h_start2, start2, end2, h_end2,
            h_start3, start3, end3, h_end3
        ]

        self.assertEqual(
            expected_timestamp_ordering,
            list(sorted(expected_timestamp_ordering))
        )

    def test_time_stats(self):

        d = TestSandboxDir(self)

        pipeline_for_correct_timestamps_in_task_history = \
            d.pipeline_instance_from_generator(correct_timestamps_in_task_history, completed=True)


        stats = list(
            fetch_task_groups_stats(
                pipeline_for_correct_timestamps_in_task_history.pipeline_instance_dir, no_header=True
            )
        )

        self.assertEqual(len(stats), 3)

        for step_name, min_t, max_t, total_t, avg_t, s_dev in stats:
            self.assertEqual(min_t, max_t)
            self.assertTrue(avg_t >= 1)
