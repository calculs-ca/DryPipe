import unittest

from dry_pipe.monitoring import TaskGroupMetric, PipelineMetricsTable


class MonitoringTests(unittest.TestCase):

    def test_counters(self):

        g = TaskGroupMetric("k", 3)

        g.inc("a")
        g.inc("a")
        g.inc("b")

        self.assertEqual([2, 1], g.counts([["a"], ["b"]]))

        self.assertEqual([2, 1, 0], g.counts([["a"], ["b"], ["c"]]))

        self.assertEqual([0, 3, 0], g.counts([["x"], ["a", "b"], ["c"]]))

    def test_metrics_gen(self):

        gA = TaskGroupMetric("A", 1)
        gB = TaskGroupMetric("B", 2)
        gC = TaskGroupMetric("A", 4)

        gA.inc("s1")

        gB.inc("s2")
        gB.inc("s2")

        gC.inc("s3")
        gC.inc("s3")
        gC.inc("s3")

        header, body, footer = PipelineMetricsTable._table_from_task_group_metrics(
            ["s1", "s2", "s3"],
            [["s1"], ["s2"], ["s3"]],
            [gA, gB, gC]
        )

        self.assertEqual(
            [['A', 1, 0, 0, 1],
             ['B', 0, 2, 0, 2],
             ['A', 0, 0, 3, 3]],
            body
        )

        self.assertEqual(
            [None, 1, 2, 3, 6],
            footer
        )

        self.assertEqual(
            ['task group', 's1', 's2', 's3', 'total'],
            header
        )



