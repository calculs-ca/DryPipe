import unittest
import pipeline_with_dynamic_dag
from test_utils import TestSandboxDir


class AggregateTaskTests(unittest.TestCase):

    def test_pipeline_with_dynamic_dag(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_dynamic_dag.pipeline_task_generator, completed=True
        )

        agg_task = pipeline_instance.tasks["aggregate_all"]

        self.assertEqual(agg_task.out.aggregate_inflated_number.fetch(), 20)

        self.assertEqual(
            {s for s in agg_task.out.insane_strings_passed_through.fetch().split(",")},
            {s for s in "abc1,abc2,abc4,abc3".split(",")}
        )
