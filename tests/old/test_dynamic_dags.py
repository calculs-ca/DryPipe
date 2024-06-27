import unittest

import pipeline_with_complex_task_groups
import pipeline_with_dynamic_dag
import pipeline_with_duck_typing
from test_utils import TestSandboxDir


class DynamicDagTests(unittest.TestCase):

    def test_pipeline_with_dynamic_dag(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_dynamic_dag.pipeline_task_generator, completed=True
        )

        for t in pipeline_instance.tasks:
            self.assertEqual(t.get_state().state_name, "completed")

        agg_task = pipeline_instance.tasks["aggregate_all"]

        self.assertEqual(agg_task.out.aggregate_inflated_number.fetch(), 20)

        self.assertEqual(
            {s for s in agg_task.out.insane_strings_passed_through.fetch().split(",")},
            {s for s in "abc1,abc2,abc4,abc3".split(",")}
        )

    def test_pipeline_with_complex_task_groups(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_complex_task_groups.pipeline_task_generator, completed=True
        )

        agg_task = pipeline_instance.tasks["grand_total"]

        self.assertEqual(agg_task.out.grand_total.fetch(), 240)

    def _test_pipeline_with_duck_typing(self):

        d = TestSandboxDir(self)

        s = [45, 3, 365, 6, 6788, 34, 2, 111]

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_with_duck_typing.create_dag_generator(s),
            completed=True
        )

        for t in pipeline_instance.tasks:
            self.assertEqual(t.get_state().state_name, "completed")

