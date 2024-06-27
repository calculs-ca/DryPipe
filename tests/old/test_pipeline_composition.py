import unittest

from pipelines_with_sub_pipelines import sum_and_multiply_by_x_and_add_y, sum_and_multiply_by_x_and_add_y_and_z
from test_utils import TestSandboxDir


class PipelineCompositionTests(unittest.TestCase):

    def test_composed_pipeline_2_levels(self):

        pipeline_instance = TestSandboxDir(self).pipeline_instance_from_generator(
            sum_and_multiply_by_x_and_add_y([1, 2, 3, 4], 5, 6),
            completed=True
        )

        self.assertEqual(
            int(pipeline_instance.tasks["add_y"].out.final_result.fetch()),
            sum([1, 2, 3, 4]) * 5 + 6
        )

    def test_composed_pipeline_3_levels(self):

        pipeline_instance = TestSandboxDir(self).pipeline_instance_from_generator(
            sum_and_multiply_by_x_and_add_y_and_z([1, 2, 3, 4], 5, 6, 17),
            completed=True
        )

        self.assertEqual(
            int(pipeline_instance.tasks["add_z"].out.final_result.fetch()),
            sum([1, 2, 3, 4]) * 5 + 6 + 17
        )
