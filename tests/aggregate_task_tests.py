import glob
import inspect
import json
import unittest

import test_helpers

from test_02_dynamic_dep_graph import pipeline_with_dynamic_dep_graph
from test_02_dynamic_dep_graph.pipeline_with_dynamic_dep_graph import get_expected_agg_result, \
    all_pipeline_tasks_with_wait_for_completion, all_pipeline_tasks
from test_utils import TestSandboxDir



class AggregateTaskTests(unittest.TestCase):

    def test_agg_task_with_matching_tasks_pipeline(self):
        self._validate_agg_task_pipeline(all_pipeline_tasks)

    def test_agg_task_with_completed_matching_tasks_pipeline(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            all_pipeline_tasks_with_wait_for_completion, completed=True
        )

        agg_task = pipeline_instance.tasks["aggregate_all"]

        self.assertEqual(agg_task.out.aggregate_inflated_number.fetch(), 20)

        self.assertEqual(
            {s for s in agg_task.out.insane_strings_passed_through.fetch().split(",")},
            {s for s in "abc1,abc2,abc4,abc3".split(",")}
        )


    def _validate_agg_task_pipeline(self, agg_pipeline_generator):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            agg_pipeline_generator, completed=True
        )

        agg_task = pipeline_instance.tasks["aggregate_all"]
        expected_agg_result = get_expected_agg_result(agg_task)

        self.assertEqual(expected_agg_result, 20)

        self.assertEqual(1, sum([
            1
            for line in test_helpers.load_file_as_string(agg_task.v_abs_task_env_file()).split("\n")
            if line.startswith("export all_work_chunk_tasks_outputs=$__pipeline_instance_dir/publish/work_chunk.*")
        ]))


    def test_launch_watch_and_launch_agg_task(self):

        d = TestSandboxDir(self)
        pipeline = d.pipeline_instance_from_generator(pipeline_with_dynamic_dep_graph.all_pipeline_tasks)

        self.assertEqual(len(pipeline.tasks), 2)

        init_task, agg_task = pipeline.tasks

        pipeline.run_sync()

        actual_agg_result = get_expected_agg_result(agg_task)

        self.assertEqual(actual_agg_result, 20)

    def test_agg_task_pipeline_graph(self):

        d = TestSandboxDir(self)
        pipeline = d.pipeline_instance_from_generator(pipeline_with_dynamic_dep_graph.all_pipeline_tasks)

        pipeline.run_sync()

        g = pipeline.summarized_dependency_graph()

        r = json.dumps(g, indent=4)
