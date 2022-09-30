import os
import unittest

import test_helpers as th
from test_02_dynamic_dep_graph import pipeline_with_dynamic_dep_graph
from test_utils import TestSandboxDir


class InOutHashingTests(unittest.TestCase):

    def _test_input_hash_tracking(self):

        pipeline_instance = TestSandboxDir(self).pipeline_instance_from_generator(
            pipeline_with_dynamic_dep_graph.all_pipeline_tasks
        )

        pipeline_instance.init_work_dir()

        init_task, agg_task = pipeline_instance.tasks

        pipeline_instance.run_sync()

        self.assertEqual(
            th.count(pipeline_instance, lambda task: task.has_completed()),
            6
        )

        preparation_task = next(pipeline_instance.tasks_for_key_prefix("preparation_task"))

        task_for_work_chunk_2 = next(pipeline_instance.tasks_for_key_prefix("work_chunk.2"))

        self.assertEqual(task_for_work_chunk_2.key, "work_chunk.2")

        sig_chunk2_before = preparation_task.signature_of_produced_file("work_chunk.2.txt")

        th.rewrite_file(os.path.join(preparation_task.v_abs_work_dir(), "work_chunk.2.txt"), "23")

        # flag "stale" task signatures
        total_changed = pipeline_instance.recompute_signatures()

        self.assertEqual(1, total_changed)

        self.assertTrue(task_for_work_chunk_2.is_input_signature_flagged_as_changed())

        self.assertNotEqual(
            sig_chunk2_before,
            preparation_task.signature_of_produced_file("work_chunk.2.txt")
        )

        task_for_work_chunk_2.get_state().transition_to_waiting_for_deps()

        pipeline_instance.run_sync()

        total_changed = pipeline_instance.recompute_signatures()

        task_for_work_chunk_2.write_output_signature_file()

        self.assertFalse(task_for_work_chunk_2.is_input_signature_flagged_as_changed())

