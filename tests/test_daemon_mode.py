import os
import unittest

import pipeline_for_ssh_crash_recovery_tests
import pipeline_with_single_python_task
from dry_pipe import DryPipe, TaskConf
from dry_pipe.janitors import Janitor
from dry_pipe.pipeline import Pipeline
from pipeline_with_multistep_tasks import three_steps_pipeline_task_generator, three_steps_pipeline_expected_output
from test_01_simple_static_pipeline.simple_static_pipeline import simple_static_pipeline, validate_pipeline_execution
from test_utils import TestSandboxDir, copy_pre_existing_file_deps_from_code_dir, ensure_remote_dirs_dont_exist
import test_utils


class DaemonModeTests(unittest.TestCase):

    def test_load_instances_from_base_dir(self):

        d = TestSandboxDir(self)

        pipeline = DryPipe.create_pipeline(simple_static_pipeline)

        i = Pipeline.pipeline_instances_iterator({d.sandbox_dir: pipeline})

        self.assertEqual(len(list(i)), 0)

        i.create_instance_in(d.sandbox_dir, "p1")

        self.assertEqual(len(list(i)), 1)

        p1 = list(i)[0]

        self.assertTrue(p1.pipeline_instance_dir.endswith("p1"))

        i.create_instance_in(d.sandbox_dir, "p2")
        i.create_instance_in(d.sandbox_dir, "p3")

        self.assertEqual(len(list(i)), 3)

    def test_load_instances_two_types_from_base_dir(self):

        d = TestSandboxDir(self)
        d1 = d.create_subdir("three_steps_pipelines")
        d2 = d.create_subdir("pipelines_with_single_python_task")

        pipeline1 = DryPipe.create_pipeline(three_steps_pipeline_task_generator)
        pipeline2 = DryPipe.create_pipeline(pipeline_with_single_python_task.pipeline)

        i = pipeline1.pipeline_instances_iterator({
            d1: pipeline1,
            d2: pipeline2
        })

        self.assertEqual(len(list(i)), 0)

        i.create_instance_in(d1, "p1")

        self.assertEqual(len(list(i)), 1)

        p1 = list(i)[0]

        self.assertTrue(p1.pipeline_instance_dir.endswith("p1"))

        i.create_instance_in(d1, "p2")
        i.create_instance_in(d2, "p3")

        self.assertEqual(len(list(i)), 3)

    def test_run_two_instances_until_done(self):

        d = TestSandboxDir(self)

        pipeline = DryPipe.create_pipeline(simple_static_pipeline)

        i = Pipeline.pipeline_instances_iterator({d.sandbox_dir: pipeline})
        i.create_instance_in(d.sandbox_dir, "p1")
        i.create_instance_in(d.sandbox_dir, "p2")

        for i0 in i:
            copy_pre_existing_file_deps_from_code_dir(i0)

        janitor = Janitor(pipeline_instances_iterator=i)

        work_iterator = janitor.iterate_main_work(sync_mode=True)

        has_work = next(work_iterator)
        while has_work:
            has_work = next(work_iterator)

        c = 0

        for p in Pipeline.pipeline_instances_iterator(
                {d.sandbox_dir: pipeline}, ignore_completed=False
        ):
            validate_pipeline_execution(p, self)
            c += 1

        self.assertEqual(c, 2)

    def test_run_instances_two_types_from_base_dir_until_done(self):

        d = TestSandboxDir(self)
        d1 = d.create_subdir("three_steps_pipelines")
        d2 = d.create_subdir("pipelines_with_single_python_task")

        pipeline1 = DryPipe.create_pipeline(three_steps_pipeline_task_generator)
        pipeline2 = DryPipe.create_pipeline(pipeline_with_single_python_task.pipeline)

        dir_to_pipeline = {
            d1: pipeline1,
            d2: pipeline2
        }

        i = Pipeline.pipeline_instances_iterator(dir_to_pipeline)

        i.create_instance_in(d1, "p1_three_steps_pipelines")
        i.create_instance_in(d2, "p2_pipelines_with_single_python_task")

        janitor = Janitor(pipeline_instances_iterator=i)

        work_iterator = janitor.iterate_main_work(sync_mode=True)

        has_work = next(work_iterator)
        while has_work:
            has_work = next(work_iterator)

        c = 0

        for p in Pipeline.pipeline_instances_iterator(
            dir_to_pipeline, ignore_completed=False
        ):
            if p.instance_dir_base_name() == "p1_three_steps_pipelines":
                three_phase_task = p.tasks["three_phase_task"]
                self.assertEqual(three_steps_pipeline_expected_output(), three_phase_task.out.out_file.load_as_string())
            elif p.instance_dir_base_name() == "p2_pipelines_with_single_python_task":
                pipeline_with_single_python_task.validate_single_task_pipeline(p)
            else:
                raise Exception(f"unknown pipeline {p.instance_dir_base_name()}")
            c += 1

        self.assertEqual(c, 2)


class LongRunningDaemonModeTests(unittest.TestCase):

    def test_ssh_crash_recovery(self):

        test_utils.log_4_debug_daemon_mode()

        tc_remote = TaskConf(
            executer_type="process",
            #ssh_specs="maxl@127.0.0.1:~/.ssh/id_dsa",
            ssh_specs="maxl@ip29.ccs.usherbrooke.ca:~/.ssh/id_rsa",
            remote_base_dir="/home/maxl/drypipe_tests",
            remote_containers_dir="dummyZzz"
        )

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance_from_generator(
            pipeline_for_ssh_crash_recovery_tests.create_pipeline(
                #TaskConf.default()
                tc_remote
            )
        )

        ensure_remote_dirs_dont_exist(pipeline_instance)

        janitor = Janitor(pipeline_instance=pipeline_instance)
        thread = janitor.start()
        janitor.start_remote_janitors()
        thread.join()

        # socat tcp-listen:7775,bind=127.0.0.1,fork tcp:maxl@ip29.ccs.usherbrooke.ca:22
        # sudo tc qdisc add dev wlp1s0 root netem loss 30%


