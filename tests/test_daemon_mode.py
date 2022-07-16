import unittest

import pipeline_for_ssh_crash_recovery_tests
from dry_pipe import DryPipe, TaskConf
from dry_pipe.janitors import Janitor
from test_01_simple_static_pipeline.simple_static_pipeline import simple_static_pipeline, validate_pipeline_execution
from test_04_remote_ssh_tasks.pipeline_with_remote_tasks import ensure_remote_dirs_dont_exist
from test_utils import TestSandboxDir, copy_pre_existing_file_deps_from_code_dir


class DaemonModeTests(unittest.TestCase):

    def test_load_instances_from_base_dir(self):

        d = TestSandboxDir(self)

        pipeline = DryPipe.create_pipeline(simple_static_pipeline)

        i = pipeline.pipeline_instance_iterator_for_dir(d.sandbox_dir)

        self.assertEqual(len(list(i)), 0)

        i.create_instance_in("p1")

        self.assertEqual(len(list(i)), 1)

        p1 = list(i)[0]

        self.assertTrue(p1.pipeline_instance_dir.endswith("p1"))

        i.create_instance_in("p2")
        i.create_instance_in("p3")

        self.assertEqual(len(list(i)), 3)

    def test_run_two_instances_until_done(self):

        d = TestSandboxDir(self)

        pipeline = DryPipe.create_pipeline(simple_static_pipeline)

        i = pipeline.pipeline_instance_iterator_for_dir(d.sandbox_dir)
        i.create_instance_in("p1")
        i.create_instance_in("p2")

        for i0 in i:
            copy_pre_existing_file_deps_from_code_dir(i0)

        janitor = Janitor(pipeline=DryPipe.create_pipeline(simple_static_pipeline), pipeline_instances_dir=d.sandbox_dir)

        work_iterator = janitor.iterate_main_work(sync_mode=True)

        has_work = next(work_iterator)
        while has_work:
            has_work = next(work_iterator)

        c = 0

        for p in pipeline.pipeline_instance_iterator_for_dir(
                d.sandbox_dir, ignore_completed=False
        ):
            validate_pipeline_execution(p, self)
            c += 1

        self.assertEqual(c, 2)

    def test_ssh_crash_recovery(self):

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

        pipeline_instance.run_sync(stay_alive_when_no_more_work=True, sync=False)

        # socat tcp-listen:7775,bind=127.0.0.1,fork tcp:maxl@ip29.ccs.usherbrooke.ca:22
        # sudo tc qdisc add dev wlp1s0 root netem loss 30%


