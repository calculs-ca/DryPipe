import os
import unittest

from dry_pipe import TaskConf
from pipeline_with_two_remote_sites import create_pipeline_generator_two_remote_sites
from test_04_remote_ssh_tasks import pipeline_with_remote_tasks
from test_04_remote_ssh_tasks.pipeline_with_remote_tasks import \
    ensure_remote_dirs_dont_exist, complete_and_validate_pipeline_instance
from test_utils import TestSandboxDir


class RemoteTaskTests1(unittest.TestCase):

    def test_remote_tasks_basics(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance(pipeline_with_remote_tasks.create_pipeline_with_remote_tasks(
            TaskConf(
                executer_type="process",
                ssh_specs=f"maxl@ip32.ccs.usherbrooke.ca:~/.ssh/id_rsa",
                remote_base_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests",
                remote_containers_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests/containers"
            )
        ))

        ensure_remote_dirs_dont_exist(pipeline_instance)

        complete_and_validate_pipeline_instance(pipeline_instance, self)


    def test_remote_tasks_using_remote_code_dir(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance(pipeline_with_remote_tasks.create_pipeline_with_remote_tasks(
            TaskConf(
                executer_type="process",
                ssh_specs=f"maxl@ip32.ccs.usherbrooke.ca:~/.ssh/id_rsa",
                remote_base_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests",
                remote_pipeline_code_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests/remote_code_dir_test_04"
            )
        ))

        ensure_remote_dirs_dont_exist(pipeline_instance)

        pipeline_instance.pipeline.prepare_remote_sites()

        complete_and_validate_pipeline_instance(pipeline_instance, self)

    def test_pipeline_with_two_remote_sites(self):

        d = TestSandboxDir(self)
        tc1 = TaskConf(
            executer_type="process",
            ssh_specs=f"maxl@ip32.ccs.usherbrooke.ca:~/.ssh/id_rsa",
            remote_base_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests"
        )

        tc2 = TaskConf(
            executer_type="process",
            ssh_specs="maxl@ip29.ccs.usherbrooke.ca:~/.ssh/id_rsa",
            remote_base_dir="/home/maxl/drypipe_tests"
        )

        pipeline_instance = d.pipeline_instance_from_generator(create_pipeline_generator_two_remote_sites(tc1, tc2))

        for task_conf in pipeline_instance.remote_sites_task_confs():
            if task_conf.ssh_specs.startswith("maxl@ip32"):
                task_conf_ip32 = task_conf
            elif task_conf.ssh_specs.startswith("maxl@ip29"):
                task_conf_ip29 = task_conf
            else:
                raise Exception("!")

        remote_executor_ip32 = task_conf_ip32.create_executer()
        remote_executor_ip29 = task_conf_ip29.create_executer()

        ensure_remote_dirs_dont_exist(pipeline_instance)

        remote_executor_ip29.upload_overrides(pipeline_instance, task_conf_ip29)
        remote_executor_ip32.upload_overrides(pipeline_instance, task_conf_ip32)

        pipeline_instance.run_sync()

        with open(os.path.join(d.sandbox_dir, "publish", "t3", "f3.txt")) as f:
            self.assertEqual(
                {1, 2},
                {int(s) for s in f.read().split() if s.strip() != ""}
            )





class RemoteTaskTests2(unittest.TestCase):

    def test_remote_tasks_basics_with_container(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance(pipeline_with_remote_tasks.create_pipeline_with_remote_tasks(
            TaskConf(
                executer_type="process",
                ssh_specs="maxl@ip29.ccs.usherbrooke.ca:~/.ssh/id_rsa",
                remote_base_dir="/home/maxl/drypipe_tests",
                container="singularity-test-container.sif",
                remote_containers_dir="/home/maxl/containers"
                # command_before_launch_container="module add singularity",
            )
        ))

        ensure_remote_dirs_dont_exist(pipeline_instance)

        complete_and_validate_pipeline_instance(pipeline_instance, self)


class RemoteTaskTestsWithSlurm(unittest.TestCase):

    def test_remote_tasks_with_slurm(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance(pipeline_with_remote_tasks.create_pipeline_with_remote_tasks(
            TaskConf(
                executer_type="slurm",
                slurm_account="def-xroucou_cpu",
                sbatch_options=[
                    "--time=0:5:00"
                ],
                ssh_specs=f"maxl@ip32.ccs.usherbrooke.ca:~/.ssh/id_rsa",
                remote_base_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests"
            )
        ))

        ensure_remote_dirs_dont_exist(pipeline_instance)

        complete_and_validate_pipeline_instance(pipeline_instance, self)

    def test_remote_tasks_with_slurm_and_container(self):

        d = TestSandboxDir(self)

        pipeline_instance = d.pipeline_instance(pipeline_with_remote_tasks.create_pipeline_with_remote_tasks(
            TaskConf(
                executer_type="slurm",
                slurm_account="def-xroucou_cpu",
                sbatch_options=[
                    "--time=0:5:00"
                ],
                ssh_specs="maxl@ip29.ccs.usherbrooke.ca:~/.ssh/id_rsa",
                container="singularity-test-container.sif",
                remote_containers_dir="/home/maxl/containers",
                remote_base_dir="/home/maxl/drypipe_tests"
            )
        ))

        ensure_remote_dirs_dont_exist(pipeline_instance)

        complete_and_validate_pipeline_instance(pipeline_instance, self)
