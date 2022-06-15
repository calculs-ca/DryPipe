import os
import unittest

from dry_pipe import TaskConf
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
