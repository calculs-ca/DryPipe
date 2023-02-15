import glob
import os
import unittest

import pipeline_with_code_and_container_upsync
import pipelines_with_external_file_deps_and_remote_task
from dry_pipe import TaskConf, DryPipe
from pipeline_with_two_remote_sites import create_pipeline_generator_two_remote_sites
from pipelines_with_remote_tasks import dag_gen_fileset_output
from test_04_remote_ssh_tasks import pipeline_with_remote_tasks
from test_04_remote_ssh_tasks.pipeline_with_remote_tasks import \
    complete_and_validate_pipeline_instance
from test_utils import TestSandboxDir, ensure_remote_dirs_dont_exist


class RemoteTaskTests1(unittest.TestCase):

    def _test_prepare_code_dir_remote_site(self, d, with_container):

        gen, validator, tc = pipeline_with_code_and_container_upsync.dag_gen(
            self,
            with_container
        )

        pipeline_code_dir = os.path.dirname(os.path.dirname(__file__))

        pipeline_instance = DryPipe.create_pipeline(
            gen,
            pipeline_code_dir=pipeline_code_dir,
            remote_task_confs=[tc]
        ).create_pipeline_instance(
            pipeline_instance_dir=d.sandbox_dir,
            task_conf=tc
        )

        ensure_remote_dirs_dont_exist(pipeline_instance)

        pipeline_instance.pipeline.prepare_remote_sites()

        pipeline_instance.run_sync()

        validator(pipeline_instance)

    def test_prepare_code_dir_remote_site(self):
        d = TestSandboxDir(self)
        self._test_prepare_code_dir_remote_site(d, None)

    def test_prepare_code_dir_remote_site_with_container(self):
        d = TestSandboxDir(self)
        pipeline_code_dir = os.path.dirname(os.path.dirname(__file__))
        container = os.path.join(pipeline_code_dir, "tests", "containers", "singularity-test-container.sif")
        self._test_prepare_code_dir_remote_site(d, container)

    def test_remote_file_cache(self):

        d = TestSandboxDir(self)

        gen, validator = pipelines_with_external_file_deps_and_remote_task.dag_gen(TaskConf(
            executer_type="process",
            ssh_specs=f"maxl@ip32.ccs.usherbrooke.ca:~/.ssh/id_rsa",
            remote_base_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests"
        ), self)


        pipeline_instance = d.pipeline_instance_from_generator(gen)

        ensure_remote_dirs_dont_exist(pipeline_instance, clear_file_cache=True)

        pipeline_instance.run_sync()

        validator(pipeline_instance)

    def test_remote_task_with_fileset_output(self):
        d = TestSandboxDir(self)

        gen, validator = dag_gen_fileset_output(TaskConf(
            executer_type="process",
            ssh_specs=f"maxl@ip32.ccs.usherbrooke.ca:~/.ssh/id_rsa",
            remote_base_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests",
            remote_containers_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests/containers"
        ), self)

        pipeline_instance = d.pipeline_instance_from_generator(gen)

        ensure_remote_dirs_dont_exist(pipeline_instance)

        pipeline_instance.run_sync()

        validator(pipeline_instance)

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
            remote_base_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests",
            remote_containers_dir="dummy1"
        )

        tc2 = TaskConf(
            executer_type="process",
            ssh_specs="maxl@ip29.ccs.usherbrooke.ca:~/.ssh/id_rsa",
            remote_base_dir="/home/maxl/drypipe_tests",
            remote_containers_dir="dummy2"
        )

        pipeline_instance = d.pipeline_instance_from_generator(create_pipeline_generator_two_remote_sites(tc1, tc2))

        for task_conf in pipeline_instance.remote_sites_task_confs():
            if task_conf.ssh_specs.startswith("maxl@ip32"):
                task_conf_ip32 = task_conf
            elif task_conf.ssh_specs.startswith("maxl@ip29"):
                task_conf_ip29 = task_conf
            else:
                raise Exception("!")

        ensure_remote_dirs_dont_exist(pipeline_instance)

        with task_conf_ip32.create_executer() as remote_executor_ip32:
            with task_conf_ip29.create_executer() as remote_executor_ip29:

                pipeline_instance.init_work_dir()

                remote_executor_ip29.prepare_remote_instance_directory(pipeline_instance, task_conf_ip29)
                remote_executor_ip32.prepare_remote_instance_directory(pipeline_instance, task_conf_ip32)

                pipeline_instance.run_sync()

                with open(os.path.join(d.sandbox_dir, "output", "t3", "f3.txt")) as f:
                    self.assertEqual(
                        {1, 2},
                        {int(s) for s in f.read().split() if s.strip() != ""}
                    )

                def fetch_containers_dir_in_pipeline_env(remote_executor, task_conf):
                    f = os.path.join(
                        task_conf.remote_base_dir,
                        "RemoteTaskTests1.test_pipeline_with_two_remote_sites",
                        ".drypipe",
                        "pipeline-env.sh"
                    )

                    try:
                        content = remote_executor.fetch_remote_file_content(f)
                        res = [
                            l.split("=")[1]
                            for l in content.split("\n")
                            if l.startswith("export __containers_dir")
                        ]
                        self.assertEqual(len(res), 1)
                        return res[0].strip()
                    except FileNotFoundError as fnfe:
                        raise Exception(f"{fnfe} while fetching {f} from {task_conf.ssh_specs}")

                self.assertEqual(fetch_containers_dir_in_pipeline_env(remote_executor_ip32, tc1), "dummy1")
                self.assertEqual(fetch_containers_dir_in_pipeline_env(remote_executor_ip29, tc2), "dummy2")

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

        cedar = TaskConf(
            executer_type="slurm",
            slurm_account="def-xroucou_cpu",
            sbatch_options=[
                "--time=0:5:00"
            ],
            ssh_specs=f"maxl@cedar.computecanada.ca:~/.ssh/id_ed25519",
            remote_base_dir="/home/maxl/drypipe-tests"
        )

        ip32 = TaskConf(
            executer_type="slurm",
            slurm_account="def-xroucou_cpu",
            sbatch_options=[
                "--time=0:5:00"
            ],
            ssh_specs=f"maxl@ip32.ccs.usherbrooke.ca:~/.ssh/id_rsa",
            remote_base_dir="/nfs3_ib/ip32-ib/home/maxl/drypipe-tests"
        )

        pipeline_instance = d.pipeline_instance(pipeline_with_remote_tasks.create_pipeline_with_remote_tasks(
            ip32
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
