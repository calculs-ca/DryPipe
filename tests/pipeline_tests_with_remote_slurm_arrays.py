
import os.path
import time
from pathlib import Path

from cli import cli_in_sub_process
from tests.cli_tests import test_cli
from dry_pipe import TaskConf, PortablePopen
from tests.pipeline_tests_with_slurm_arrays import PipelineWithSlurmArray, PipelineWithAutoRestart1, \
    PipelineWithAutoRestart2, PipelineWithPartialArrayMatch
from tests.test_utils import TestSandboxDir


class RemoteTestSite:

    def __init__(self, user_at_host):
        self._user_at_host = user_at_host
        self.sbatch_options = []


    def exec_remote(self, cmd):
        with PortablePopen(["ssh", self.user_at_host(), " ".join(cmd)]) as p:
            p.wait_and_raise_if_non_zero()

    def remote_base_dir(self):
        return "/home/maxl/tests-drypipe"

    def user_at_host(self):
        return self._user_at_host

    def remote_pipeline_dir(self, pipeline_instance_dir):
        return os.path.join(self.remote_base_dir(), os.path.basename(pipeline_instance_dir))

    def reset(self, pipeline_instance_dir):
        d = self.remote_pipeline_dir(pipeline_instance_dir)
        self.exec_remote(["rm", "-Rf", d])
        self.exec_remote(["mkdir", "-p", d])

    def ssh_remote_dst(self):
        return f"{self.user_at_host()}:{self.remote_base_dir()}"


remote_test_site = RemoteTestSite("maxl@gh1301")
remote_test_site.sbatch_options = ["-p", "c-gh"]

#remote_test_site = RemoteTestSite("maxl@ip40.ccs.usherbrooke.ca")
#remote_test_site.sbatch_options = ["--nodelist=cp41"]


class RemoteArrayTaskFullyAutomatedRun(PipelineWithSlurmArray):

    def launches_tasks_in_process(self):
        return False

    def is_log_level_debug(self):
        return True

    def remote_test_site(self):
        return remote_test_site

    def custom_sleep_schedule(self):
        return "2"

    def task_conf(self):

        repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

        rts = remote_test_site

        tc = TaskConf(
            executer_type="slurm",
            sbatch_options=remote_test_site.sbatch_options,
            ssh_remote_dest=rts.ssh_remote_dst(),
            extra_env={
                "PYTHONPATH": ":".join([
                    f"$__pipeline_instance_dir/external-file-deps{repo_dir}"
                ]),
                "DRYPIPE_SLURM_STD_OUT_ERR_LOG": "True",
                "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            }
            #run_as_group="def-xroucou"
        )
        tc.python_bin = None
        return tc

    def test_run_pipeline(self):
        d = TestSandboxDir(self)
        self.pre_run(d.sandbox_dir)

        pipeline_instance = self.create_pipeline_instance(self.pipeline_instance_dir)
        pipeline_instance.monitor=self.create_monitor()
        pipeline_instance.run_sync(run_tasks_in_process=False, sleep_schedule=self.custom_sleep_schedule_parsed())

        tasks_by_keys = {
            task.key: task
            for task in pipeline_instance.query("*")
        }

        self.validate(tasks_by_keys)


        rst = self.remote_test_site()

        #TODO: change a log, to validate that rsync really worked

        test_cli(
            self,
            f"--pipeline-instance-dir={self.pipeline_instance_dir}",
            "fetch-remote-state",
            f"--task-key=array_parent",
            "--wait",
        )

        for k, t in tasks_by_keys.items():
            if k.startswith("t_"):
                with open(t.outputs.slurm_result_in_file) as f:
                    r = int(f.read().strip())
                    expected = int(t.outputs.slurm_result)
                    self.assertEqual(expected, r, "slurm_result_in_file does not match expected result")
                    self.assertEqual(expected, int(t.outputs.var_result), "var_result does not match expected result")

                self.assertEqual(
                    {
                        f"output/{k}/slurm_result.txt",
                        f"output/{k}/a.txt",
                        f"output/{k}/sub1/a/a.txt"
                    },
                    {
                        str(Path(f).relative_to(self.pipeline_instance_dir))
                        for f in t.outputs.random_files
                    }
                )


class RemoteArrayTaskFullyAutomatedRun2Steps2Sbatches(RemoteArrayTaskFullyAutomatedRun):

    def sbatch_step2(self, dsl):
        return ["--time=30:00", "-p", "c-gh"]



class RemotePipelineWithAutoRestart1(PipelineWithAutoRestart1):

    def remote_test_site(self):
        return remote_test_site

    def is_log_level_debug(self):
        return True

    def test_run_pipeline(self):
        d = TestSandboxDir(self)
        self.pre_run(d.sandbox_dir)
        super().test_run_pipeline()

    def sbatch_step2(self, dsl):
        return ["--time=30:00", "-p", "c-gh"]


class RemotePipelineWithAutoRestart2(PipelineWithAutoRestart2):

    def remote_test_site(self):
        return remote_test_site

    def test_run_pipeline(self):
        d = TestSandboxDir(self)
        self.pre_run(d.sandbox_dir)
        super().test_run_pipeline()


class PipelineWithPartialArrayMatchRemote(PipelineWithPartialArrayMatch):

    def remote_test_site(self):
        return remote_test_site

    def task_conf(self):
        rts = self.remote_test_site()
        return TaskConf(
            executer_type="slurm",
            ssh_remote_dest=rts.ssh_remote_dst(),
            sbatch_options=rts.sbatch_options,
            extra_env={
                "DRYPIPE_TASK_DEBUG": "True" if self.is_log_level_debug() else "False",
                "PYTHONPATH": self.python_path_for_remote_site(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            },
            use_squeue=False
        )

    def test_run_pipeline(self):
        d = TestSandboxDir(self)
        self.pre_run(d.sandbox_dir)
        super().test_run_pipeline()

    #def launches_tasks_in_process(self):
    #    return True
