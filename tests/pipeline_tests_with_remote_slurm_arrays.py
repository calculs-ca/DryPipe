
import os.path
from pathlib import Path

from dry_pipe import TaskConf, PortablePopen
from tests.cli_tests import test_cli
from tests.pipeline_tests_with_slurm_arrays import PipelineWithSlurmArray
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

    #def remote_pipeline_dir(self):
    #    return os.path.join(self.remote_base_dir(), os.path.basename(self.test_sandbox_dir.test_name))

    def reset(self, pipeline_instance_dir):
        d = os.path.join(self.remote_base_dir(), os.path.basename(pipeline_instance_dir))
        self.exec_remote(["rm", "-Rf", d])
        self.exec_remote(["mkdir", "-p", d])

    def ssh_remote_dst(self):
        return f"{self.user_at_host()}:{self.remote_base_dir()}"


remote_test_site_gh1301 = RemoteTestSite("maxl@gh1301")
remote_test_site_gh1301.sbatch_options = ["-p", "c-gh"]



class RemoteArrayTaskFullyAutomatedRun(PipelineWithSlurmArray):

    def launches_tasks_in_process(self):
        return False

    def task_conf(self):

        repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

        rts = remote_test_site_gh1301

        tc = TaskConf(
            executer_type="slurm",
            sbatch_options=remote_test_site_gh1301.sbatch_options,
            #slurm_account="def-xroucou",
            ssh_remote_dest=rts.ssh_remote_dst(),
            extra_env={
                "DRYPIPE_TASK_DEBUG": "True",
                "PYTHONPATH": ":".join([
                    f"$__pipeline_instance_dir/external-file-deps{repo_dir}"
                ])
            }
            #run_as_group="def-xroucou"
        )
        tc.python_bin = None
        return tc

    def test_run_pipeline(self):

        rts = remote_test_site_gh1301
        rts.reset(self.pipeline_instance_dir)

        pipeline_instance = self.create_pipeline_instance(self.pipeline_instance_dir)
        pipeline_instance.monitor=self.create_monitor()
        pipeline_instance.run_sync(run_tasks_in_process=True)

        tasks_by_keys = {
            task.key: task
            for task in pipeline_instance.query("*")
        }

        self.validate(tasks_by_keys)

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


class CliTestsPipelineWithSlurmArrayRemote(PipelineWithSlurmArray):

    def create_prepare_and_run_pipeline(self, d, until_patterns=["*"]):
        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)
        pipeline_instance.run_sync(until_patterns)
        return pipeline_instance

    def do_validate(self, pipeline_instance):
        self.validate({
            task.key: task
            for task in pipeline_instance.query("*")
        })

    def test_run_pipeline(self):
        pass

    def task_conf(self):

        repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

        tc = TaskConf(
            executer_type="slurm",
            #slurm_account="def-xroucou",
            sbatch_options=remote_test_site_gh1301.sbatch_options,
            extra_env={
                "DRYPIPE_TASK_DEBUG": "True",
                "PYTHONPATH": ":".join([
                    f"{remote_test_site_gh1301.remote_base_dir()}/CliTestsPipelineWithSlurmArrayRemote.test_array_upload_run_and_download/.drypipe",
                    f"{remote_test_site_gh1301.remote_base_dir()}/CliTestsPipelineWithSlurmArrayRemote.test_array_upload_run_and_download/external-file-deps{repo_dir}"
                ])
            }
        )
        tc.python_bin = None
        return tc

    def test_array_upload_run_and_download(self):
        d = TestSandboxDir(self)

        pipeline_instance = self.create_prepare_and_run_pipeline(d)

        pid = pipeline_instance.state_file_tracker.pipeline_instance_dir

        remote_test_site_gh1301.exec_remote(["rm", "-Rf", remote_test_site_gh1301.remote_base_dir()])
        remote_test_site_gh1301.exec_remote(["mkdir", "-p", remote_test_site_gh1301.remote_base_dir()])

        ssh_dest = f"{remote_test_site_gh1301.user_at_host()}:{remote_test_site_gh1301.remote_base_dir()}"

        test_cli(
            '--pipeline-instance-dir', pid,
            'task',
            f'{pid}/.drypipe/z',
            '--wait'
        )

        test_cli(
            '--pipeline-instance-dir', pid,
            'array-upload',
            '--task-key=array_parent',
            f'--ssh-remote-dest={ssh_dest}'
        )

        test_cli(
            '--pipeline-instance-dir', pid,
            'task',
            f'{pid}/.drypipe/array_parent',
            '--wait',
            f'--ssh-remote-dest={ssh_dest}'
        )

        test_cli(
            '--pipeline-instance-dir', pid,
            'array-download',
            '--task-key=array_parent',
            f'--ssh-remote-dest={ssh_dest}'
        )

        self.do_validate(pipeline_instance)
