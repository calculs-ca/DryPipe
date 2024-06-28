
import os.path
from dry_pipe import TaskConf, PortablePopen
from tests.cli_tests import test_cli
from tests.pipeline_tests_with_slurm_arrays import PipelineWithSlurmArray
from tests.test_utils import TestSandboxDir


class RemoteTestSite:

    def __init__(self, test_sandbox_dir):
        self.test_sandbox_dir = test_sandbox_dir

    def exec_remote(self, cmd):
        with PortablePopen(["ssh", self.user_at_host(), " ".join(cmd)]) as p:
            p.wait_and_raise_if_non_zero()

    def remote_base_dir(self):
        return "/home/maxl/tests-drypipe"

    def user_at_host(self):
        return "maxl@mp2.ccs.usherbrooke.ca"

    def remote_pipeline_dir(self):
        return os.path.join(self.remote_base_dir(), os.path.basename(self.test_sandbox_dir.test_name))

    def reset(self):
        self.exec_remote(["rm", "-Rf", self.remote_pipeline_dir()])
        self.exec_remote(["mkdir", "-p", self.remote_pipeline_dir()])

    def ssh_remote_dst(self):
        return f"{self.user_at_host()}:{self.remote_base_dir()}"


remote_test_site = RemoteTestSite(None)


class RemoteArrayTaskFullyAutomatedRun(PipelineWithSlurmArray):

    def launches_tasks_in_process(self):
        return False

    def do_validate(self, pipeline_instance):
        self.validate({
            task.key: task
            for task in pipeline_instance.query("*")
        })

    def task_conf(self):

        repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

        rts = RemoteTestSite(TestSandboxDir(self))

        tc = TaskConf(
            executer_type="slurm",
            slurm_account="def-xroucou",
            ssh_remote_dest=rts.ssh_remote_dst(),
            extra_env={
                "DRYPIPE_TASK_DEBUG": "True",
                "PYTHONPATH": ":".join([
                    f"{rts.remote_base_dir()}/RemoteArrayTaskFullyAutomatedRun.test_run_pipeline/.drypipe", # <- put in TaskConf overrides
                    # OR prepend dynamically on remote sites
                    f"{rts.remote_base_dir()}/RemoteArrayTaskFullyAutomatedRun.test_run_pipeline/external-file-deps{repo_dir}"
                ])
            }
        )
        tc.python_bin = None
        return tc

    def test_run_pipeline(self):
        d = TestSandboxDir(self)

        rts = RemoteTestSite(d)
        rts.reset()

        pipeline_instance = self.create_pipeline_instance(d.sandbox_dir)
        pipeline_instance.run_sync(run_tasks_in_process=True)

        self.validate({
            task.key: task
            for task in pipeline_instance.query("*")
        })

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
            slurm_account="def-xroucou",
            extra_env={
                "DRYPIPE_TASK_DEBUG": "True",
                "PYTHONPATH": ":".join([
                    f"{remote_test_site.remote_base_dir()}/CliTestsPipelineWithSlurmArrayRemote.test_array_upload/.drypipe",
                    f"{remote_test_site.remote_base_dir()}/CliTestsPipelineWithSlurmArrayRemote.test_array_upload/external-file-deps/{repo_dir}"
                ])
            }
        )
        tc.python_bin = None
        return tc

    def test_array_upload(self):
        d = TestSandboxDir(self)

        pipeline_instance = self.create_prepare_and_run_pipeline(d)

        pid = pipeline_instance.state_file_tracker.pipeline_instance_dir

        remote_test_site.exec_remote(["rm", "-Rf", remote_test_site.remote_base_dir()])
        remote_test_site.exec_remote(["mkdir", "-p", remote_test_site.remote_base_dir()])

        ssh_dest = f"{remote_test_site.user_at_host()}:{remote_test_site.remote_base_dir()}"

        test_cli(
            '--pipeline-instance-dir', pid,
            'task',
            f'{pid}/.drypipe/z',
            '--wait'
        )

        test_cli(
            '--pipeline-instance-dir', pid,
            'upload-array',
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
            'download-array',
            '--task-key=array_parent',
            f'--ssh-remote-dest={ssh_dest}'
        )

        self.do_validate(pipeline_instance)
