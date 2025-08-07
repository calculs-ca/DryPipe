import os

from tests.pipeline_tests_with_remote_slurm_arrays import RemoteTestSite
from tests.pipeline_tests_with_single_tasks import TestFileSet
from dry_pipe import TaskConf


class RemoteTestFileSet(TestFileSet):

    def task_conf(self):

        repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

        rts = RemoteTestSite()

        tc = TaskConf(
            executer_type="slurm",
            slurm_account="def-xroucou",
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

        rts = RemoteTestSite()
        rts.reset(self.pipeline_instance_dir)

        pipeline_instance = self.create_pipeline_instance(self.pipeline_instance_dir)
        pipeline_instance.monitor=self.create_monitor()
        pipeline_instance.run_sync(run_tasks_in_process=True)

        tasks_by_keys = {
            task.key: task
            for task in pipeline_instance.query("*")
        }

        self.validate(tasks_by_keys)
