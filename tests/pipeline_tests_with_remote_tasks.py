import os
from pathlib import Path

from tests.pipeline_tests_with_remote_slurm_arrays import RemoteTestSite
from tests.pipeline_tests_with_single_tasks import TestFileSet
from dry_pipe import TaskConf


class RemoteTestFileSet(TestFileSet):

    def task_conf(self):

        rts = RemoteTestSite()

        tc = TaskConf(
            executer_type="slurm",
            slurm_account="def-xroucou",
            ssh_remote_dest=rts.ssh_remote_dst(),
            extra_env={
                "DRYPIPE_TASK_DEBUG": "True"
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


class RemoteTestFileSetWithDataDirVar(RemoteTestFileSet):

    def dag_gen(self, dsl):
        yield dsl.task(
            key="t",
            task_conf=self.task_conf()
        ).inputs(
            x=3,
            y=5,
            data_dir=Path("data-dir")
        ).outputs(
            random_files=dsl.file_set("**/*", "*.no"),
            palindrome=int
        ).calls(
            """
            #!/usr/bin/bash

            export palindrome=`cat $data_dir/palindrome.txt`            
            cp $data_dir/palindrome.txt $__task_output_dir            

            mkdir -p $__task_output_dir/a1/b        
            mkdir -p $__task_output_dir/a2/c/x/y/z            

            echo "z" > $__task_output_dir/a1/a.yes
            touch $__task_output_dir/a2/c/x/y/z/pop

            echo "z" > $__task_output_dir/z.no                    
            echo "z" > $__task_output_dir/a2/b.txt

            """
        )()



class RemoteTestFileSetWithGlobus(RemoteTestFileSetWithDataDirVar):

    def task_conf(self):

        repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

        rts = RemoteTestSite()

        tc = TaskConf(
            executer_type="slurm",
            slurm_account="def-xroucou",
            ssh_remote_dest=rts.ssh_remote_dst(),
            globus_transfer="3e9dcd5e-6274-11f0-be3d-0efa17cb03ab:29f94847-8c7b-4c7b-b102-b3f3d5351e83",
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
