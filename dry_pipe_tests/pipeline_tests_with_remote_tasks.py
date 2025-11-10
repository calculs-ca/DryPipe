import os
from pathlib import Path

from test_utils import TestSandboxDir
from dry_pipe_tests.pipeline_tests_with_remote_slurm_arrays import RemoteTestSite, remote_test_site
from dry_pipe_tests.pipeline_tests_with_single_tasks import TestFileSet
from dry_pipe import TaskConf


class RemoteTestFileSet(TestFileSet):

    def remote_test_site(self):
        return remote_test_site

    def test_run_pipeline(self):
        d = TestSandboxDir(self)
        self.pre_run(d.sandbox_dir)

        pipeline_instance = self.create_pipeline_instance(self.pipeline_instance_dir)
        pipeline_instance.monitor=self.create_monitor()
        pipeline_instance.run_sync(run_tasks_in_process=True, sleep_schedule=self.custom_sleep_schedule_parsed())

        tasks_by_keys = {
            task.key: task
            for task in pipeline_instance.query("*")
        }

        self.validate(tasks_by_keys)


class RemoteTestFileSetWithDataDirVar(RemoteTestFileSet):

    def dag_gen(self, dsl):

        pre_t = dsl.task(
            key="pre-t"
        ).outputs(
            d=dsl.file("subdir"),
            d2=dsl.file_set("**/*.zaz")
        ).calls(
            """
            #!/usr/bin/bash
            mkdir -p $__task_output_dir/subdir/d1/d2
            echo "123abc" > $__task_output_dir/subdir/d1/d2/f1.txt
            echo "abc123" > $__task_output_dir/subdir/d1/d2/f2.txt
                    
            mkdir -p $__task_output_dir/subdir2/d1
            echo "!abc123" > $__task_output_dir/subdir2/d1/f.zaz
            """
        )()

        yield pre_t


        yield dsl.task(
            key="t",
            task_conf=self.task_conf()
        ).inputs(
            pre_t.outputs.d,
            pre_t.outputs.d2,
            x=3,
            y=5,
            data_dir=Path("data-dir")
        ).outputs(
            random_files=dsl.file_set("**/*", "*.no"),
            palindrome=int,
            round_trip=dsl.file("rt.txt")
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
                                
            cat $__pipeline_instance_dir/output/pre-t/subdir/d1/d2/f1.txt > $round_trip
            cat $__pipeline_instance_dir/output/pre-t/subdir/d1/d2/f2.txt >> $round_trip
            cat $__pipeline_instance_dir/output/pre-t//subdir2/d1/f.zaz >> $round_trip

            """
        )()

    def validate(self, tasks_by_keys):
        super().validate(tasks_by_keys)

        s = tasks_by_keys["t"].outputs.round_trip.content_as_string()

        res = [
            line.strip()
            for line in s.strip().split("\n")
        ]

        self.assertEqual(res, ["123abc", "abc123", "!abc123"])


class RemoteTestFileSetWithGlobus(RemoteTestFileSetWithDataDirVar):

    def task_conf(self):

        repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

        rts = RemoteTestSite("maxl@mp2.ccs.usherbrooke.ca")

        globus_tok=str(Path(repo_dir, "tests", "tok.json"))
        client_id = "aca3664d-645e-4ea1-9afd-e73d6772a970"
        tc = TaskConf(
            executer_type="slurm",
            slurm_account="def-xroucou",
            ssh_remote_dest=rts.ssh_remote_dst(),
            globus_transfer=
                f"3e9dcd5e-6274-11f0-be3d-0efa17cb03ab:29f94847-8c7b-4c7b-b102-b3f3d5351e83:{globus_tok}:{client_id}",
            extra_env={
                "PYTHONPATH": ":".join([
                    f"$__pipeline_instance_dir/external-file-deps{repo_dir}"
                ]),
                "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            }
            #run_as_group="def-xroucou"
        )
        tc.python_bin = None
        return tc
