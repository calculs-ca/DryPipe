import os.path
import unittest

from dry_pipe import DryPipe, TaskConf


class CliTests(unittest.TestCase):

    def test_prepare_remote_sites(self):

        def tasks(dsl):
            yield dsl.task(key="abc")\
                .calls("""
                    #!/usr/bin/env bash
                    echo "nothing.."
                """)

        this_dir = os.path.dirname(os.path.abspath(__file__))

        pipeline = DryPipe.create_pipeline(
            tasks,
            pipeline_code_dir=os.path.join(this_dir, "test_code_dir"),
            remote_task_confs=[
                TaskConf(
                    executer_type="process",
                    ssh_specs=f"maxl@ip29.ccs.usherbrooke.ca:~/.ssh/id_rsa",
                    remote_pipeline_code_dir="/home/maxl/test_prepare_remote_sites"
                )
            ]
        )
        pipeline.prepare_remote_sites()
