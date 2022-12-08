import os

import test_helpers
from base_tests import test_containers_dir
from dry_pipe import DryPipe


def create_pipeline_with_remote_tasks(remote_task_conf):

    use_remote_code_dir = remote_task_conf.remote_pipeline_code_dir is not None

    def gen_tasks(dsl):

        local_task = dsl.task(
            key="local_task"
        ).produces(
            precious_output=dsl.file("name-of-pipeline-host.txt")
        ).calls(
            "xyz/write-hostname-to-file-other-script.sh"
        ).calls("""
            #!/usr/bin/env bash
            echo "54321abc" > $__pipeline_instance_dir/file-in-instance-dir.txt
        """)()

        yield local_task

        remote_task = dsl.task(
            key="remote_task",
            task_conf=remote_task_conf
        ).consumes(
            name_of_pipeline_host=local_task.out.precious_output,
            file_in_instance_dir=dsl.file("file-in-instance-dir.txt")
        ).produces(
            precious_output=dsl.file("precious-remote-output.txt"),
            v=dsl.var(str)
        ).calls(
            "xyz/write-hostname-to-file-other-script.sh" if use_remote_code_dir else """
            #!/usr/bin/env bash
            
            if [[ "${PLEASE_CRASH}" ]]; then
              exit 1
            fi
            
            if [[ ! -z "${name_of_pipeline_host+x}" ]]; then
              cat $name_of_pipeline_host >> $precious_output
            fi
            
            echo "hello from $(cat /etc/hostname)" >> $precious_output            
        """).calls("""
            #!/usr/bin/env bash
            echo "file_in_instance_dir:"
            cat $file_in_instance_dir        
            
            export v=$(cat $file_in_instance_dir)        
        """)()

        yield remote_task

    p = DryPipe.create_pipeline(
        gen_tasks,
        pipeline_code_dir=os.path.join(
            os.path.dirname(__file__), "src"
        ),
        containers_dir=test_containers_dir(),
        remote_task_confs=[remote_task_conf]
    )

    p.create_pipeline_instance(pipeline_instance_dir=os.path.dirname(__file__))

    return p


def complete_and_validate_pipeline_instance(pipeline_instance, test_case):

    pipeline_instance.run_sync()

    local_task, remote_task = pipeline_instance.tasks

    local_task_result = test_helpers.load_file_as_string(os.path.join(
        local_task.v_abs_work_dir(),
        "name-of-pipeline-host.txt"
    ))

    test_case.assertTrue("hello" in local_task_result)

    remote_task_result = test_helpers.load_file_as_string(os.path.join(
        remote_task.v_abs_work_dir(),
        "precious-remote-output.txt"
    )).strip()

    test_case.assertTrue(local_task_result in remote_task_result)

    test_case.assertTrue(len(remote_task_result.split("\n")) == 2)

    z = remote_task.out.v.fetch()

    test_case.assertEqual(z, "54321abc")
