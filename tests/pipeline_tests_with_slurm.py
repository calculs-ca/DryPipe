import os.path

from base_pipeline_test import BasePipelineTest, TestWithDirectorySandbox
from dry_pipe import DryPipe
from dry_pipe.state_machine import StateFileTracker
from test_state_machine import StateMachineTester


def add_steps_for_slurm_array_parent(task, task_builder):

    sbatch_script = """               
       __script_location=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )
       sbatch \\
           --array=$SBATCH_ARRAY_SPEC $SBATCH_ARGS \\
           --account={self.task_conf.slurm_account} \\
           --export=__script_location=$__script_location,__is_slurm=True,DRYPIPE_TASK_DEBUG=$DRYPIPE_TASK_DEBUG \\
           --signal=B:USR1@50 \\
           --parsable \\
           $__script_location/task                              
    """

    return

def add_steps_for_remote_task(task, task_builder):

    task_output_dir = task.output_dir()

    with open(os.path.join(task_output_dir, "upload-includes.txt")) as f:
        f.write("...")

    with open(os.path.join(task_output_dir, "download-includes.txt")) as f:
        f.write("...")

    rsync_upload_script = """    
    """

    rsync_download_script = """
    """


class PipelineWithSlurmArray(BasePipelineTest):

    def dag_gen(self, dsl):

        t0 = dsl.task(
            key="z"
        ).outputs(
            r=str
        ).calls("""
            #!/usr/bin/env bash            
            export r="abc"
        """)()

        yield t0

        for i in [1, 2]:
            for c in ["a", "b"]:
                yield dsl.task(
                    key=f"t_{c}_{i}",
                    is_slurm_array_child=True
                ).inputs(
                    r=t0.outputs.r,
                    i=i
                ).outputs(
                    slurm_result=int
                ).calls("""
                    #!/usr/bin/env bash            
                    export slurm_result=$(( $x + $z ))
                """)()

        for match in dsl.query_all_or_nothing("t_*", state="ready"):
            yield dsl.task(
                key=f"t_array_parent"
            ).slurm_array_parent(
                children_tasks=match.tasks # "t_*"
            )()

        for _ in dsl.query_all_or_nothing("t_a_*"):
            yield dsl.task(
                key=f"a-digest"
            )()

        for _ in dsl.query_all_or_nothing("t_b_*"):
            yield dsl.task(
                key=f"b-digest"
            )()

    def validate(self, tasks_by_keys):
        pass


class SlurmArrayHandingStateMachineTest(TestWithDirectorySandbox):

    def test_slurm_array_state_machine(self):

        t = PipelineWithSlurmArray()

        tester = StateMachineTester(self, lambda dsl: t.dag_gen(dsl), StateFileTracker(self.pipeline_instance_dir))

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready('z')

        tester.set_completed_on_disk("z")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready('t_array_parent')

        tester.set_completed_on_disk("t_a_1")
        tester.set_completed_on_disk("t_b_1")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready()

        tester.set_completed_on_disk("t_a_2")

        tester.iterate_once_and_mutate_set_of_next_state_files_ready()
        tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        tester.assert_set_of_next_tasks_ready("a-digest")

        #tester.assert_set_of_next_tasks_ready(*[])



        #tester.iterate_once_and_mutate_set_of_next_state_files_ready()


        #i2 = tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        #tester.assert_set_of_next_tasks_ready(*[])

        #tester.assert_set_of_next_tasks_ready('t_a_1', 't_a_2', 't_b_2', 't_b_1')
        # because of pending queries, it's never over until it's over:
        #for _ in [1,2,3,4,5]:
        #    tester.iterate_once_and_mutate_set_of_next_state_files_ready()

        #tester.assert_set_of_next_tasks_ready(*[])
        #tester.state_file_tracker.set_completed_on_disk("t1")

