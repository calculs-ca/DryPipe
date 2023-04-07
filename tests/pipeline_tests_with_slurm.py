import os.path
from functools import reduce

from base_pipeline_test import BasePipelineTest, TestWithDirectorySandbox
from dry_pipe import DryPipe
from dry_pipe.state_machine import StateFileTracker
from test_state_machine import StateMachineTester

def format_sbatch_array(array_indexes):

    def is_consecutive(n1, n2):
        return n1 + 1 == n2

    array_indexes = list(sorted(set(array_indexes)))

    def f(res, i):
        match res:
            case None:
                return [[i]]
            case [*o, [m]]:
                if is_consecutive(m, i):
                    return o + [[m, i]]
                else:
                    return o + [[m]] + [[i]]
            case [*o, [m, n]]:
                if is_consecutive(n, i):
                    return o + [[m, i]]
                else:
                    return o + [[m, n]] + [[i]]

    r1 = reduce(f, array_indexes, None)

    def g():
        for t in r1:
            match t:
                case [x]:
                    yield f"{x}"
                case [i, j]:
                    yield f"{i}-{j}"

    r2 = ",".join(g())
    print(f"{r2}")
    return r2




@DryPipe.python_call()
def manage_slurm_array(__task_output_dir, __pipeline_instance_dir):

    tracker = StateFileTracker(__pipeline_instance_dir)

    ready_count = 0
    fail_count = 0
    completed_count = 0
    array_task_count = 0

    # Upload to remote des,

    "array_children_task_keys.0.tsv"
    "array_children_task_keys.1.tsv"
    "array_children_task_keys.2.tsv"

    with open(os.path.join(__task_output_dir, "array_children_task_keys.0.tsv")) as f:
        slurm_array_id = 0
        for line in f:
            k = line.strip()
            state_file = tracker.load_state_file(k, slurm_array_id=slurm_array_id)
            if state_file.is_ready():
                ready_count += 1
            elif state_file.is_failed():
                fail_count += 1
            elif state_file.is_completed():
                completed_count += 1
            slurm_array_id += 1
        array_task_count = slurm_array_id

    if ready_count == 0:
        print("no 'ready' tasks to run")
        return

    # check if any was started

    array_indexes = []
    for state_file in tracker.all_state_files():
        if state_file.is_ready():
            state_file.transition_to_pre_launch()
            array_indexes.append(state_file.slurm_array_id)

    array_spec = format_sbatch_array(array_indexes)

    "squeue -u %A"


def add_steps_for_slurm_array_parent(task, task_builder):

    sbatch_script = """               
       __script_location=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )
       sbatch \\
           --array=$SBATCH_ARRAY_SPEC $SBATCH_ARGS \\
           --account=${slurm_account} \\
           --export=__script_location=$__script_location,DRYPIPE_TASK_DEBUG=$DRYPIPE_TASK_DEBUG \\
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

