import glob
import json
import logging
import os.path
import textwrap
import time
import unittest
from functools import reduce
from pathlib import Path

from base_pipeline_test import BasePipelineTest, TestWithDirectorySandbox
from dry_pipe import DryPipe, PortablePopen
from dry_pipe.state_machine import StateFileTracker
from mockups import TaskMockup
from test_state_machine import StateMachineTester


def throttle(share_dir, threshold, task_key):

    def count_in_queue():
        return len(glob.glob(os.path.join(share_dir, "*")))


    while True:
        if count_in_queue() >= threshold:
            time.sleep(5)
            continue
        else:
            break

    Path(os.path.join(share_dir, task_key)).touch()



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

    def iterate_task_batches():
        for task_keys_file in glob.glob(os.path.join(__task_output_dir, "array_children_task_keys.*.tsv")):
            _, batch_number, _ = os.path.basename(task_keys_file).split(".")
            yield int(batch_number), task_keys_file

    task_batches = [
        b[1] for b in sorted(iterate_task_batches(), key=lambda t: t[0])
    ]

    tracker = StateFileTracker(__pipeline_instance_dir)

    ready_count = 0
    fail_count = 0
    completed_count = 0
    array_task_count = 0

    with open(task_batches[0]) as f:
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


    job_ids = []

    def squeue_array_status():
        job_ids_as_str = ",".join(job_ids)
        with PortablePopen(f'squeue -r --format=%i --jobs={job_ids_as_str}' , shell=True) as p:
            p.wait_and_raise_if_non_zero()
            for line in p.iterate_stdout_lines():
                job_id, task_array_id = line.split("_")



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


class SlurmArrayScenario(TestWithDirectorySandbox):

    def setUp(self):
        self.task_control_dir = os.path.join(
            self.pipeline_instance_dir,
            ".drypipe",
            "parent-task"
        )

        self.current_step = 0

        self.steps = []

        for s in self.scenario():
            t = textwrap.dedent(s).strip()
            states_by_task_key = {}
            for squeue_for_task in t.split("\n"):
                squeue_for_task = squeue_for_task.strip().split()
                states_by_task_key[squeue_for_task[0]] = squeue_for_task[1:]
            self.steps.append(states_by_task_key)

        slurm_scenario = self

        class SlurmArrayParentTask4Test(SlurmArrayParentTask):

            def call_sbatch(self, command_args):
                slurm_scenario.current_step

            def call_squeue(self):
                for task_key, state in slurm_scenario.steps[slurm_scenario.current_step].items():
                    pass


        self.parent_task = SlurmArrayParentTask4Test(self.task_control_dir)

        with open(self.parent_task.task_keys_file(), "w") as f:
            for k in self.children_task_keys():
                f.write(f"{k}\n")

    def children_task_keys(self):
        task_keys = set()
        for s in self.steps:
            for k in s.keys():
                task_keys.add(k)
        return sorted(task_keys)

    def load_task_keys(self, f):
        def gen():
            with open(f) as _f:
                for k in _f:
                    yield k.strip()
        return set(gen())

    def assert_task_keys_in_array_file(self, array_number: int, task_keys: set[str]):
        self.assertEqual(
            self.load_task_keys(self.parent_task.i_th_arrays_file(array_number)),
            task_keys
        )


    def test(self):

        self.parent_task.prepare_and_launch_next_array()

        self.assert_task_keys_in_array_file(0, {"t1", "t2", "t3", "t4"})
        self.parent_task.compare_squeue_with_state_files()



    def scenario(self):
        # s: started
        # f: failed
        # c: complete
        return [
            """
            t1 s s c   
            t2 s f f
            t3 s s c
            t4 s s t
            """,
            """   
            t2 s c c
            t4 s f f
            """,
            "t4 s s c"
        ]



class SlurmArrayParentTask:

    def __init__(self, control_dir, logger=None):
        #self.restart_at_step = restart_at_step
        self.control_dir = control_dir
        self.debug = True

        #with open(self.control_dir, "task-conf.json") as tc:
        #    self.task_conf = json.loads(tc.read())

        self.tracker = StateFileTracker(
            pipeline_instance_dir=os.path.dirname(os.path.dirname(control_dir))
        )

        self.state_file = self.tracker.create_true_state_if_new_else_fetch_from_memory(
            TaskMockup("parent-task")
        )

        if logger is None:
            self.logger = logging.getLogger('log nothing').addHandler(logging.NullHandler())
        else:
            self.logger = logger

    def task_keys_file(self):
        return os.path.join(self.control_dir, "task-keys.tsv")

    def children_task_keys(self):
        with open(self.task_keys_file()) as f:
            for line in f:
                yield line.strip()

    def prepare_sbatch_command(self, task_key_file, array_size):

        if array_size == 0:
            raise Exception(f"should not start an empty array")
        elif array_size == 1:
            array_arg = "0"
        else:
            array_arg = f"0-{array_size - 1}"

        return [
            "sbatch",
            f"--array={array_arg}",
            f"--account={self.task_conf.slurm_account}",
            f"--output={self.control_dir}/out.log",
            f"--export={0}".format(",".join([
                f"DRYPIPE_CONTROL_DIR={self.control_dir}",
                f"DRYPIPE_TASK_KEY_FILE_BASENAME={os.path.basename(task_key_file)}",
                f"DRYPIPE_TASK_DEBUG={self.debug}"
            ])),
            "--signal=B:USR1@50",
            "--parsable",
            f"{self.control_dir}/cli"
        ]

    def call_sbatch(self, command_args):
        cmd = " ".join(command_args)
        logging.info("will launch array: %s", cmd)
        with PortablePopen(cmd, shell=True) as p:
            p.wait_and_raise_if_non_zero()
            return p.stdout_as_string().strip()

    def call_squeue(self):
        raise NotImplementedError()

    def compare_squeue_with_state_files(self):
        raise Exception("warning, scheduled tasks have been detected that have never launched")

    def mv_failed_tasks_to_ready(self):
        pass

    def launch_array(self):
        pass

    def arrays_files(self):
        return glob.glob(os.path.join(self.control_dir, "array.*.tsv"))

    def i_th_arrays_file(self, array_number):
        return os.path.join(self.control_dir, f"array.{array_number}.tsv")

    def submitted_arrays_files(self):
        return glob.glob(os.path.join(self.control_dir, "array.*.job.*"))

    def new_array_job_array_file(self, array_number, job_id):
        return os.path.join(self.control_dir, f"array.{array_number}.job.{job_id}")

    def iterate_next_task_state_files(self, start_next_n):
        i = 0
        for k in self.children_task_keys():
            state_file = self.tracker.fetch_true_state_and_update_memory_if_changed(k)
            if state_file.is_in_pre_launch():
                pass
            elif state_file.is_ready():
                yield state_file
                i += 1
            if start_next_n is not None and start_next_n >= i:
                break

    def prepare_and_launch_next_array(self, start_next_n=None, restart_failed=False):

        arrays_files = list(self.arrays_files())

        if len(arrays_files) > 0:
            last_array_file_idx = max([
                int(f.split(".")[1])
                for f in arrays_files
            ])
        else:
            last_array_file_idx = 0

        if len(arrays_files) == 0:
            next_task_key_file = os.path.join(self.control_dir, "array.0.tsv")
            next_array_number = 0
        else:
            next_array_number = last_array_file_idx + 1
            next_task_key_file = os.path.join(self.control_dir, f"array.{next_array_number}.tsv")

        if restart_failed:
            self.mv_failed_tasks_to_ready()

        next_task_state_files = list(self.iterate_next_task_state_files(start_next_n))

        if len(next_task_state_files) == 0:
            print("no more tasks to run")
        else:
            with open(next_task_key_file) as _next_task_key_file:
                for state_file in next_task_state_files:
                    _next_task_key_file.write(f"{state_file.task_key}\n")

            command_args = self.prepare_sbatch_command(next_task_key_file, len(next_task_state_files))
            job_id = self.call_sbatch(command_args)
            with open(self.new_array_job_array_file(next_array_number, job_id), "w") as f:
                f.write(" ".join(command_args))



class SlurmArrayLaunchTest(TestWithDirectorySandbox):

    def test_launch_slurm_array(self):
        self.pipeline_instance_dir


if __name__ == '__main__':
    s = SlurmArrayScenario(None)

    s.test()
