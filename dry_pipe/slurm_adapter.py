from dataclasses import dataclass
import glob
import logging
import os
from typing import Iterator, Tuple, List

from dry_pipe import PortablePopen


@dataclass
class InvalidStateError(Exception):
    drypipe_state_as_string: str
    squeue_state: str

class SlurmArrayParentTask:

    def __init__(self, task_key, tracker, task_conf, logger=None):
        self.task_key = task_key
        self.debug = True
        self.task_conf = task_conf
        self.tracker = tracker
        if logger is None:
            self.logger = logging.getLogger('log nothing')
            self.logger.addHandler(logging.NullHandler())
        else:
            self.logger = logger

    def control_dir(self):
        return os.path.join(self.tracker.pipeline_work_dir,  self.task_key)

    def task_keys_file(self):
        return os.path.join(self.control_dir(),  "task-keys.tsv")

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
            f"--output={self.control_dir()}/out.log",
            f"--export={0}".format(",".join([
                f"DRYPIPE_CONTROL_DIR={self.control_dir()}",
                f"DRYPIPE_TASK_KEY_FILE_BASENAME={os.path.basename(task_key_file)}",
                f"DRYPIPE_TASK_DEBUG={self.debug}"
            ])),
            "--signal=B:USR1@50",
            "--parsable",
            f"{self.control_dir()}/cli"
        ]

    def call_sbatch(self, command_args):
        cmd = " ".join(command_args)
        logging.info("will launch array: %s", cmd)
        with PortablePopen(cmd, shell=True) as p:
            p.wait_and_raise_if_non_zero()
            return p.stdout_as_string().strip()

    def call_squeue(self, submitted_job_ids):
        job_ids_as_str = ",".join(list(submitted_job_ids))
        # see JOB STATE CODES: https://slurm.schedmd.com/squeue.html
        with PortablePopen(f'squeue -r --format="%i %t" --states=all --jobs={job_ids_as_str}', shell=True) as p:
            p.wait_and_raise_if_non_zero()
            for line in p.iterate_stdout_lines():
                yield line

    def call_squeue_and_index_response(self, submitted_job_ids, mockup_squeue_call=None) -> dict[str,dict[int,str]]:

        res: dict[str,dict[int,str]] = {}

        if mockup_squeue_call is None:
            squeue_func = lambda: self.call_squeue(submitted_job_ids)
        else:
            squeue_func = lambda: mockup_squeue_call(submitted_job_ids)

        for line in squeue_func():
            line = line.strip()
            job_id_array_idx, squeue_state = line.split()
            job_id, array_idx = job_id_array_idx.split("_")

            array_idx_to_state_dict = res.get(job_id)
            if array_idx_to_state_dict is None:
                array_idx_to_state_dict = {}
                res[job_id] = array_idx_to_state_dict

            array_idx = int(array_idx)
            array_idx_to_state_dict[array_idx] = squeue_state
        return res

    def submitted_job_ids_to_tuple_array_idx_task_keys(self, ended_job_ids) \
            -> Iterator[Tuple[str, Iterator[Tuple[int, str]]]]:
        """
        :return: job_id -> list_of_task_keys_indexed_by_array_idx
        """
        for array_n, job_id, _ in self.submitted_arrays_files():
            if job_id in ended_job_ids:
                continue
            def gen():
                with open(self.i_th_array_file(array_n)) as f:
                    line_number = 0
                    for line in f:
                        task_key = line.strip()
                        yield line_number, task_key
                        line_number += 1
            yield job_id, gen()

    def submitted_job_ids(self) -> set[str]:
        return set({
            job_id
            for _, job_id, _ in self.submitted_arrays_files()
        })

    def mock_compare_and_reconcile_squeue_with_state_files(self, mock_squeue_lines: List[str]):
        def mockup_squeue_call(submitted_job_ids):
            yield from mock_squeue_lines

        return self.compare_and_reconcile_squeue_with_state_files(mockup_squeue_call)

    def compare_and_reconcile_squeue_with_state_files(self, mockup_squeue_call=None)-> dict[str, Tuple[str, str, str]]:

        dict_unexpected_states = {}

        submitted_job_ids: set[str] = self.submitted_job_ids()

        indexed_squeue_response: dict[str, dict[int,str]] = \
            self.call_squeue_and_index_response(submitted_job_ids, mockup_squeue_call)

        ended_job_ids: set[str] = submitted_job_ids - indexed_squeue_response.keys()

        for job_id, iter_array_idx_task_keys in self.submitted_job_ids_to_tuple_array_idx_task_keys(ended_job_ids):

            for array_idx, task_key in iter_array_idx_task_keys:

                squeue_state = None
                squeue_results_for_job_id = indexed_squeue_response.get(job_id)
                if squeue_results_for_job_id is not None:
                    squeue_state = squeue_results_for_job_id.get(array_idx)

                incoherent_states = self.compare_and_reconcile_task_state_file_with_squeue_state(
                    task_key,
                    squeue_state
                )
                if incoherent_states is not None:
                    dict_unexpected_states[task_key] = incoherent_states
                    drypipe_state_as_string, expected_squeue_state, actual_queue_state = incoherent_states
                    if actual_queue_state is None:
                        actual_queue_state = "nothing"
                    self.logger.warning(
                        "task %s in state %s and squeue returned %",
                        task_key,
                        drypipe_state_as_string,
                        actual_queue_state
                    )

        return dict_unexpected_states

    def fetch_state_file(self, task_key):
        _, state_file = self.tracker.fetch_true_state_and_update_memory_if_changed(task_key)
        return state_file

    def compare_and_reconcile_task_state_file_with_squeue_state(self, task_key, squeue_state):
        state_file = self.fetch_state_file(task_key)
        drypipe_state_as_string = state_file.state_as_string()
        if "." in drypipe_state_as_string:
            drypipe_state_as_string = drypipe_state_as_string.split(".")[0]

        # see JOB STATE CODES at https://slurm.schedmd.com/squeue.html

        if drypipe_state_as_string in ["completed", "failed", "killed", "timed-out"]:
            if squeue_state is not None:
                return drypipe_state_as_string, None, drypipe_state_as_string
        elif drypipe_state_as_string in ["ready", "waiting"]:
            if squeue_state is not None:
                return drypipe_state_as_string, None, drypipe_state_as_string
        elif drypipe_state_as_string.endswith("_step-started"):
            if squeue_state is None:
                state_file.transition_to_crashed()
                return drypipe_state_as_string,  "R,PD", None
            elif squeue_state not in ["R", "PD"]:
                return drypipe_state_as_string, "R,PD", squeue_state
        elif drypipe_state_as_string.endswith("state.step-started"):
            if squeue_state is None:
                state_file.transition_to_crashed()
                return drypipe_state_as_string,  "R,PD", None
            elif squeue_state not in ["R", "PD"]:
                return drypipe_state_as_string, "R,PD", squeue_state


    def arrays_files(self) -> Iterator[Tuple[int, str]]:

        def gen():
            for f in glob.glob(os.path.join(self.control_dir(), "array.*.tsv")):
                b = os.path.basename(f)
                idx = b.split(".")[1]
                idx = int(idx)
                yield idx, f

        yield from sorted(gen(), key= lambda t: t[0])

    def i_th_array_file(self, array_number):
        return os.path.join(self.control_dir(), f"array.{array_number}.tsv")

    def submitted_arrays_files(self) -> Iterator[Tuple[int, str, str]]:
        def gen():
            for f in glob.glob(os.path.join(self.control_dir(), "array.*.job.*")):
                b = os.path.basename(f)
                _, array_n, _, job_id = b.split(".")
                array_n = int(array_n.strip())
                yield array_n, job_id, f

        yield from sorted(gen(), key=lambda t: t[0])


    def i_th_submitted_array_file(self, array_number, job_id):
        return os.path.join(self.control_dir(), f"array.{array_number}.job.{job_id}")

    def iterate_next_task_state_files(self, start_next_n, restart_failed):
        i = 0
        for k in self.children_task_keys():
            state_file = self.tracker.load_state_file(k)
            if state_file.is_in_pre_launch():
                pass
            elif state_file.is_ready():
                yield state_file
                i += 1
            elif restart_failed and (state_file.is_failed() or state_file.is_timed_out()):
                self.tracker.register_pre_launch(state_file)
                yield state_file
                i += 1
            if start_next_n is not None and start_next_n >= i:
                break

    def prepare_and_launch_next_array(self, start_next_n=None, restart_failed=False, call_sbatch_mockup=None):

        arrays_files = list(self.arrays_files())

        if len(arrays_files) == 0:
            next_array_number = 0
        else:
            last_array_file_idx = arrays_files[-1][0]
            next_array_number = last_array_file_idx + 1

        next_task_key_file = os.path.join(self.control_dir(), f"array.{next_array_number}.tsv")

        next_task_state_files = list(self.iterate_next_task_state_files(start_next_n, restart_failed))

        if len(next_task_state_files) == 0:
            return 0
        else:
            with open(next_task_key_file, "w") as _next_task_key_file:
                for state_file in next_task_state_files:
                    _next_task_key_file.write(f"{state_file.task_key}\n")
                    self.tracker.register_pre_launch(state_file)

            command_args = self.prepare_sbatch_command(next_task_key_file, len(next_task_state_files))

            if call_sbatch_mockup is not None:
                call_sbatch_func = call_sbatch_mockup
            else:
                call_sbatch_func = lambda: self.call_sbatch(command_args)

            job_id = call_sbatch_func()
            if job_id is None:
                raise Exception(f"sbatch returned None:\n {command_args}")
            with open(self.i_th_submitted_array_file(next_array_number, job_id), "w") as f:
                f.write(" ".join(command_args))

            return len(next_task_state_files)
