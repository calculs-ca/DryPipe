from datetime import timedelta
import glob
import json
import logging
import math
import os
import subprocess
from itertools import groupby
from pathlib import Path

from dry_pipe import PortablePopen, TaskConf
from dry_pipe.slurm_codes import SlurmJobStateLongCodes, SlurmJobStateCodes, SlurmTime
from dry_pipe.state_file_tracker import StateFileTracker

def dedent_lines(txt):
    return "\n".join([
        line.strip()
        for line in txt.split("\n")
    ])


class SAcctParser:

    def invoke(self, job_id, fake_outputs=None, logger=None):

        sacct_output = None
        if fake_outputs is not None:
            sacct_output = fake_outputs.get(job_id)
            if sacct_output is None:
                raise Exception(f'No fake sacct output for job_id {job_id}')
            sacct_output = dedent_lines(sacct_output)
        else:

            cmd = f'sacct -n --format="JobId,State,JobName,ExitCode,Submit,Start,End" --parsable -j {job_id}'

            if logger is not None:
                logger.debug(f'command: %s', cmd)

            subprocess_result = subprocess.run(cmd, capture_output=True, shell=True, text=True)
            sacct_output = subprocess_result.stdout

        def g():
            for line in sacct_output.split("\n"):
                line = line.strip()
                if line == "":
                    continue
                job_id, long_code_state, job_name, exit_code, submit_time, start_time, end_time, empty = line.split("|")
                assert empty == ""
                if job_name in {"batch", "extern", "allocation"}:
                    continue

                yield SAcctRow(line, job_id, long_code_state, job_name, exit_code, submit_time, start_time, end_time)

        return sacct_output, list(g())


class SQueueParser:
    """
    substitute parse for when sacct is not available, the output is almost identical, this parsers should
    produce SAcctRow that are identical to what would be produced by SAcctParser

    squeue --noheader --format="%i|%T|%j|0:0|" -j 9666
    9666_[5-100]|PENDING|sleep.sh|0:0|
    9666_4|RUNNING|sleep.sh|0:0|
    9666_3|RUNNING|sleep.sh|0:0|
    9666_2|RUNNING|sleep.sh|0:0|
    9666_1|RUNNING|sleep.sh|0:0|
    """

    def invoke(self, job_id, fake_outputs=None, logger=None):

        squeue_output = None
        if fake_outputs is not None:
            squeue_output = fake_outputs.get(job_id)
            if squeue_output is None:
                raise Exception(f'No fake squeue output for job_id {job_id}')
            squeue_output = dedent_lines(squeue_output)
        else:
                               # JOBID|STATE|NAME|0:0|SUBMIT_TIME|START_TIME|END_TIME|
            with PortablePopen(f'squeue --noheader --format="%i|%T|%j|0:0|%V|%S|%e|" -j {job_id}', shell=True) as p:
                p.wait_and_raise_if_non_zero()
                squeue_output = p.stdout_as_string()

        def g():
            for line in squeue_output.split("\n"):
                line = line.strip()
                if line == "":
                    continue
                job_id, long_code_state, job_name, submit_time, start_time, end_time, empty = line.split("|")
                assert empty == ""
                yield SAcctRow(line, job_id, long_code_state, job_name, None, submit_time, start_time, end_time, origin="squeue")

        return squeue_output, list(g())


class SyntheticSAcctRow:
    def __init__(self, task_key, long_code_state, dry_pipe_state):
        self.task_key = task_key
        self.long_code_state = long_code_state
        self.dry_pipe_state = dry_pipe_state
        self.dry_pipe_step_number = None

    def timestamp_for_state_override_priority(self):
        return "9999-11-31T00:23:59"
    
    def full_dump(self):
        return f"{self.task_key}:{self.long_code_state}"

class SAcctRow:

    def __init__(self, acct_line, job_id, long_code_state, job_name, exit_code_and_signal, submit_time, start_time, end_time, origin="sacct"):
        self.acct_line = acct_line
        self.exit_code_and_signal = exit_code_and_signal
        self.long_code_state = long_code_state
        self.array_child_idx = None
        self.job_name = job_name
        self.task_key = None
        self.array_range = None
        self.dry_pipe_state = None
        self.dry_pipe_step_number = None
        self.child_job_id = None
        self.submit_time = submit_time
        self.start_time = start_time
        self.end_time = end_time

        if "_" in job_id:
            self.is_array_task = True
            a_job_id, array_idx_or_range = job_id.split("_")
            self.job_id = a_job_id
            if not job_id.endswith("]"):
                self.array_child_idx = int(array_idx_or_range)
            else:
                self.is_pending_range = True
                array_idx_or_range = array_idx_or_range[1:-1]
                if "-" in array_idx_or_range:
                    min, max = array_idx_or_range.split("-")
                    self.array_range = range(int(min), int(max) + 1)
                else:
                    i = int(array_idx_or_range)
                    self.array_range = range(i, i + 1)
        else:
            self.job_id = job_id

        if job_name != "cli":

            if ":" not in job_name:
                raise Exception(f"unexpected job name {job_name}, line: {self.acct_line} origin: {origin}")

            if ">" in job_name:
                job_name, self.child_job_id = job_name.split(">")

            self.task_key, self.dry_pipe_state = job_name.split(":")

            if "." in self.dry_pipe_state:
                self.dry_pipe_step_number = int(self.dry_pipe_state.split(".")[1])

    def timestamp_for_state_override_priority(self):
        if self.long_code_state == "PENDING":
            return self.submit_time
        elif self.long_code_state == "CANCELLED":
            return self.end_time
        else:
            for t in [self.end_time, self.start_time, self.submit_time]:
                if t is not None and t != "Unknown" and t != "NONE" and t != "N/A":
                    return t
        raise Exception(f"can't assign timestamp to: {self.acct_line}")

    def is_child_task(self):
        return self.array_child_idx is not None

    def is_pending_array_range(self):
        return self.array_range is not None

    def __str__(self):
        k = "?" if self.task_key is None else self.task_key
        return f"Task(key={k}, {self.long_code_state}, {self.job_id}, {self.dry_pipe_state})"

    def full_dump(self):
        def g():
            yield f"array_range={self.array_range}"

        s = ",".join(g())

        return f"{self.__str__()}[{s}]"

class ArraySubmitInfo:
    """
    array submit files are named as follows:

        array.<array_submit_idx>.tsv
        array.<array_submit_idx>.job.<job_id>

    where array_submit_idx is a number from 0,1,... N representing the ith array submission,
    """

    def __init__(self, submit_array_idx, job_id, submit_file, logger, sacct_parser):

        self.array_submit_file_name = Path(submit_file).name

        assert self.array_submit_file_name == f"array.{submit_array_idx}.job.{job_id}"

        self.sacct_parser = sacct_parser
        self.submit_array_idx = submit_array_idx
        self.job_id = job_id
        self.submit_file = submit_file
        self.array_keys_file = Path(submit_file).parent.joinpath(f"array.{submit_array_idx}.tsv")
        self.is_provably_ended = False
        self.logger = logger
        self.array_keys = []
        self.pending_child_keyes = set([])
        self.sacct_output = None
        self.sacct_rows = None

        self.sacct_logs_files_sequence = SequenceOfFiles(
            "array.{0}.job.{1}.{2}.sacct.out".format(submit_array_idx, job_id, "{0}"),
            self.array_keys_file.parent,
            lambda f: f.split(".")[4]
        )

        def g():
            with open(self.array_keys_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line != "":
                        yield line
        self.task_keys_per_array_index = list(g())

    def invoke_sacct(self, fake_outputs=None):
        self.sacct_output, self.sacct_rows = self.sacct_parser.invoke(self.job_id, fake_outputs, logger=self.logger)
        file, n = self.sacct_logs_files_sequence.next_file_and_number()

        # don't save sacct output, if it's the same as previous
        if n > 0:
            prev_file = self.sacct_logs_files_sequence.file_name(n - 1)
            if Path(prev_file).exists():
                with open(prev_file, "r") as f:
                    prev_output = f.read()
                    if prev_output == self.sacct_output:
                        self.logger.debug("sacct identical for job_id %s, won't save", self.job_id)
                        return

        with open(file, "w") as f:
            f.write(self.sacct_output)


class SequenceOfFiles:

    def __init__(self, name_template, directory, index_from_file_name):
        self.name_template = name_template
        self.directory = directory
        self.index_from_file_name = index_from_file_name

    def next_file_and_number(self):
        files = list(self.list_files())

        if len(files) == 0:
            next_number = 0
        else:
            last_file_idx = files[-1][0]
            next_number = last_file_idx + 1

        return self.file_name(next_number), next_number

    def file_name(self, number):
        return os.path.join(self.directory, self.name_template.format(number))

    def list_files(self):

        def gen():
            for f in glob.glob(os.path.join(self.directory, self.name_template.format("*"))):
                b = os.path.basename(f)
                idx = int(self.index_from_file_name(b))
                yield idx, f

        yield from sorted(gen(), key= lambda t: t[0])



class SlurmArrayBatchSubmit:

    def __init__(self, array_task_manager, pre_submit_func, sbatch_command, post_submit_func, task_keys):
        self.array_task_manager = array_task_manager
        self.sbatch_command = sbatch_command
        self.pre_submit_func = pre_submit_func
        self.post_submit_func = post_submit_func
        self.task_keys = task_keys

    def invoke(self, fake_job_id=None):
        
        self.pre_submit_func()

        if fake_job_id is not None:
            job_id = fake_job_id
        else:
            job_id = None
            if self.array_task_manager.for_dry_run:
                print(f"DRY RUN inhibited command: {' '.join(self.sbatch_command)}")
            else:
                with PortablePopen(self.sbatch_command) as p:
                    p.wait_and_raise_if_non_zero()
                    job_id = p.stdout_as_string().strip()

        if not self.array_task_manager.for_dry_run:
            self.post_submit_func(job_id)

class ArrayTaskManager:

    def __init__(self, task_process, auto_restart_manager=None, sacct_parser=SAcctParser(), for_dry_run=False, slurm_max_jobs=None):
        self.task_process = task_process
        self.arrays_submitted_sacct_info = None
        self.child_task_sacct_rows = None
        self.last_sacct_row_per_task_key = dict([])
        self.array_files_sequence = SequenceOfFiles(
            "array.{0}.tsv",
            self.array_task_control_dir(),
            lambda f: f.split(".")[1]
        )
        self.auto_restart_manager = auto_restart_manager
        self.sacct_parser=sacct_parser
        self.for_dry_run = for_dry_run

        if slurm_max_jobs is not None:
            int(slurm_max_jobs)

        self.slurm_max_jobs = slurm_max_jobs
        

    def next_array_file_name_and_number(self):
        return self.array_files_sequence.next_file_and_number()

    def gen_last_sacct_row_per_task_key(self):

        def g():

            for array_info in self.arrays_submitted_sacct_info:
                for sacct_row in array_info.sacct_rows:
                    if sacct_row.is_pending_array_range():
                        for idx in sacct_row.array_range:
                            yield array_info.task_keys_per_array_index[idx], sacct_row

            # sacct rows that have child indexes, override synthetic rows ^
            for array_info in self.arrays_submitted_sacct_info:
                for sacct_row in array_info.sacct_rows:
                    if sacct_row.is_child_task():
                        task_key = array_info.task_keys_per_array_index[sacct_row.array_child_idx]
                        yield task_key, sacct_row

            for child_task_sacct_row in self.child_task_sacct_rows:
                yield child_task_sacct_row.task_key, child_task_sacct_row

            if isinstance(self.sacct_parser, SQueueParser):
                self.logger().warning("Using SQueueParser")
                for task_key in self.children_task_keys():
                    state_file = self.find_state_file_for_task_key(task_key)
                    if state_file.is_failed():
                        yield task_key, SyntheticSAcctRow(task_key, SlurmJobStateCodes.FAILED.long_code, state_file.state())
                    elif state_file.is_completed():
                        yield task_key, SyntheticSAcctRow(task_key, SlurmJobStateCodes.COMPLETED.long_code, state_file.state())
                    elif state_file.is_timed_out():
                        yield task_key, SyntheticSAcctRow(task_key, SlurmJobStateCodes.TIMEOUT.long_code, state_file.state())

        def k(t):
            return t[0]

        for task_key, sacct_rows in  groupby(sorted(g(), key=k), key=k):
            last_sacct_row = sorted(
                sacct_rows,
                key=lambda r: r[1].timestamp_for_state_override_priority()
            )[-1][1]
            yield task_key, last_sacct_row


    def list_array_states(self):
        pwd = self.pipeline_work_dir()
        for task_key in self.children_task_keys():
            state_file_path = StateFileTracker.find_state_file_path_if_exists(os.path.join(pwd, task_key))
            yield task_key, state_file_path.name

    def invoke_sacct(self, fake_sacct_outputs=None):


        self.arrays_submitted_sacct_info = [
            ArraySubmitInfo(array_n, job_id, f, self.logger(), self.sacct_parser)
            for array_n, job_id, f in self.submitted_arrays_files()
        ]

        self.logger().debug("task has %s submitted_arrays_files", len(self.arrays_submitted_sacct_info))

        for array_info in self.arrays_submitted_sacct_info:
            array_info.invoke_sacct(fake_sacct_outputs)

        def gen_child_job_ids():
            for array_info in self.arrays_submitted_sacct_info:
                for sacct_row in array_info.sacct_rows:
                    if sacct_row.child_job_id is not None:
                        yield sacct_row.child_job_id.strip()

        child_job_ids = list(gen_child_job_ids())

        try:
            if len(child_job_ids) > 0:
                p = SAcctParser()
                sacct_output, rows = p.invoke(",".join(child_job_ids), logger=self.logger())
                self.child_task_sacct_rows = rows

                self.logger().info("array has spawned %s non array jobs", len(child_job_ids))

                if self.is_log_level_debug():
                    for r in self.child_task_sacct_rows:
                        self.logger().debug(r.__str__())

            else:
                self.child_task_sacct_rows = []

            self.last_sacct_row_per_task_key = dict(self.gen_last_sacct_row_per_task_key())
        except Exception as e:
            self.logger().error("Failed to invoke sacct due to error: %s", e)


    def last_sact_state_code_by_task_keys(self):
        return {k: r.long_code_state for k, r in self.last_sacct_row_per_task_key.items()}

    def logger(self):
        return self.task_process.task_logger

    def is_log_level_debug(self):
        return self.task_process.is_task_logger_debug_level()

    def array_task_control_dir(self):
        return self.task_process.control_dir

    def task_control_dir_of(self, task_key):
        return self.pipeline_work_dir().joinpath(task_key)

    def pipeline_work_dir(self):
        return Path(self.array_task_control_dir()).parent

    def i_th_submitted_array_file(self, array_number, job_id):
        return os.path.join(self.array_task_control_dir(), f"array.{array_number}.job.{job_id}")

    def submitted_arrays_files(self):

        def gen():
            for f in glob.glob(os.path.join(self.array_task_control_dir(), "array.*.job.*")):
                if f.endswith(".out"):
                    continue
                b = os.path.basename(f)
                _, array_n, _, job_id = b.split(".")
                array_n = int(array_n.strip())
                yield array_n, job_id, f

        yield from sorted(gen(), key=lambda t: t[0])

    def task_keys_file(self):
        return os.path.join(self.array_task_control_dir(), "task-keys.tsv")

    def _children_task_keys(self):
        with open(self.task_keys_file()) as f:
            for line in f:
                line = line.strip()
                if line != "":
                    yield line

    def sample_child_task_conf(self):
        def pick_first():
            for task_key in self._children_task_keys():
                return task_key
            raise Exception(f"No task key found in {self.task_keys_file()}")

        sample_child_task_key = pick_first()

        return TaskConf.from_json_file(self.task_control_dir_of(sample_child_task_key))

    def children_task_keys(self):
        return set(self._children_task_keys())


    def array_task_conf(self):
        return self.task_process.task_conf

    def find_state_file_for_task_key(self, task_key):
        return StateFileTracker.find_state_file_if_exists(self.pipeline_work_dir(), task_key)
    
    def _slurm_max_concurrent_jobs_specifier(self):
        if self.slurm_max_jobs is None:
            return ""
        else:
            return f"%{self.slurm_max_jobs}"

    def prepare_sbatch_command(self, task_key_file, array_size, sbatch_options):

        if array_size == 0:
            raise Exception(f"should not start an empty array")
        elif array_size == 1:
            array_arg = "0"
        else:
            array_arg = f"0-{array_size - 1}{self._slurm_max_concurrent_jobs_specifier()}"

        def sbatch_lines():
            yield "sbatch"
            yield f"--array={array_arg}"
            a = self.array_task_conf().slurm_account
            if a is not None:
                yield f"--account={a}"

            yield f"--output={self.array_task_control_dir()}/launch-%A_%a.out"

            for o in sbatch_options:
                if self.task_process.packed_job_size is None:
                    yield o
                elif not o.startswith("--time="):
                    yield o
                else:
                    _, t = o.split("=")
                    slurm_time = SlurmTime(t)
                    scaled_time = slurm_time * self.task_process.packed_job_size
                    self.logger().info(f"packed array, walltime is {self.task_process.packed_job_size} times longer, {slurm_time} becomes: {scaled_time}")
                    yield f"--time={scaled_time}"

            def gen_env():
                yield f"DRYPIPE_TASK_CONTROL_DIR={self.array_task_control_dir()}"
                yield f"DRYPIPE_TASK_KEY_FILE_BASENAME={os.path.basename(task_key_file)}"
                yield f"DRYPIPE_TASK_DEBUG={self.task_process.is_debug()}"
                if self.task_process.packed_job_size is not None:
                    yield f"DRYPIPE_PACKED_JOB_SIZE={self.task_process.packed_job_size}"

            yield "--export={0}".format(",".join(gen_env()))

            if self.task_process is not None and self.task_process.wait_for_completion:
                yield "--wait"

            yield "--signal=B:USR1@50"
            yield "--parsable"
            yield f"{self.pipeline_work_dir()}/cli"

        return list(sbatch_lines())

    def group_by_sbatch_options(self, task_keys):

        step_invocations_of_children_tasks = self.sample_child_task_conf().step_invocations

        def get_sbatch_options_or_none(step_invocation):
            so = step_invocation.get("sbatch_options")
            if so == "":
                return None
            return so

        sbatch_options_by_step_number = [
            get_sbatch_options_or_none(step)
            for step in step_invocations_of_children_tasks
        ]

        assert sbatch_options_by_step_number[0] is None

        sbatch_options_by_step_number[0] = self.array_task_conf().sbatch_options

        if self.logger().isEnabledFor(logging.DEBUG):
            self.logger().debug(f"sbatch_options_by_step_number: {sbatch_options_by_step_number}")

        def last_sbatch_option_idx(step_number):
            for i in range(step_number, 0, -1):
                sbo = sbatch_options_by_step_number[i]
                if sbo is not None:
                    return i

            return 0

        def g():
            for task_key in task_keys:
                sacct_row = self.last_sacct_row_per_task_key.get(task_key)
                if sacct_row is None:
                    yield 0, task_key
                elif sacct_row.dry_pipe_step_number is None:
                    yield 0, task_key
                else:
                    yield last_sbatch_option_idx(sacct_row.dry_pipe_step_number), task_key

        def key(t):
            return t[0]

        res = [
            (sbatch_options_by_step_number[sbo_idx],  set([t[1] for t in sbo_idx_task_keys]))
            for sbo_idx, sbo_idx_task_keys in groupby(sorted(g(), key=key), key=key)
        ]

        if self.logger().isEnabledFor(logging.DEBUG):
            for sbatch_opts, keys in res:
                self.logger().debug(f"{len(keys)} with sbatch_options: {sbatch_opts}  {len(keys)}")

        return res


    def task_keys_for_next_batch(self, restart_failed=False, include_all_incompleted=False):

        self.logger().debug(f"Task has auto-restart manager: %s ", self.auto_restart_manager is not None)


        def g():

            self.logger().debug("TASKS INCLUDED OR EXCLUDED in next submit")

            for task_key in self.children_task_keys():
                sacct_row = self.last_sacct_row_per_task_key.get(task_key)
                if sacct_row is None:
                    #never launched
                    self.logger().debug(f"INCLUDED %s\tnever launched", task_key)
                    yield task_key
                    continue

                if sacct_row.dry_pipe_state == "completed":
                    self.logger().debug(f"EXCLUDED %s\t all steps completed", task_key)
                    continue

                if SlurmJobStateLongCodes.pending_or_running(sacct_row.long_code_state):
                    self.logger().debug(f"EXCLUDED %s\t pending or running", task_key)
                    continue

                if SlurmJobStateLongCodes.is_canceled(sacct_row.long_code_state):
                    self.logger().debug(f"INCLUDED %s\tcanceled", task_key)
                    yield task_key
                    continue

                is_failed = False

                if sacct_row.dry_pipe_state is not None and sacct_row.dry_pipe_state.startswith("failed"):
                    is_failed = True

                if SlurmJobStateLongCodes.has_failed(sacct_row.long_code_state):
                    is_failed = True

                if is_failed:
                    def state_file_if_restart():
                        if restart_failed:
                            return self.find_state_file_for_task_key(task_key)

                        if self.auto_restart_manager is not None:
                            state_file = self.find_state_file_for_task_key(task_key)
                            if self.auto_restart_manager.should_restart(state_file, logger=self.logger()):
                                return state_file
                        return None

                    state_file_for_restart = state_file_if_restart()
                    if state_file_for_restart is not None:
                        if not self.for_dry_run:
                            StateFileTracker.transition_to_pre_launch(state_file_for_restart)
                        self.logger().debug(f"INCLUDED %s\t failed, and restart_failed is %s ", task_key, restart_failed)
                        yield task_key
                        continue

                if include_all_incompleted:
                    self.logger().debug(f"INCLUDED %s\t matches no criteria for inclusion, state: %s", task_key,
                                        sacct_row.full_dump())
                    yield task_key
                else:
                    self.logger().debug(f"EXCLUDED %s\t matches no criteria for inclusion, state: %s", task_key, sacct_row.full_dump())


        return set(g())

    def filter_task_keys(self, long_code_state_func):
        for task_key, sacct_row in self.last_sacct_row_per_task_key.items():
            if long_code_state_func(sacct_row.long_code_state):
                yield task_key

    def active_tasks(self):
        return list(self.filter_task_keys(SlurmJobStateLongCodes.pending_or_running))

    def failed_cancelled_timedout_tasks(self):
        def g():
            for task_key, sacct_row in self.last_sacct_row_per_task_key.items():
                if sacct_row.dry_pipe_state is not None and sacct_row.dry_pipe_state.startswith("failed"):
                    yield task_key
                elif SlurmJobStateLongCodes.has_failed(sacct_row.long_code_state):
                    yield task_key
        return list(g())

    def completed_tasks(self):
        def g():
            for task_key, sacct_row in self.last_sacct_row_per_task_key.items():
                if sacct_row.dry_pipe_state == "completed":
                    yield task_key
        return list(g())


    def next_submits(self, restart_failed=False, include_all_incompleted=False, set_of_task_keys=None, sbatch_option_overrider=lambda o: o):

        if set_of_task_keys is None:
            next_task_keys = self.task_keys_for_next_batch(restart_failed, include_all_incompleted)
        else:
            next_task_keys = set_of_task_keys

        self.logger().info(f"%s tasks in next sbatch submit", len(next_task_keys))

        self.logger().info(f"%s", next_task_keys)        

        def g():

            sbatch_groups = list(self.group_by_sbatch_options(next_task_keys))

            if len(sbatch_groups) > 1 and self.task_process.packed_job_size is not None:
                raise Exception(f"--packed-job-size is not compatible with multi sbatch groups")

            for sbatch_options, task_keys in sbatch_groups:            

                self.logger().info(f"sbatch_options: {sbatch_options}")

                next_task_key_file, next_array_number = self.next_array_file_name_and_number()
                task_keys_for_saving = sorted(task_keys)
                def pre_submit_func():

                    if self.for_dry_run:
                        ntkf = Path("/tmp", Path(next_task_key_file).name)
                        print(f"DRY RUN: {ntkf.absolute()}")
                    else:
                        ntkf = next_task_key_file

                    self.logger().info("next array task keys in %s", ntkf)

                    
                    with open(ntkf, "w") as _next_task_key_file:
                        for task_key in task_keys_for_saving:
                            _next_task_key_file.write(f"{task_key}\n")

                tasks_in_batch = len(task_keys)

                if self.task_process.packed_job_size is None:                    
                    array_size = tasks_in_batch
                else:                    
                    array_size = math.ceil(tasks_in_batch / self.task_process.packed_job_size)
                    

                command_args = self.prepare_sbatch_command(
                    next_task_key_file, array_size, sbatch_options
                )

                def post_submit_func(job_id):
                    if not self.for_dry_run:
                        self.logger().info("array job id: %s", job_id)
                        with open(self.i_th_submitted_array_file(next_array_number, job_id), "w") as f:
                            f.write(" ".join(command_args))

                yield SlurmArrayBatchSubmit(self, pre_submit_func, command_args, post_submit_func, task_keys)

        return list(g())

    def manage_auto_restarts_from_remote(self):

        self.invoke_sacct()

        launch_count_this_round = 0

        for submit in self.next_submits():

            submit.invoke()
            launch_count_this_round += len(submit.task_keys)

            #else:
            #    c = len(submit.task_keys)
            #    self.logger().info("DRY RUN mode, would have launched %s tasks otherwise", c)
            #    if self.is_log_level_debug():
            #        self.logger().debug("not lauched tasks (because of DRY RUN): %s", submit.task_keys)

        res = {
            "launch_count_this_round": launch_count_this_round,
            "total_children_tasks": len(self.children_task_keys()),
            "active_tasks": len(self.active_tasks()),
            "completed_tasks": len(self.completed_tasks()),
            "failed_cancelled_timedout_tasks": len(self.failed_cancelled_timedout_tasks())
        }

        if self.for_dry_run:
            self.logger().info("DRY run: %s", json.dumps(res))

        return res