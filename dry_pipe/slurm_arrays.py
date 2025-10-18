import glob
import os
from itertools import groupby
from pathlib import Path

from dry_pipe import PortablePopen, TaskConf
from dry_pipe.slurm_array_task import AutoRestartManager
from dry_pipe.slurm_codes import SlurmJobStateLongCodes
from dry_pipe.state_file_tracker import StateFileTracker


class SAcctParser:

    def invoke(self, job_id, fake_outputs=None):

        sacct_output = None
        if fake_outputs is not None:
            sacct_output = fake_outputs.get(job_id)
            if sacct_output is None:
                raise Exception(f'No fake sacct output for job_id {job_id}')
        else:
            with PortablePopen(f'sacct --format="JobId,State,JobName,ExitCode" --parsable -j {job_id}') as p:
                p.wait_and_raise_if_non_zero()
                sacct_output = p.stdout_as_string()

        def g():
            for line in sacct_output.split("\n"):
                line = line.strip()
                if line == "":
                    continue
                job_id, end, long_code_state, job_name, empty = line.split("|")
                assert empty == ""
                if job_name in {"batch", "extern"}:
                    continue

                yield SAcctRow(line, job_id, end, long_code_state, job_name)

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

    def invoke(self, job_id, fake_outputs=None):

        squeue_output = None
        if fake_outputs is not None:
            squeue_output = fake_outputs.get(job_id)
            if squeue_output is None:
                raise Exception(f'No fake squeue output for job_id {job_id}')
        else:
            with PortablePopen(f'squeue --noheader --format="%i|%T|%j|0:0|" -j {job_id}') as p:
                p.wait_and_raise_if_non_zero()
                squeue_output = p.stdout_as_string()

        def g():
            for line in squeue_output.split("\n"):
                line = line.strip()
                if line == "":
                    continue
                job_id, end, long_code_state, job_name, empty = line.split("|")
                assert empty == ""
                yield SAcctRow(line, job_id, end, long_code_state, job_name)

        return squeue_output, list(g())


class SAcctRow:

    def __init__(self, acct_line, job_id, long_code_state, job_name, exit_code_and_signal):
        self.acct_line = acct_line
        self.exit_code_and_signal = exit_code_and_signal
        self.long_code_state = long_code_state
        self.array_child_idx = None
        self.job_name = job_name
        self.task_key = None
        self.array_range = None
        self.dry_pipe_state = None
        self.dry_pipe_step_number = None

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
            self.task_key, self.dry_pipe_state = job_name.split(":")
            if "." in self.dry_pipe_state:
                self.dry_pipe_step_number = int(self.dry_pipe_state.split(".")[1])


    def is_child_task(self):
        return self.array_child_idx is not None

    def is_pending_array_range(self):
        return self.array_range is not None

    def __str__(self):
        k = "?" if self.task_key is None else self.task_key
        return f"Task(key={k}, {self.long_code_state}, {self.job_id})"


class ArraySubmitInfo:
    """
    array submit files are named as follows:

        array.<array_submit_idx>.tsv
        array.<array_submit_idx>.job.<job_id>

    where array_submit_idx is a number from 0,1,... N representing the ith array submission,
    """

    def __init__(self, submit_array_idx, job_id, submit_file, logger, sacct_parser=SAcctParser()):

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
            self.array_keys_file.parent
        )

        def g():
            with open(self.array_keys_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line != "":
                        yield line
        self.task_keys_per_array_index = list(g())

    def invoke_sacct(self, fake_outputs=None):
        self.sacct_output, self.sacct_rows = self.sacct_parser.invoke(self.job_id, fake_outputs)
        file, _ = self.sacct_logs_files_sequence.next_file_and_number()
        with open(file, "w") as f:
            f.write(self.sacct_output)


class SequenceOfFiles:

    def __init__(self, name_template, directory):
        self.name_template = name_template
        self.directory = directory

    def next_file_and_number(self):
        files = list(self.list_files())

        if len(files) == 0:
            next_number = 0
        else:
            last_file_idx = files[-1][0]
            next_number = last_file_idx + 1

        return os.path.join(self.directory, self.name_template.format(next_number)), next_number

    def list_files(self):

        def gen():
            for f in glob.glob(os.path.join(self.directory, self.name_template.format("*"))):
                b = os.path.basename(f)
                idx = b.split(".")[1]
                idx = int(idx)
                yield idx, f

        yield from sorted(gen(), key= lambda t: t[0])



class SlurmArrayBatchSubmit:

    def __init__(self, pre_submit_func, sbatch_command, post_submit_func, task_keys):
        self.sbatch_command = sbatch_command
        self.pre_submit_func = pre_submit_func
        self.post_submit_func = post_submit_func
        self.task_keys = task_keys

    def invoke(self, fake_job_id=None):

        self.pre_submit_func()

        if fake_job_id is not None:
            job_id = fake_job_id
        else:
            with PortablePopen(self.sbatch_command) as p:
                p.wait_and_raise_if_non_zero()
                job_id = p.stdout_as_string().strip()

        self.post_submit_func(job_id)

class ArrayTaskManager:

    def __init__(self, task_process):
        self.task_process = task_process
        self.arrays_submitted_sacct_info = None
        self.last_sacct_row_per_task_key = dict([])
        self.array_files_sequence = SequenceOfFiles("array.{0}.tsv", self.array_task_control_dir())

    def next_array_file_name_and_number(self):
        return self.array_files_sequence.next_file_and_number()

    def gen_last_sacct_row_per_task_key(self):

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


    def invoke_sacct(self, fake_sacct_outputs=None):

        self.arrays_submitted_sacct_info = [
            ArraySubmitInfo(array_n, job_id, f, self.logger)
            for array_n, job_id, f in self.submitted_arrays_files()
        ]

        for array_info in self.arrays_submitted_sacct_info:
            array_info.invoke_sacct(fake_sacct_outputs)

        self.last_sacct_row_per_task_key = dict(self.gen_last_sacct_row_per_task_key())


    def last_sact_state_code_by_task_keys(self):
        return {k: r.long_code_state for k, r in self.last_sacct_row_per_task_key.items()}

    def logger(self):
        return self.task_process.task_logger

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


    def auto_restart_condition_regexp_per_log_file(self):
        return self.array_task_conf().auto_restart_condition_regexp_per_log_file

    def find_state_file_for_task_key(self, task_key):
        return StateFileTracker.find_state_file_if_exists(self.pipeline_work_dir(), task_key)

    def prepare_sbatch_command(self, task_key_file, array_size, sbatch_options):

        if array_size == 0:
            raise Exception(f"should not start an empty array")
        elif array_size == 1:
            array_arg = "0"
        else:
            array_arg = f"0-{array_size - 1}"

        def sbatch_lines():
            yield "sbatch"
            yield f"--array={array_arg}"
            a = self.array_task_conf().slurm_account
            if a is not None:
                yield f"--account={a}"

            yield f"--output={self.array_task_control_dir()}/launch-%A_%a.log"

            yield from sbatch_options

            yield "--export={0}".format(",".join([
                f"DRYPIPE_TASK_CONTROL_DIR={self.array_task_control_dir()}",
                f"DRYPIPE_TASK_KEY_FILE_BASENAME={os.path.basename(task_key_file)}",
                f"DRYPIPE_TASK_DEBUG={self.is_debug()}"
            ]))

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

        for sbo_idx, sbo_idx_task_keys in groupby(sorted(g(), key=key), key=key):
            yield sbatch_options_by_step_number[sbo_idx], set([t[1] for t in sbo_idx_task_keys])


    def task_keys_for_next_batch(self):

        def g():
            if self.auto_restart_condition_regexp_per_log_file() is None:
                arm = None
            else:
                arm = AutoRestartManager(self.array_task_conf().auto_restart_condition_regexp_per_log_file)

            for task_key in self.children_task_keys():
                sacct_row = self.last_sacct_row_per_task_key.get(task_key)
                if sacct_row is None:
                    #never launched
                    yield task_key
                    continue

                if SlurmJobStateLongCodes.pending_or_running(sacct_row.long_code_state):
                    continue

                if SlurmJobStateLongCodes.is_canceled(sacct_row.long_code_state):
                    yield task_key
                    continue

                if SlurmJobStateLongCodes.has_failed(sacct_row.long_code_state) and arm is not None:
                    state_file = self.find_state_file_for_task_key(task_key)
                    if arm.should_restart(state_file, alternate_logger=self.logger()):
                        yield task_key
                        continue
        return set(g())

    def next_submits(self):

        next_task_keys = self.task_keys_for_next_batch()

        for sbatch_options, task_keys in self.group_by_sbatch_options(next_task_keys):

            next_task_key_file, next_array_number = self.next_array_file_name_and_number()

            def pre_submit_func():
                self.logger().info("next array task keys in %s", next_task_key_file)

                with open(next_task_key_file, "w") as _next_task_key_file:
                    for task_key in task_keys:
                        _next_task_key_file.write(f"{task_key}\n")

            command_args = self.prepare_sbatch_command(
                next_task_key_file, len(task_keys), sbatch_options
            )

            def post_submit_func(job_id):
                self.logger().info("array job id: %s", job_id)
                with open(self.i_th_submitted_array_file(next_array_number, job_id), "w") as f:
                    f.write(" ".join(command_args))

            yield SlurmArrayBatchSubmit(pre_submit_func, command_args, post_submit_func, task_keys)
