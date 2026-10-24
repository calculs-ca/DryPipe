import fcntl
import getpass
import glob
import math
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from dry_pipe import PortablePopen
from dry_pipe.slurm_arrays import SbatchRejected


def _print(msg):
    print(msg, file=sys.stderr, flush=True)


def account_of_sbatch_options(sbatch_options):
    """the account of --account=A, --account A or -A A, the last one wins, as with sbatch, None when none"""
    options = list(sbatch_options)

    def accounts():
        for option, next_option in zip(options, options[1:] + [None]):
            if option.startswith("--account="):
                yield option.split("=", 1)[1]
            elif option in ("--account", "-A"):
                yield next_option

    return next(reversed(list(accounts())), None)


def count_queued_jobs(account, timeout_secs):
    """
    the number of jobs of the user in the queue on the slurm account, all pipelines included, array elements
    counted one by one (-r), as MaxSubmitJobs counts them, None when squeue fails or times out
    """
    if shutil.which("squeue") is None:
        return None

    with PortablePopen(["squeue", "-u", getpass.getuser(), "-A", account, "-h", "-r", "-o", "%i"]) as p:
        try:
            stdout, _ = p.communicate(timeout=timeout_secs)
        except subprocess.TimeoutExpired:
            p.popen.kill()
            return None

    if p.popen.returncode != 0:
        return None

    return len([line for line in stdout.split("\n") if line.strip() != ""])


class LoopIsRunning(Exception):
    pass


class LoopLock:
    """
    .drypipe/<array-key>/loop.lock, held by the process that runs a loop of the array, it holds the
    host, pid, user and start time of that process. The file is deleted when the process ends, and a killed
    process releases its lock, leaving an unlocked file
    """

    def __init__(self, array_control_dir):
        self.path = Path(array_control_dir, "loop.lock")
        self.file = None

    def acquire(self):
        f = open(self.path, "a+")
        try:
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            f.seek(0)
            holder = f.read().strip()
            f.close()
            raise LoopIsRunning(self._running_message(holder))

        def is_deleted_by_previous_holder():
            # the file was opened before its holder deleted it, the lock is on a file no one else will see
            return not self.path.exists() or os.stat(self.path).st_ino != os.fstat(f.fileno()).st_ino

        if is_deleted_by_previous_holder():
            f.close()
            self.acquire()
            return

        f.seek(0)
        f.truncate()
        f.write("\t".join([
            socket.gethostname(), str(os.getpid()), getpass.getuser(), datetime.now().strftime("%Y-%m-%d %H:%M")
        ]))
        f.flush()
        self.file = f

    def release(self):
        if self.file is not None:
            # deleted before closing: deleting it after the lock is released could delete another process's lock
            os.remove(self.path)
            self.file.close()
            self.file = None

    def _running_message(self, holder):
        if holder.count("\t") != 3:
            return f"a loop of this array is running, {self.path} is locked by another process"

        host, pid, user, started = holder.split("\t")

        def stop_command():
            if host == socket.gethostname():
                return f"kill {pid}"
            return f"ssh {host} kill {pid}"

        return "\n".join([
            f"a loop of this array is being run by pid {pid} on {host} (user {user}), started {started}",
            f"to stop it:  {stop_command()}"
        ])


class LaunchLoop:
    """
    .drypipe/<array-key>/loop.<n>.<state>.tsv, state is running, complete or abandoned.
    The keys of the loop, in submit order, followed by a "# array.<x>" comment per array submitted by the loop,
    written before its sbatch: an array is submitted only if its array.<x>.job.<job_id> exists.
    A "# options: ..." comment is appended each time the loop is started or resumed, the last one wins.
    """

    def __init__(self, array_task_manager, path):
        self.array_task_manager = array_task_manager
        self.path = Path(path)

    @staticmethod
    def _loop_files(array_control_dir, state="*"):
        return glob.glob(os.path.join(array_control_dir, f"loop.*.{state}.tsv"))

    @staticmethod
    def _unfinished_loop_file(array_control_dir):
        running = LaunchLoop._loop_files(array_control_dir, "running")
        if len(running) == 0:
            return None
        if len(running) > 1:
            raise Exception(f"more than one running loop: {running}")
        return running[0]

    @staticmethod
    def unfinished(array_task_manager):
        path = LaunchLoop._unfinished_loop_file(array_task_manager.array_task_control_dir())
        return None if path is None else LaunchLoop(array_task_manager, path)

    @staticmethod
    def saved_options(array_control_dir):
        """{"--option": "value"} of the "# options:" comments of the unfinished loop, the last ones win"""
        path = LaunchLoop._unfinished_loop_file(array_control_dir)
        if path is None:
            return {}

        prefix = "# options:"
        with open(path) as f:
            option_lines = [line[len(prefix):] for line in f if line.startswith(prefix)]

        return dict(
            token.split("=", 1)
            for line in option_lines
            for token in shlex.split(line)
        )

    def append_options(self, options):
        with open(self.path, "a") as f:
            f.write(f"# options: {shlex.join(options)}\n")

    @staticmethod
    def create(array_task_manager, task_keys):

        def next_number():
            numbers = [
                int(os.path.basename(f).split(".")[1])
                for f in LaunchLoop._loop_files(array_task_manager.array_task_control_dir())
            ]
            return max(numbers, default=0) + 1

        path = Path(array_task_manager.array_task_control_dir(), f"loop.{next_number()}.running.tsv")
        with open(path, "w") as f:
            for task_key in task_keys:
                f.write(f"{task_key}\n")

        return LaunchLoop(array_task_manager, path)

    def number(self):
        return int(self.path.name.split(".")[1])

    def _lines(self):
        with open(self.path) as f:
            return [line.strip() for line in f if line.strip() != ""]

    def task_keys(self):
        return [line for line in self._lines() if not line.startswith("#")]

    def _submitted_array_numbers(self):
        commented = {
            int(line.split(".")[1]) for line in self._lines() if line.startswith("# array.")
        }
        return {
            array_n for array_n, _, _ in self.array_task_manager.submitted_arrays_files()
            if array_n in commented
        }

    def remaining_keys(self):

        def submitted_keys():
            for array_n in self._submitted_array_numbers():
                with open(self.array_task_manager.array_files_sequence.file_name(array_n)) as f:
                    yield from (line.strip() for line in f if line.strip() != "")

        submitted = set(submitted_keys())
        return [key for key in self.task_keys() if key not in submitted]

    def _rename(self, state):
        new_path = self.path.with_name(f"loop.{self.number()}.{state}.tsv")
        os.rename(self.path, new_path)
        self.path = new_path

    def abandon(self):
        self._rename("abandoned")

    def run(
        self, max_queued, margin, min_batch, sleep_secs, tasks_per_job, count_queued_jobs, submits_of,
        sleep=time.sleep, invoke_submit=lambda submit: submit.invoke()
    ):
        """
        submits_of(task_keys) returns the SlurmArrayBatchSubmits of task_keys, the loop exits when all its
        tasks are submitted
        """

        tasks_per_job = tasks_per_job or 1
        is_submitting = False
        is_stop_requested = False

        def stop(signum, frame):
            nonlocal is_stop_requested
            if is_submitting:
                is_stop_requested = True
            else:
                _print(f"loop {self.number()} stopped by signal {signum}")
                sys.exit(1)

        previous_handlers = {signum: signal.signal(signum, stop) for signum in (signal.SIGTERM, signal.SIGINT)}

        def submit(task_keys):
            nonlocal is_submitting
            is_submitting = True
            try:
                for array_submit in submits_of(task_keys):
                    with open(self.path, "a") as f:
                        f.write(f"# array.{array_submit.array_number}\n")
                    try:
                        invoke_submit(array_submit)
                        _print(f"submitted array.{array_submit.array_number} with {len(array_submit.task_keys)} tasks")
                    except SbatchRejected as e:
                        _print(f"sbatch rejected array.{array_submit.array_number}, will retry next cycle:\n{e}")
            finally:
                is_submitting = False

            if is_stop_requested:
                _print(f"loop {self.number()} stopped by signal")
                sys.exit(1)

        def cycle():
            remaining = self.remaining_keys()
            queued = count_queued_jobs()
            now = datetime.now().strftime("%Y-%m-%d %H:%M")

            if queued is None:
                _print(f"{now} squeue failed, skipping this cycle, {len(remaining)} tasks left")
                return

            free = max_queued - margin - queued
            remaining_jobs = math.ceil(len(remaining) / tasks_per_job)

            if free < min(min_batch, remaining_jobs):
                _print(f"{now} {queued} jobs queued, {free} free < {min_batch}, {len(remaining)} tasks left")
                return

            _print(f"{now} {queued} jobs queued, {free} free, {len(remaining)} tasks left")
            submit(remaining[:free * tasks_per_job])

        try:
            while True:
                cycle()
                if len(self.remaining_keys()) == 0:
                    break
                sleep(sleep_secs)
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)

        self._rename("complete")
        _print(f"loop {self.number()} complete, all its tasks are submitted")
