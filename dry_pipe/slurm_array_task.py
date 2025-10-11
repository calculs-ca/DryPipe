import contextlib
import glob
import json
import logging
import mmap
import os
import re
import time
from itertools import groupby
from pathlib import Path
from typing import List, Iterator, Tuple

from dry_pipe import PortablePopen, TaskConf, RemotePipelineSpecs
from dry_pipe.state_file_tracker import StateFileTracker
from dry_pipe.task_lib import upload_task_inputs_rsync, download_task_outputs_rsync
from dry_pipe.task_process import TaskProcess

logger = logging.getLogger(__name__)

class SlurmArrayParentTask:

    def __init__(self, task_process, mockup_run_launch_local_processes=False):
        self.task_process = task_process
        self.tracker = StateFileTracker(pipeline_instance_dir=task_process.pipeline_instance_dir)
        self.pipeline_instance_dir = os.path.dirname(self.tracker.pipeline_work_dir)
        self.mockup_run_launch_local_processes = mockup_run_launch_local_processes
        self.debug = task_process.task_logger.isEnabledFor(logging.DEBUG)

    def control_dir(self):
        return os.path.join(self.tracker.pipeline_work_dir,  self.task_process.task_key)

    def task_keys_file(self):
        return os.path.join(self.control_dir(),  "task-keys.tsv")

    def children_task_keys(self):
        with open(self.task_keys_file()) as f:
            for line in f:
                yield line.strip()

    def prepare_sbatch_command(self, task_key_file, array_size, sbatch_options_override=None):

        if array_size == 0:
            raise Exception(f"should not start an empty array")
        elif array_size == 1:
            array_arg = "0"
        else:
            array_arg = f"0-{array_size - 1}"

        def sbatch_lines():
            yield "sbatch"
            yield f"--array={array_arg}"
            a = self.task_process.task_conf.slurm_account
            if a is not None:
                yield f"--account={a}"

            # stdout and stderr are handled by TaskProcess, slurm redirection is almost always empty
            slurm_std_out_err_log = os.environ.get("DRYPIPE_SLURM_STD_OUT_ERR_LOG") == "True"
            #slurm_std_out_err_log = self.task_process.is_debug()

            if slurm_std_out_err_log:
                yield f"--output={self.control_dir()}/debug-%A_%a.log"
            else:
                yield "--output=/dev/null"

            if sbatch_options_override is not None:
                yield from sbatch_options_override
            else:
                yield from self.task_process.task_conf.sbatch_options

            if slurm_std_out_err_log:
                yield f"--error={self.control_dir()}/debug-%A_%a.log"

            yield "--export={0}".format(",".join([
                f"DRYPIPE_TASK_CONTROL_DIR={self.control_dir()}",
                f"DRYPIPE_TASK_KEY_FILE_BASENAME={os.path.basename(task_key_file)}",
                f"DRYPIPE_TASK_DEBUG={self.debug}"
            ]))

            if self.task_process.wait_for_completion:
                yield "--wait"

            yield "--signal=B:USR1@50"
            yield "--parsable"
            yield f"{self.pipeline_instance_dir}/.drypipe/cli"

        return list(sbatch_lines())

    def call_sbatch(self, command_args):
        cmd = " ".join(command_args)
        with self.task_process.create_time_logger("sbatch invocation", self.task_process.task_logger.debug):
            with PortablePopen(cmd, shell=True) as p:
                p.wait_and_raise_if_non_zero()
                return p.stdout_as_string().strip()

    def call_squeue(self, submitted_job_ids, only_active):
        job_ids_as_str = ",".join(list(submitted_job_ids))
        # see JOB STATE CODES: https://slurm.schedmd.com/squeue.html

        states = "" if only_active else "--states=all"

        squeue_cmd = f'squeue -r --noheader --format="%i %t" {states} --jobs={job_ids_as_str}'
        self.task_process.task_logger.debug(squeue_cmd)
        with PortablePopen(squeue_cmd, shell=True) as p:
            p.wait()
            if p.popen.returncode != 0 and only_active:
                if "Invalid job id specified" in p.safe_stderr_as_string():
                    self.task_process.task_logger.info("job_id %s no longer active" % job_ids_as_str)
                    return
                    #raise InvalidSlurmJobIdException()
            p.raise_if_non_zero()
            for line in p.iterate_stdout_lines():
                yield line

    def _task_key_to_job_id_and_array_idx_map(self):

        submitted_arrays_files = list(self.submitted_arrays_files_with_job_is_running_status())

        job_id_array_idx_to_task_key = {}

        for array_n, job_id, job_submission_file, is_assumed_terminated in submitted_arrays_files:
            if not is_assumed_terminated:
                for task_key, array_idx in self.task_keys_in_i_th_array_file(array_n):
                    job_id_array_idx_to_task_key[(job_id, array_idx)] = task_key

        return job_id_array_idx_to_task_key

    def submitted_job_ids(self):
        return [
            job_id
            for array_n, job_id, f in self.submitted_arrays_files()
        ]

    def number_of_active_sbatch_submissions(self):
        #try:
        res = self.call_squeue_and_index_response(self.submitted_job_ids(), only_active=True)
        return len(res)
        #except InvalidSlurmJobIdException:
        #    return 0

    def submitted_arrays_files_with_job_is_running_status(self) -> Iterator[Tuple[int, str, str, bool]]:
        def _array_ended_completely(file):
            with open(file) as _file:
                for line in _file:
                    if line.strip() == "BATCH_ENDED":
                        return True
            return False

        for array_n, job_id, f in self.submitted_arrays_files():
            array_ended_completely = _array_ended_completely(f)
            yield array_n, job_id, f, array_ended_completely

    def manage_auto_restarts_from_remote(self):

        if self.task_process.task_conf.auto_restart_condition_regexp_per_log_file is not None:
            arm = AutoRestartManager(self.task_process.task_conf.auto_restart_condition_regexp_per_log_file)
        else:
            arm = None

        self.compare_and_reconcile_squeue_with_state_files()

        if arm is not None:
            relaunch_count = self.prepare_and_launch_next_array(auto_restart_manager=arm)
        else:
            relaunch_count = 0

        report = self.inspect_child_tasks()

        number_of_active_sbatch_submissions = \
            self.number_of_active_sbatch_submissions()

        report["active_sbatch_submissions"] = number_of_active_sbatch_submissions
        report["relaunch_count"] = relaunch_count

        return report


    def compare_and_reconcile_squeue_with_state_files(self, mockup_squeue_call=None, alternate_logger=None):
        """
        Returns empty dict when job has ended (completed or crashed, timed out, etc)
        or a dictionary:
            task_key -> (drypipe_state_as_string expected_squeue_state, actual_queue_state), ...}
        when the tasks StateFile is unexpected, i.e. not coherent with information from squeue

        For example, the following response {'t1': ('_step-started', 'R,PD', None)}, means:

        task t1 is '_step-started' according to the state_file, BUT is 'R' (running) according to squeue,
        the expected squeue state should be 'PD' (pending).
        """

        if alternate_logger is not None:
            this_logger = alternate_logger
        else:
            this_logger = self.task_process.task_logger


        submitted_arrays_files = list(self.submitted_arrays_files_with_job_is_running_status())

        assumed_active_job_ids = {
            job_id: array_n
            for array_n, job_id, file, array_ended_completely in submitted_arrays_files
            if not array_ended_completely
        }.keys()
        # 363:

        if len(assumed_active_job_ids) == 0:
            this_logger.info("no active array running")
            return dict([])

        job_ids_to_array_idx_to_squeue_state = self.call_squeue_and_index_response(assumed_active_job_ids, mockup_squeue_call)
        # {'363': {3: 'CD', 2: 'CD', 1: 'CD', 0: 'CD'}}

        task_key_to_job_id_and_array_idx = self._task_key_to_job_id_and_array_idx_map()

        for job_id, array_idx_to_state_codes in job_ids_to_array_idx_to_squeue_state.items():
            def is_running_or_will_run(slurm_code):
                if slurm_code in {"PD", "R", "CG", "CF"}:
                    return True
                elif slurm_code in {"F", "CD", "TO", "ST", "PR", "RV", "SE", "BF", "CA", "DL", "OOM", "NF"}:
                    return False
                this_logger.warning("rare code: %s", slurm_code)
                return False

            is_running_or_will_run_count = 0

            for array_idx, state_code in array_idx_to_state_codes.items():
                if is_running_or_will_run(state_code):
                    is_running_or_will_run_count += 1
                else:
                    task_key = task_key_to_job_id_and_array_idx[(job_id, array_idx)]
                    latest_state = self.fetch_state_file(task_key)

                    if not latest_state.has_ended():
                        is_running_or_will_run_count += 1
                    # validate state_file

            if is_running_or_will_run_count == 0:
                for array_n, job_id_, job_submission_file, array_ended_completely in submitted_arrays_files:
                    if job_id_ == job_id:
                        if not array_ended_completely:
                            this_logger.info("array %s will be flagged as ended", job_submission_file)
                            with open(job_submission_file, "a") as f:
                                f.write("\nBATCH_ENDED\n")
                            break
                        else:
                            this_logger.warning("submission %s already ended", job_submission_file)

        unexpected_states = self._validate_squeue_states_and_state_files(
            submitted_arrays_files, job_ids_to_array_idx_to_squeue_state
        )

        return unexpected_states


    def _validate_squeue_states_and_state_files(self, submitted_arrays_files, job_ids_to_array_idx_to_squeue_state):

        task_key_to_job_id_array_idx = {}

        for array_n, job_id, job_submission_file, is_assumed_terminated in submitted_arrays_files:
            if not is_assumed_terminated:
                for task_key, array_idx in self.task_keys_in_i_th_array_file(array_n):
                    task_key_to_job_id_array_idx[task_key] = (job_id, array_idx)

        dict_unexpected_states = {}

        for task_key, (job_id, array_idx) in task_key_to_job_id_array_idx.items():
            array_idx_to_squeue_state = job_ids_to_array_idx_to_squeue_state.get(job_id)
            squeue_state = None
            if array_idx_to_squeue_state is not None:
                squeue_state = array_idx_to_squeue_state.get(array_idx)
            unexpected_states = self.compare_and_reconcile_task_state_file_with_squeue_state(task_key, squeue_state)
            if unexpected_states is not None:
                dict_unexpected_states[task_key] = unexpected_states
                drypipe_state_as_string, expected_squeue_state, actual_queue_state = unexpected_states
                self.task_process.task_logger.warning(
                    "unexpected squeue state '%s', expected '%s' for task '%s'",
                    actual_queue_state, expected_squeue_state, task_key
                )
        return dict_unexpected_states


    def call_squeue_and_index_response(self, submitted_job_ids, mockup_squeue_call=None, only_active=False):
        """
          job_id -> (array_idx, job_state_code)
        """
        res: dict[str,dict[int,str]] = {}

        if self.mockup_run_launch_local_processes:
            squeue_func = lambda: []
        elif mockup_squeue_call is None:
            squeue_func = lambda: self.call_squeue(submitted_job_ids, only_active)
        else:
            squeue_func = lambda: mockup_squeue_call(submitted_job_ids)

        for line in squeue_func():
            try:

                line = line.strip()
                job_id_array_idx, squeue_state = line.split()
                job_id, array_idx = job_id_array_idx.split("_")

                array_idx_to_state_dict = res.get(job_id)
                if array_idx_to_state_dict is None:
                    array_idx_to_state_dict = {}
                    res[job_id] = array_idx_to_state_dict

                array_idx = int(array_idx)
                array_idx_to_state_dict[array_idx] = squeue_state
            except Exception as ex:
                self.task_process.task_logger.error(f"failed while parsing squeue line: '%s'", line)
                raise ex
        return res

    def mock_compare_and_reconcile_squeue_with_state_files(self, mock_squeue_lines: List[str]):
        def mockup_squeue_call(submitted_job_ids):
            yield from mock_squeue_lines

        return self.compare_and_reconcile_squeue_with_state_files(mockup_squeue_call)

    def fetch_state_file(self, task_key):
        _, state_file = self.tracker.fetch_true_state_and_update_memory_if_changed(task_key)
        return state_file

    def compare_and_reconcile_task_state_file_with_squeue_state(self, task_key, squeue_state):
        state_file = self.fetch_state_file(task_key)
        drypipe_state_as_string = state_file.state_as_string()
        # strip "state."
        drypipe_state_as_string = drypipe_state_as_string[6:]
        if "." in drypipe_state_as_string:
            drypipe_state_as_string = drypipe_state_as_string.split(".")[0]

        # see JOB STATE CODES at https://slurm.schedmd.com/squeue.html

        self.task_process.task_logger.debug(
            "task_key=%s, drypipe_state_as_string=%s, squeue_state=%s ",
            task_key, drypipe_state_as_string, squeue_state
        )

        if drypipe_state_as_string in ["completed", "failed", "killed", "timed-out"]:
            if squeue_state != "CD" and squeue_state is not None:
                return drypipe_state_as_string, None, squeue_state
        elif drypipe_state_as_string in ["ready", "waiting"]:
            if squeue_state is not None:
                return drypipe_state_as_string, None, squeue_state
        elif drypipe_state_as_string.endswith("_step-started"):
            if squeue_state is None:
                self.tracker.transition_to_crashed(state_file)
                return drypipe_state_as_string,  "R,PD", None
            elif squeue_state not in ["R", "PD"]:
                return drypipe_state_as_string, "R,PD", squeue_state
        elif drypipe_state_as_string.endswith("step-started"):
            if squeue_state is None:
                self.tracker.transition_to_crashed(state_file)
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

    def task_keys_in_i_th_array_file(self, array_number):
        with open(self.i_th_array_file(array_number)) as f:
            array_idx = 0
            for line in f:
                yield line.strip(), array_idx
                array_idx += 1

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

    def iterate_next_task_state_files(
        self, start_next_n, restart_failed, include_pre_launch, dry_run=False, auto_restart_manager=None
    ):
        i = 0
        for k in self.children_task_keys():
            state_file = self.tracker.load_state_file(k)
            if state_file.is_in_pre_launch():
                if include_pre_launch:
                    self.task_process.task_logger.debug("will launch %s", state_file.task_key)
                    yield state_file
                    i += 1
            elif state_file.is_ready():
                self.task_process.task_logger.debug("will launch %s", state_file.task_key)
                yield state_file
                i += 1
            elif not restart_failed and auto_restart_manager is not None and state_file.is_failed():
                if auto_restart_manager.should_restart(state_file,alternate_logger=self.task_process.task_logger):
                    yield state_file
                    i += 1
            elif restart_failed and (state_file.is_failed() or state_file.is_timed_out() or state_file.is_killed()):
                self.task_process.task_logger.debug("will launch %s", state_file.task_key)
                if not dry_run:
                    self.tracker.register_pre_launch(state_file, restart_failed)
                yield state_file
                i += 1
            if start_next_n is not None and i >= start_next_n:
                break

    def split_into_steps_with_sbatch_options(self, next_task_state_files):

        def g():
            for sf in next_task_state_files:

                step_number, i1, i2, i3 = self.task_process.read_task_state(sf.control_dir())

                if step_number == 0:
                    yield "", sf, None
                else:
                    tc = TaskConf.from_json_file(sf.control_dir())

                    def last_sbatch_option():
                        for i in range(step_number, 0, -1):
                            step = tc.step_invocations[i]
                            if "sbatch_options" in step:
                                sbo =  step["sbatch_options"]
                                if sbo is not None:
                                    return " ".join(map(str,sbo)), sbo
                        return "", None

                    sbos, sbo = last_sbatch_option()

                    yield sbos, sf, sbo

        def key(t):
            return t[0]

        g0 = list(g())

        def g1():
            for sbo, gr in groupby(sorted(g0, key=key), key=key):

                gr = list(gr)
                sfs = [sf for _, sf, _ in gr]
                sbos = [sbo for _, _, sbo in gr]

                if sbo == "":
                    yield None, sfs
                else:
                    yield sbos[0], sfs

        res = sorted(g1(), key=lambda t: 0 if t[0] is None else 10)

        return res


    def next_array_file_name_and_number(self):
        arrays_files = list(self.arrays_files())

        if len(arrays_files) == 0:
            next_array_number = 0
        else:
            last_array_file_idx = arrays_files[-1][0]
            next_array_number = last_array_file_idx + 1

        return os.path.join(self.control_dir(), f"array.{next_array_number}.tsv"), next_array_number


    def prepare_and_launch_next_array(
        self,
        limit=None,
        restart_failed=False, call_sbatch_mockup=None, include_pre_launch=False, dry_run=False,
        auto_restart_manager=None,
        alternate_logger=None,
    ):

        if alternate_logger is not None:
            this_logger = alternate_logger
        else:
            this_logger = self.task_process.task_logger

        next_task_state_files = list(
            self.iterate_next_task_state_files(
                limit, restart_failed, include_pre_launch, dry_run, auto_restart_manager
            )
        )

        if len(next_task_state_files) == 0:
            this_logger.info("no tasks to launch")
            return 0
        else:

            for sbatch_options, state_files_in_batch in self.split_into_steps_with_sbatch_options(next_task_state_files):

                next_task_key_file, next_array_number = self.next_array_file_name_and_number()

                this_logger.info("next array task keys in %s", next_task_key_file)

                with open(next_task_key_file, "w") as _next_task_key_file:
                    for state_file in state_files_in_batch:
                        _next_task_key_file.write(f"{state_file.task_key}\n")
                        self.tracker.register_pre_launch(state_file)

                command_args = self.prepare_sbatch_command(
                    next_task_key_file, len(state_files_in_batch), sbatch_options
                )

                if call_sbatch_mockup is not None:
                    call_sbatch_func = call_sbatch_mockup
                    this_logger.info("Will use SBATCH MOCKUP")
                elif self.mockup_run_launch_local_processes:
                    this_logger.info("Will fake SBATCH as local process")
                    call_sbatch_func = lambda: self._sbatch_mockup_launch_as_local_proceses()
                else:
                    this_logger.info("will submit array: %s", " ".join(command_args))
                    call_sbatch_func = lambda: self.call_sbatch(command_args)

                job_id = call_sbatch_func()
                if job_id is None:
                    raise Exception(f"sbatch returned None:\n {command_args}")
                this_logger.info("array job id: %s", job_id)
                with open(self.i_th_submitted_array_file(next_array_number, job_id), "w") as f:
                    f.write(" ".join(command_args))

            return len(next_task_state_files)

    def _sbatch_mockup_launch_as_local_proceses(self):

        launch_idx, next_array_file = list(self.arrays_files())[-1]

        def gen_task_keys_to_launch():
            with open(next_array_file) as af:
                for line in af:
                    line = line.strip()
                    if line != "":
                        yield line

        for task_key in gen_task_keys_to_launch():
            tp = TaskProcess(
                os.path.join(self.tracker.pipeline_work_dir, task_key),
                as_subprocess=True,
                wait_for_completion=True
            )
            tp.run()

        return f"123400{launch_idx}"

    def obsolete_run_array(self, restart_failed, reset_failed, limit):

        self.prepare_and_launch_next_array(limit)

        if self.mockup_run_launch_local_processes:
            return

        self.task_process.task_logger.info("will run array %s", self.task_process.task_key)

        if self.debug:
            pause_in_seconds = [0, 0, 0, 0, 0, 1]
        else:
            pause_in_seconds = [2, 2, 3, 3, 4, 10, 30, 60, 120, 120, 180, 240, 300]

        pause_idx = 0
        max_idx = len(pause_in_seconds) - 1
        while True:
            res = self.compare_and_reconcile_squeue_with_state_files()
            if res is None:
                break
            next_sleep = pause_in_seconds[pause_idx]
            self.task_process.task_logger.debug("will sleep %s seconds", next_sleep)
            time.sleep(next_sleep)
            if pause_idx < max_idx:
                pause_idx += 1

        self.submitted_arrays_files_with_job_is_running_status()

        total_children_tasks = 0
        ended_tasks = 0
        completed_tasks = 0
        failed_tasks = []
        for task_key in self.children_task_keys():
            total_children_tasks += 1
            state_file = self.tracker.load_state_file(task_key)
            if state_file.has_ended():
                ended_tasks += 1
            if state_file.is_completed():
                completed_tasks += 1

            if state_file.is_failed():
                failed_tasks.append(state_file.task_key)

        # We could fail earlier, at the first failure inside self.compare_and_reconcile_squeue_with_state_files()
        # but letting "compare_and_reconcile" until the last job is alive, improves monitoring, at the cost of a single
        # task running and polling the file system
        if len(failed_tasks) > 0:
            raise Exception(f"at least one failed task: {','.join(failed_tasks)}")

        if completed_tasks == total_children_tasks:
            self.task_process.task_logger.info("array %s completed", self.task_process.task_key)
            return True

        return False

    def inspect_child_tasks(self):

        total_children_tasks = 0
        ended_tasks = 0
        completed_tasks = 0
        failed_task_keys = []
        for task_key in self.children_task_keys():
            total_children_tasks += 1
            state_file = self.tracker.load_state_file(task_key)
            if state_file.has_ended():
                ended_tasks += 1
            if state_file.is_completed():
                completed_tasks += 1

            if state_file.is_failed():
                failed_task_keys.append(state_file.task_key)

        return {
            "total_children_tasks": total_children_tasks,
            "ended_tasks": ended_tasks,
            "completed_tasks": completed_tasks,
            "failed_task_keys": failed_task_keys
        }


    def _upload_array(self):

        task_key = self.task_process.task_key

        if self.task_process.task_conf.ssh_remote_dest is None:
            raise Exception(
                f"upload_array not possible for task {task_key}, " +
                f"requires ssh_remote_dest in TaskConf OR --ssh-remote-dest argument to be set"
            )

        upload_task_inputs_rsync.func(
            __task_key=self.task_process.task_key,
            __task_control_dir=self.task_process.control_dir,
            __remote_pipeline_specs=RemotePipelineSpecs(self.task_process),
            __task_logger=self.task_process.task_logger,
            __task_process=self.task_process,
            __pipeline_instance_dir=self.task_process.pipeline_instance_dir,
            __task_conf = self.task_process.task_conf
        )

    def _download_array(self):
        download_task_outputs_rsync.func(__task_process=self.task_process)

    @staticmethod
    def create_array_parent(pipeline_instance_dir, new_task_key, matcher, slurm_account, split_into, extra_env):

        state_file_tracker = StateFileTracker(pipeline_instance_dir)

        control_dir = Path(state_file_tracker.pipeline_work_dir, new_task_key)

        control_dir.mkdir(exist_ok=True)

        not_ready_task_keys = []

        tc = TaskConf(
            executer_type="slurm",
            slurm_account=slurm_account,
            extra_env=extra_env
        )
        tc.is_slurm_parent = True
        tc.inputs.append({
            "upstream_task_key": None,
            "name_in_upstream_task": None,
            "file_name": None,
            "value": None,
            "name": "children_tasks",
            "type": "task-list"
        })
        tc.save_as_json(control_dir, "")

        with open(os.path.join(control_dir, "task-keys.tsv"), "w") as tc:
            for resolved_task in state_file_tracker.load_tasks_for_query(matcher, include_non_completed=True):

                if resolved_task.is_completed():
                    continue

                # ensure upstream dependencies are met
                task_process = TaskProcess(resolved_task.control_dir(), no_logger=True)
                task_process._unserialize_and_resolve_inputs_outputs(ensure_all_upstream_deps_complete=True)

                if not resolved_task.is_ready():
                    not_ready_task_keys.append(resolved_task.key)

                tc.write(resolved_task.key)
                tc.write("\n")

        if len(not_ready_task_keys) > 0:
            print(f"Warning: {len(not_ready_task_keys)} are not in 'ready' state:")

        Path(os.path.join(control_dir, "state.ready")).touch(exist_ok=True)

    def list_array_states(self):
        for child_task_key in self.children_task_keys():
            child_task_control_dir = os.path.join(self.task_process.pipeline_work_dir, child_task_key)

            state_file_path = StateFileTracker.find_state_file_if_exists(child_task_control_dir)
            if state_file_path is not None:
                yield child_task_key, state_file_path.name


class AutoRestartManager:

    def __init__(self, auto_restart_condition_regexp_per_log_file, max_restart=3):

        self.max_restart = max_restart

        if auto_restart_condition_regexp_per_log_file is None:
            self.auto_restart_condition_regexp_per_log_file = None
        else:
            def compile_regexp(f, pattern):
                try:
                    return re.compile(pattern)
                except Exception as ex:
                    raise Exception(f"Failed to compile regex pattern '{pattern}', for file '{f}'")

            self.auto_restart_condition_regexp_per_log_file = {
                f: [
                    None if r is None else compile_regexp(f, r)
                    for r in regexen
                ]
                for f, regexen in auto_restart_condition_regexp_per_log_file.items()
            }

    def restart_file(self, state_file):
        return Path(state_file.control_dir(), "restarts.tsv")


    def _last_line_of_prev_restarts_per_file_and_restart_count(self, state_file):

        restarts = self.restart_file(state_file)

        last_line_of_prev_restarts_per_file = {
            f: None
            for f in self.auto_restart_condition_regexp_per_log_file.keys()
        }

        restart_decisions_for_missing_log_files = set([])

        if not restarts.exists():
            return last_line_of_prev_restarts_per_file, restart_decisions_for_missing_log_files, 0
        else:

            restart_counter = [0]

            def g():

                yield from last_line_of_prev_restarts_per_file.items()

                with open(restarts, "r") as f:
                    rows = []
                    for line in f:
                        line = line.strip()
                        if line == "":
                            continue
                        if line.startswith("RESET"):
                            rows.clear()
                            continue
                        row = [
                            l.strip() for l in line.split("\t", maxsplit=3)
                        ]
                        log_name, line_in_log, matching_line = row
                        if matching_line == "MISSING":
                            restart_decisions_for_missing_log_files[log_name] = True
                        else:
                            rows.append(row)

                    for row in rows:
                        log_name, line_in_log, ignore = row
                        restart_counter[0] = restart_counter[0] + 1
                        yield log_name, int(line_in_log) if line_in_log != "None" else None

            last_line_of_prev_restarts_per_file = dict(g())

        return  last_line_of_prev_restarts_per_file, restart_decisions_for_missing_log_files, restart_counter[0]

    def _record_restart(self, state_file, log_file_name, line_number, matching_line):
        with open(self.restart_file(state_file), "a") as f:
            f.write(f"{log_file_name}\t{line_number}\t{matching_line.strip()}\n")

    def should_restart_with_details(self, state_file, alternate_logger=None):

        if alternate_logger is None:
            alternate_logger = logger

        last_line_of_prev_restarts_per_file, restart_decisions_for_missing_log_files, restart_count = \
            self._last_line_of_prev_restarts_per_file_and_restart_count(state_file)

        if restart_count >= self.max_restart:
            alternate_logger.info("%s has reached max relaunch %s", state_file.task_key, restart_count)
            return False, 0, restart_count, None

        for f, regexen in self.auto_restart_condition_regexp_per_log_file.items():
            for r in regexen:
                log_file = Path(state_file.control_dir(), f)

                if r is None:
                    # a task without a log is abnormal, most often as a result of a restartable error,
                    # we restart, but at most once
                    if not log_file.exists() and restart_count == 0:

                        if f in restart_decisions_for_missing_log_files:
                            continue

                        self._record_restart(state_file, f, None, "MISSING_FILE")
                        alternate_logger.info(
                            "%s has no log %s, will relaunch", state_file.task_key, log_file
                        )
                        return True, None, restart_count, f
                    else:
                        continue
                else:
                    if not log_file.exists():
                        continue

                match_starting_at_line = last_line_of_prev_restarts_per_file[f]

                with open(log_file) as log_file_h:
                    c = 0

                    last_line_of_match_occurrence = None

                    for line in log_file_h:
                        c += 1
                        if match_starting_at_line is not None and c <= match_starting_at_line:
                            continue

                        if r.match(line):
                            last_line_of_match_occurrence = c

                    if last_line_of_match_occurrence is not None:
                        alternate_logger.info(
                            "will relaunch %s, relaunches so far: %s", state_file.task_key, restart_count
                        )
                        self._record_restart(state_file, f, last_line_of_match_occurrence, line)
                        return True, c, restart_count, f

        alternate_logger.debug("will NOT relaunch %s", state_file.task_key)

        return False, None, restart_count, None

    def should_restart(self, state_file, alternate_logger=None):
        should_restart, matching_line_number, restart_count, matching_log_filename = \
            self.should_restart_with_details(state_file, alternate_logger)
        return should_restart
