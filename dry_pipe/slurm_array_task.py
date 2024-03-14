import glob
import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import List, Iterator, Tuple

from dry_pipe import PortablePopen, TaskConf
from dry_pipe.core_lib import StateFileTracker
from dry_pipe.task_process import TaskProcess


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

    def prepare_sbatch_command(self, task_key_file, array_size):

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

            if self.debug:
                yield f"--output={self.control_dir()}/debug-%A_%a.log"
            else:
                yield "--output=/dev/null"

            yield from self.task_process.sbatch_options()

            if self.debug:
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
        with PortablePopen(cmd, shell=True) as p:
            p.wait_and_raise_if_non_zero()
            return p.stdout_as_string().strip()

    def call_squeue(self, submitted_job_ids):
        job_ids_as_str = ",".join(list(submitted_job_ids))
        # see JOB STATE CODES: https://slurm.schedmd.com/squeue.html
        squeue_cmd = f'squeue -r --noheader --format="%i %t" --states=all --jobs={job_ids_as_str}'
        self.task_process.task_logger.debug(squeue_cmd)
        with PortablePopen(squeue_cmd, shell=True) as p:
            #p.wait()
            # "stderr: slurm_load_jobs error: Invalid job id specified"
            #if self.popen.returncode != 0:
            #    p.safe_stderr_as_string()
            p.wait_and_raise_if_non_zero()
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

    def compare_and_reconcile_squeue_with_state_files(self, mockup_squeue_call=None):
        """
        Returns None when job has ended (completed or crashed, timed out, etc)
        or a dictionary:
            task_key -> (drypipe_state_as_string expected_squeue_state, actual_queue_state), ...}
        when the tasks state file has is not expected according to the task status returned by squeue.

        For example, the following response {'t1': ('_step-started', 'R,PD', None)}, means:

        task t1 is '_step-started' according to the state_file, BUT is 'R' (running) according to squeue,
        the expected squeue state should be 'PD' (pending).
        """

        submitted_arrays_files = list(self.submitted_arrays_files_with_job_is_running_status())

        assumed_active_job_ids = {
            job_id: array_n
            for array_n, job_id, file, is_terminated in submitted_arrays_files
            if not is_terminated
        }
        # 363:

        if len(assumed_active_job_ids) == 0:
            return None

        job_ids_to_array_idx_to_squeue_state = self.call_squeue_and_index_response(assumed_active_job_ids, mockup_squeue_call)
        # {'363': {3: 'CD', 2: 'CD', 1: 'CD', 0: 'CD'}}

        task_key_to_job_id_and_array_idx = self._task_key_to_job_id_and_array_idx_map()

        for job_id, array_idx_to_state_codes in job_ids_to_array_idx_to_squeue_state.items():
            def is_running_or_will_run(slurm_code):
                if slurm_code in {"PD", "R", "CG", "CF"}:
                    return True
                elif slurm_code in {"F", "CD", "TO", "ST", "PR", "RV", "SE", "BF", "CA", "DL", "OOM", "NF"}:
                    return False
                self.task_process.task_logger.warning("rare code: %s", slurm_code)
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
                for _, job_id_, job_submission_file, is_assumed_terminated in submitted_arrays_files:
                    if job_id_ == job_id:
                        if not is_assumed_terminated:
                            with open(job_submission_file, "a") as f:
                                f.write("\nBATCH_ENDED\n")
                            break
                        else:
                            self.task_process.task_logger.warning("submission %s already ended")

        u_states = self._validate_squeue_states_and_state_files(
            submitted_arrays_files, job_ids_to_array_idx_to_squeue_state
        )

        return u_states


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


    def call_squeue_and_index_response(self, submitted_job_ids, mockup_squeue_call=None):
        """
          job_id -> (array_idx, job_state_code)
        """
        res: dict[str,dict[int,str]] = {}

        if self.mockup_run_launch_local_processes:
            squeue_func = lambda: []
        elif mockup_squeue_call is None:
            squeue_func = lambda: self.call_squeue(submitted_job_ids)
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

    def submitted_arrays_files_with_job_is_running_status(self) -> Iterator[Tuple[int, str, str, bool]]:
        def s(file):
            with open(file) as _file:
                for line in _file:
                    if line.strip() == "BATCH_ENDED":
                        return True
            return False

        for array_n, job_id, f in self.submitted_arrays_files():
            status = s(f)
            yield array_n, job_id, f, status

    def i_th_submitted_array_file(self, array_number, job_id):
        return os.path.join(self.control_dir(), f"array.{array_number}.job.{job_id}")

    def _iterate_next_task_state_files(self, start_next_n, restart_failed):
        i = 0
        for k in self.children_task_keys():
            state_file = self.tracker.load_state_file(k)
            if state_file.is_in_pre_launch():
                pass
            elif state_file.is_ready():
                yield state_file
                i += 1
            elif restart_failed and (state_file.is_failed() or state_file.is_timed_out()):
                self.tracker.register_pre_launch(state_file, restart_failed)
                yield state_file
                i += 1
            if start_next_n is not None and i >= start_next_n:
                break

    def prepare_and_launch_next_array(self, limit=None, restart_failed=False, call_sbatch_mockup=None):

        arrays_files = list(self.arrays_files())

        if len(arrays_files) == 0:
            next_array_number = 0
        else:
            last_array_file_idx = arrays_files[-1][0]
            next_array_number = last_array_file_idx + 1

        next_task_key_file = os.path.join(self.control_dir(), f"array.{next_array_number}.tsv")

        next_task_state_files = list(self._iterate_next_task_state_files(limit, restart_failed))

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
                self.task_process.task_logger.info("Will use SBATCH MOCKUP")
            elif self.mockup_run_launch_local_processes:
                self.task_process.task_logger.info("Will fake SBATCH as local process")
                call_sbatch_func = lambda: self._sbatch_mockup_launch_as_local_proceses()
            else:
                self.task_process.task_logger.info("will submit array: %s", " ".join(command_args))
                call_sbatch_func = lambda: self.call_sbatch(command_args)

            job_id = call_sbatch_func()
            if job_id is None:
                raise Exception(f"sbatch returned None:\n {command_args}")
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

    def run_array(self, restart_failed, reset_failed, limit):

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
        for task_key in self.children_task_keys():
            total_children_tasks += 1
            state_file = self.tracker.load_state_file(task_key)
            if state_file.has_ended():
                ended_tasks += 1
            if state_file.is_completed():
                completed_tasks += 1

        if completed_tasks == total_children_tasks:
            self.task_process.task_logger.info("array %s completed", self.task_process.task_key)
            return True

        return False

    def upload_array(self):

        task_key = self.task_process.task_key

        if self.task_process.task_conf.ssh_remote_dest is None:
            raise Exception(
                f"upload_array not possible for task {task_key}, " +
                f"requires ssh_remote_dest in TaskConf OR --ssh-remote-dest argument to be set"
            )

        def dump_unique_files_in_file(files, dep_file):
            uniq_files = set()
            with open(dep_file, "w") as tf:
                for dep_file in files:
                    if dep_file not in uniq_files:
                        tf.write(dep_file)
                        tf.write("\n")
                        uniq_files.add(dep_file)

        internal_dep_file_txt = os.path.join(self.control_dir(), "deps.txt")
        external_dep_file_txt = os.path.join(self.control_dir(), "external-deps.txt")
        external_file_deps = []

        def gen_inner_pipeline_file_deps():

            def gen_internal_file_deps():
                def _gen_0():
                    for child_task_key in self.children_task_keys():

                        p = TaskProcess(
                            os.path.join(self.tracker.pipeline_work_dir, child_task_key),
                            ensure_all_upstream_deps_complete=True
                        )

                        for _, file in p.inputs.rsync_file_list_produced_upstream():
                            yield file

                        for file in p.inputs.rsync_output_var_file_list_produced_upstream():
                            yield file

                        for _, file in p.inputs.rsync_external_file_list():
                            external_file_deps.append(file)

                        yield f".drypipe/{child_task_key}/state.ready"
                        yield f".drypipe/{child_task_key}/task-conf.json"

                        for step in p.task_conf.step_invocations:
                            if step["call"] == "bash":
                                _, script = step["script"].rsplit("/", 1)
                                yield f".drypipe/{child_task_key}/{script}"

                    yield ".drypipe/cli"

                    for py_file in glob.glob(os.path.join(os.path.dirname(__file__), "*.py")):
                        yield f".drypipe/dry_pipe/{os.path.basename(py_file)}"

                    yield f".drypipe/{task_key}/task-conf.json"
                    yield f".drypipe/{task_key}/task-keys.tsv"
                    yield f".drypipe/{task_key}/state.ready"

                pipeline_instance_name = os.path.basename(self.pipeline_instance_dir)
                for f in _gen_0():
                    yield f"{pipeline_instance_name}/{f}"

            self.task_process.task_logger.info("will generate file list for upload")

            dump_unique_files_in_file(gen_internal_file_deps(), internal_dep_file_txt)

            self.task_process.task_logger.info("done")

        def gen_external_file_deps():
            if len(external_file_deps) == 0:
                self.task_process.task_logger.info("no external file deps")
            else:
                self.task_process.task_logger.info("will generate external file deps")

            dump_unique_files_in_file(external_file_deps, external_dep_file_txt)

        # gen all deps files before rsync
        gen_inner_pipeline_file_deps()

        gen_external_file_deps()

        user_at_host, remote_base_dire, ssh_key_file = self.task_process.task_conf.parse_ssh_remote_dest()

        ssh_remote_dest = f"{user_at_host}:{remote_base_dire}"

        pid = os.path.abspath(os.path.dirname(self.task_process.pipeline_instance_dir))

        pid_base_name = os.path.basename(self.task_process.pipeline_instance_dir)

        remote_pid = os.path.join(remote_base_dire, pid_base_name)

        def do_rsync(src, dst, deps_file):
            rsync_cmd = f"rsync --mkpath -a --dirs --files-from={deps_file} {src}/ {dst}/"
            self.task_process.task_logger.info("%s", rsync_cmd)
            self.task_process.invoke_rsync(rsync_cmd)

        overrides_basename = "task-conf-overrides.json"

        def gen_task_conf_remote_overrides():
            tmp_overrides_file = os.path.join(self.control_dir(), overrides_basename)
            with open(tmp_overrides_file, "w") as tmp_overrides:
                tmp_overrides.write(json.dumps(
                    {
                        "external_files_root": f"{remote_pid}/external-file-deps"
                    },
                    indent=2
                ))

            shutil.copy(
                tmp_overrides_file,
                os.path.join(self.task_process.control_dir, f"task-conf-overrides-{user_at_host}.json")
            )

            dst = f"{user_at_host}:{remote_pid}/.drypipe/{self.task_process.task_key}/"

            self.task_process.invoke_rsync(f"rsync --mkpath {tmp_overrides_file} {dst}")

            os.remove(tmp_overrides_file)

        gen_task_conf_remote_overrides()

        do_rsync(pid, ssh_remote_dest, internal_dep_file_txt)

        do_rsync("", f"{ssh_remote_dest}/{pid_base_name}/external-file-deps", external_dep_file_txt)

    def download_array(self):
        def gen_result_files():
            for child_task_key in self.children_task_keys():
                p = TaskProcess(
                    os.path.join(self.tracker.pipeline_work_dir, child_task_key),
                    ensure_all_upstream_deps_complete=False
                )
                for file in p.outputs.rsync_file_list():
                    yield file

        result_file_txt = os.path.join(self.control_dir(), "result-files.txt")
        uniq_files = set()
        with open(result_file_txt, "w") as tf:
            for result_file in gen_result_files():
                if result_file not in uniq_files:
                    tf.write(result_file)
                    tf.write("\n")
                    uniq_files.add(result_file)

        rps = self.task_process.task_conf.remote_pipeline_specs(self.tracker.pipeline_instance_dir)

        pid = self.task_process.pipeline_instance_dir

        pipeline_base_name = os.path.basename(pid)

        ssh_remote_dest = \
            f"{rps.user_at_host}:{rps.remote_base_dir}/{pipeline_base_name}/"

        rsync_cmd = f"rsync -a --dirs --files-from={result_file_txt} {ssh_remote_dest} {pid}/"

        self.task_process.task_logger.debug("%s", rsync_cmd)
        self.task_process.invoke_rsync(rsync_cmd)

        remote_exec_result = self.task_process.exec_remote(rps.user_at_host, [
            rps.remote_cli,
            "list-array-states",
            f"--task-key={self.task_process.task_key}"
        ])

        for task_key_task_state in remote_exec_result.split("\n"):

            task_key_task_state = task_key_task_state.strip()

            if task_key_task_state == "":
                continue

            task_key, task_state = task_key_task_state.split("/")

            task_control_dir = os.path.join(self.task_process.pipeline_work_dir, task_key)

            state_file_path = StateFileTracker.find_state_file_if_exists(task_control_dir)

            if state_file_path is not None:
                actual_state = os.path.join(task_control_dir, task_state)
                os.rename(state_file_path.path, actual_state)

    @staticmethod
    def create_array_parent(pipeline_instance_dir, new_task_key, matcher, slurm_account, split_into):

        state_file_tracker = StateFileTracker(pipeline_instance_dir)

        control_dir = Path(state_file_tracker.pipeline_work_dir, new_task_key)

        control_dir.mkdir(exist_ok=True)

        not_ready_task_keys = []

        tc = TaskConf(
            executer_type="slurm",
            slurm_account=slurm_account
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
