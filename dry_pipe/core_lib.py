import fnmatch
import importlib
import json
import os
import shutil
import subprocess
from pathlib import Path


class PortablePopen:

    def __init__(self, process_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=None, shell=False, stdin=None):

        if stdout is None:
            raise Exception(f"stdout can't be None")

        if stderr is None:
            raise Exception(f"stderr can't be None")

        if isinstance(process_args, list):
            self.process_args = [str(p) for p in process_args]
        else:
            self.process_args = process_args

        self.popen = subprocess.Popen(
            process_args,
            stdout=stdout,
            stderr=stderr,
            shell=shell,
            env=env,
            stdin=stdin
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.popen.__exit__(exc_type, exc_val, exc_tb)

    def wait(self, timeout=None):
        return self.popen.wait(timeout)

    def stderr_as_string(self):
        return self.popen.stderr.read().decode("utf8")

    def stdout_as_string(self):
        return self.popen.stdout.read().decode("utf8")

    def read_stdout_lines(self):
        for line in self.popen.stdout.readlines():
            yield line.decode("utf8")

    def iterate_stdout_lines(self):
        for line in self.popen.stdout:
            yield line.decode("utf8")

    def safe_stderr_as_string(self):
        i = 0
        try:
            s = self.popen.stderr.read()
            i = 1
            return s.decode("utf8")
        except Exception as e:
            return f"failed to capture process stderr ({i}): {e}"

    def raise_if_non_zero(self):
        r = self.popen.returncode
        if r != 0:
            raise Exception(
                f"process invocation returned non zero {r}: {self.process_args}\nstderr: {self.safe_stderr_as_string()}"
            )

    def wait_and_raise_if_non_zero(self, timeout=None):
        self.popen.wait(timeout)
        self.raise_if_non_zero()


def assert_slurm_supported_version():

    with PortablePopen(
        ["sbatch", "--version"]
    ) as p:

        p.wait()

        if p.popen.returncode != 0:
            return

        out = p.stdout_as_string()
        version_s = out.split()[1]

        version = version_s.split(".")

        if len(version) != 3:
            raise Exception(f"unrecognized slurm version: {out}")
        else:
            v1, v2, v3 = [int(v0) for v0 in version]
            if v1 < 21:
                raise Exception(f"unsupported sbatch version {version_s}")

        return v1, v2, v3


def is_inside_slurm_job():
    return "SLURM_JOB_ID" in os.environ


class UpstreamTasksNotCompleted(Exception):
    def __init__(self, upstream_task_key, msg):
        self.upstream_task_key = upstream_task_key
        self.msg = msg

class StateFile:

    def __init__(self, task_key, current_hash_code, tracker, path=None, slurm_array_id=None):
        self.tracker = tracker
        self.task_key = task_key
        if path is not None:
            self.path = path
        else:
            self.path = os.path.join(tracker.pipeline_work_dir, task_key, "state.waiting")
        self.hash_code = current_hash_code
        self.inputs = None
        self.outputs = None
        self.is_slurm_array_child = False
        if slurm_array_id is not None:
            self.slurm_array_id = slurm_array_id
        self.is_parent_task = False

    def __repr__(self):
        return f"/{self.task_key}/{os.path.basename(self.path)}"

    def refresh(self, new_path):
        assert self.path != new_path
        self.path = new_path
        if self.path.endswith("state.completed"):
            # load from file:
            self.outputs = None
            self.inputs = None

    def transition_to_pre_launch(self):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state._step-started")

    def transition_to_crashed(self):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state.crashed")

    def transition_to_ready(self):
        self.path = os.path.join(self.tracker.pipeline_work_dir, self.task_key, "state.crashed")

    def state_as_string(self):
        return os.path.basename(self.path)

    def is_completed(self):
        return self.path.endswith("state.completed")

    def is_failed(self):
        return fnmatch.fnmatch(self.path, "*/state.failed.*")

    def is_crashed(self):
        return fnmatch.fnmatch(self.path, "*/state.crashed.*")

    def is_timed_out(self):
        return fnmatch.fnmatch(self.path, "*/state.timed-out.*")

    def is_waiting(self):
        return self.path.endswith("state.waiting")

    def has_ended(self):
        return self.is_completed() or self.is_timed_out() or self.is_failed() or self.is_crashed()

    def is_ready(self):
        return self.path.endswith("state.ready")

    def is_ready_or_passed(self):
        return not self.is_waiting()

    def is_in_pre_launch(self):
        return fnmatch.fnmatch(self.path, "*/state._step-started")

    def control_dir(self):
        return os.path.join(self.tracker.pipeline_work_dir, self.task_key)

    def output_dir(self):
        return os.path.join(self.tracker.pipeline_output_dir, self.task_key)

    def task_conf_file(self):
        return os.path.join(self.control_dir(), "task-conf.json")

    def load_task_conf_json(self):
        with open(self.task_conf_file()) as tc:
            return json.loads(tc.read())

    def touch_initial_state_file(self, is_ready):
        if is_ready:
            self.transition_to_ready()
        Path(self.path).touch(exist_ok=False)


class FileCreationDefaultModes:
    pipeline_instance_directories = 0o774
    pipeline_instance_scripts = 0o774

def func_from_mod_func(mod_func):

    mod, func_name = mod_func.split(":")

    if not mod.startswith("."):
        module = importlib.import_module(mod)
    else:
        module = importlib.import_module(mod[1:])

    python_task = getattr(module, func_name, None)
    if python_task is None:
        raise Exception(f"function {func_name} not found in module {mod}")

    return python_task



class StateFileTracker:

    def __init__(self, pipeline_instance_dir):
        self.pipeline_instance_dir = pipeline_instance_dir
        self.pipeline_work_dir = os.path.join(pipeline_instance_dir, ".drypipe")
        self.pipeline_output_dir = os.path.join(self.pipeline_instance_dir, "output")
        self.state_files_in_memory: dict[str, StateFile] = {}
        self.load_from_disk_count = 0
        self.resave_count = 0
        self.new_save_count = 0

    def instance_exists(self):
        return os.path.exists(self.pipeline_work_dir)

    def prepare_instance_dir(self, conf_dict):
        Path(self.pipeline_instance_dir).mkdir(
            exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)
        Path(self.pipeline_work_dir).mkdir(
            exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)
        Path(self.pipeline_instance_dir, "output").mkdir(
            exist_ok=False, mode=FileCreationDefaultModes.pipeline_instance_directories)

        with open(Path(self.pipeline_work_dir, "conf.json"), "w") as conf_file:
            conf_file.write(json.dumps(conf_dict, indent=4))

        src_dir_drypipe = os.path.dirname(__file__)

        dp_dir = Path(self.pipeline_work_dir, "dry_pipe")
        dp_dir.mkdir(exist_ok=True)

        shutil.copy(os.path.join(src_dir_drypipe, "__init__.py"), dp_dir)
        shutil.copy(os.path.join(src_dir_drypipe, "core_lib.py"), dp_dir)
        shutil.copy(os.path.join(src_dir_drypipe, "task_process.py"), dp_dir)
        shutil.copy(os.path.join(src_dir_drypipe, "task.py"), dp_dir)
        shutil.copy(os.path.join(src_dir_drypipe, "cli.py"), dp_dir)
        shutil.copy(os.path.join(src_dir_drypipe, "cli"), self.pipeline_work_dir)
        #os.chmod(script_lib_file, FileCreationDefaultModes.pipeline_instance_scripts)


    def set_completed_on_disk(self, task_key):
        os.rename(
            self.state_files_in_memory[task_key].path,
            os.path.join(self.pipeline_work_dir, task_key, "state.completed")
        )

    def set_ready_on_disk_and_in_memory(self, task_key):
        p = os.path.join(self.pipeline_work_dir, task_key, "state.ready")
        os.rename(self.state_files_in_memory[task_key].path, p)
        self.state_files_in_memory[task_key].path = p

    def set_step_state_on_disk_and_in_memory(self, task_key, state_base_name):
        p = os.path.join(self.pipeline_work_dir, task_key, state_base_name)
        b4 = self.state_files_in_memory[task_key].path
        os.rename(b4, p)
        self.state_files_in_memory[task_key].path = p

    def transition_to_crashed(self, state_file):
        previous_path = state_file.path
        state_file.transition_to_crashed()
        os.rename(previous_path, state_file.path)

    def register_pre_launch(self, state_file):
        previous_path = state_file.path
        state_file.transition_to_pre_launch()
        os.rename(previous_path, state_file.path)

    def completed_task_keys(self):
        for k, state_file in self.state_files_in_memory.items():
            if state_file.is_completed():
                yield k

    def all_state_files(self):
        return self.state_files_in_memory.values()

    def lookup_state_file_from_memory(self, task_key):
        return self.state_files_in_memory[task_key]

    def _find_state_file_path_in_task_control_dir(self, task_key) -> str :
        try:
            with os.scandir(os.path.join(self.pipeline_work_dir, task_key)) as i:
                for f in i:
                    if f.name.startswith("state."):
                        return f.path
        except FileNotFoundError:
            pass
        return None

    def load_state_file(self, task_key, slurm_array_id=None):
        state_file_path = self._find_state_file_path_in_task_control_dir(task_key)
        if state_file_path is None:
            raise Exception(f"no state file exists in {os.path.join(self.pipeline_work_dir, task_key)}")
        state_file = StateFile(task_key, None, self, path=state_file_path, slurm_array_id=slurm_array_id)
        self.state_files_in_memory[task_key] = state_file
        return state_file

    def fetch_true_state_and_update_memory_if_changed(self, task_key):
        state_file = self.lookup_state_file_from_memory(task_key)
        if os.path.exists(state_file.path):
            return None, state_file
        else:
            state_file_path = self._find_state_file_path_in_task_control_dir(task_key)
            if state_file_path is None:
                raise Exception(f"no state file exists in {os.path.join(self.pipeline_work_dir, task_key)}")
            state_file.refresh(state_file_path)
            return state_file, state_file

    def _load_task_conf(self, task_control_dir):
        with open(os.path.join(task_control_dir, "task-conf.json")) as tc:
            return json.loads(tc.read())

    def load_from_existing_file_on_disc_and_resave_if_required(self, task, state_file_path):

        task_control_dir = os.path.dirname(state_file_path)
        task_key = os.path.basename(task_control_dir)
        pipeline_work_dir = os.path.dirname(task_control_dir)
        assert task.key == task_key
        task_conf = self._load_task_conf(task_control_dir)
        current_hash_code = task.compute_hash_code()
        state_file = StateFile(task_key, current_hash_code, self, path=state_file_path)
        if state_file.is_completed():
            pass
            #load inputs and outputs
        else:
            if task_conf["hash_code"] != state_file.hash_code:
                task.save(state_file, current_hash_code)
                state_file.hash_code = current_hash_code
                self.resave_count += 1
        self.load_from_disk_count += 1
        return state_file

    def _iterate_all_tasks_from_disk(self, glob_filter):
        with os.scandir(self.pipeline_work_dir) as pwd_i:
            for task_control_dir_entry in pwd_i:
                if not task_control_dir_entry.is_dir():
                    continue
                if task_control_dir_entry.name == "__pycache__":
                    continue
                if task_control_dir_entry.name == "dry_pipe":
                    continue
                task_control_dir = task_control_dir_entry.path
                task_key = os.path.basename(task_control_dir)

                if glob_filter is not None:
                    if not fnmatch.fnmatch(task_key, glob_filter):
                        continue

                state_file_dir_entry = self._find_state_file_path_in_task_control_dir(task_key)
                if state_file_dir_entry is None:
                    raise Exception(f"no state file exists in {task_control_dir_entry.path}")

                yield task_key, task_control_dir, state_file_dir_entry

    def _load_task_from_state_file(self, task_key, task_control_dir, state_file_path, include_non_completed):
        if state_file_path.endswith("state.completed") or include_non_completed:
            from dry_pipe.task_process import TaskProcess
            yield TaskProcess(
                task_control_dir, ensure_all_upstream_deps_complete= not include_non_completed
            ).resolve_task(
                StateFile(task_key, None, self, path=state_file_path)
            )

    def load_tasks_for_query(self, glob_filter=None, include_non_completed=False):
        for task_key, task_control_dir, state_file_dir_entry in self._iterate_all_tasks_from_disk(glob_filter):
            yield from self._load_task_from_state_file(
                task_key, task_control_dir, state_file_dir_entry, include_non_completed
            )

    def load_single_task_or_none(self, task_key, include_non_completed=False):
        task_control_dir = os.path.join(self.pipeline_work_dir, task_key)
        state_file_path = self._find_state_file_path_in_task_control_dir(task_key)
        if state_file_path is None:
            return None
        else:
            # update mem
            self.fetch_true_state_and_update_memory_if_changed(task_key)
            l = list(self._load_task_from_state_file(
                task_key, task_control_dir, state_file_path, include_non_completed
            ))
            c = len(l)
            if c == 0:
                return None
            elif c == 1:
                return l[0]
            else:
                raise Exception(f"expected zero or one task with key '{task_key}', got {c}")

    def load_state_files_for_run(self, glob_filter=None):
        for task_key, task_control_dir, state_file_path in self._iterate_all_tasks_from_disk(glob_filter):
            state_file = StateFile(
                task_key, None, self, path=state_file_path
            )
            self.state_files_in_memory[task_key] = state_file
            task_conf = self._load_task_conf(task_control_dir)
            if state_file.is_completed():
                yield True, state_file, None
            else:
                upstream_task_keys = set()
                for i in task_conf["inputs"]:
                    k = i.get("upstream_task_key")
                    if k is not None:
                        upstream_task_keys.add(k)
                yield False, state_file, upstream_task_keys

    def create_true_state_if_new_else_fetch_from_memory(self, task):
        """
        :return: (task_is_new, state_file)
        """
        state_file_in_memory = self.state_files_in_memory.get(task.key)
        if state_file_in_memory is not None:
            # state_file_in_memory is assumed up to date, since task declarations don't change between runs
            # by design, DAG generators that violate this assumption are considered at fault
            return False, state_file_in_memory
        else:
            # we have a new task OR process was restarted
            hash_code = task.compute_hash_code()
            state_file_path = self._find_state_file_path_in_task_control_dir(task.key)
            if state_file_path is not None:
                # process was restarted, task is NOT new
                state_file_in_memory = self.load_from_existing_file_on_disc_and_resave_if_required(task, state_file_path)
                self.state_files_in_memory[task.key] = state_file_in_memory
                task.save_if_hash_has_changed(state_file_in_memory, hash_code)
                return False, state_file_in_memory
            else:
                # task is new
                state_file_in_memory = StateFile(task.key, hash_code, self)
                state_file_in_memory.is_slurm_array_child = task.is_slurm_array_child
                if task.is_slurm_parent:
                    state_file_in_memory.is_parent_task = True
                task.save(state_file_in_memory, hash_code)
                state_file_in_memory.touch_initial_state_file(False)
                self.state_files_in_memory[task.key] = state_file_in_memory
                self.new_save_count += 1
                return True, state_file_in_memory
