import os
import tempfile
import logging.config

from dry_pipe import Executor, bash_shebang
from dry_pipe.internals import flatten
from dry_pipe.script_lib import PortablePopen, create_task_logger
from dry_pipe.task_state import TaskState
from dry_pipe.utils import perf_logger_timer

logger_ssh = logging.getLogger(f"{__name__}.ssh")

class ScpUploadException(Exception):
    pass


class RemoteSSH(Executor):

    def __init__(
        self, ssh_username, ssh_host, remote_base_dir, key_filename, before_execute_bash
    ):
        if key_filename is None:
            raise Exception("key_filename can't be none")

        self.before_execute_bash = before_execute_bash
        self.remote_base_dir = remote_base_dir
        self.ssh_host = ssh_host
        self.ssh_username = ssh_username
        self.key_filename = key_filename
        self.rsync_containers = True
        self.slurm = None
        self.ssh_timeout_in_seconds = 60
        self.ssh_client = None

    def is_remote(self):
        return True

    def user_at_host(self):
        return f"{self.ssh_username}@{self.ssh_host}"

    def server_connection_key(self):
        return f"{self.ssh_username}-{self.ssh_host}"

    def connect(self):
        from pssh.clients import SSHClient
        pkey = os.path.expanduser(self.key_filename) \
            if self.key_filename is not None and self.key_filename.startswith("~") else self.key_filename
        try:
            with perf_logger_timer("RemoteSSH.connect") as t:
                self.ssh_client = SSHClient(
                    self.ssh_host,
                    self.ssh_username,
                    pkey=pkey
                )
            logger_ssh.info("new ssh connection established %s at %s", self.ssh_username, self.ssh_host)
        except Exception as ex:
            logger_ssh.exception("%s", [self.ssh_host, self.ssh_username, pkey])
            raise ex

    def invoke_remote(self, cmd, bash_error_ok=False):
        logger_ssh.debug("will invoke '%s' at %s", cmd, self.ssh_host)

        with perf_logger_timer("RemoteSSH.invoke_remote", cmd) as t:

            host_out = self.ssh_client.run_command(cmd)
            stdout_txt = "\n".join(list(host_out.stdout))
            stderr_lines = list(host_out.stderr)

        if not bash_error_ok and host_out.exit_code != 0:
            stderr_txt = "\n".join(stderr_lines)
            raise Exception(f"remote call failed {host_out.exit_code}, '{cmd}'\n{stderr_txt}\non {self}")

        logger_ssh.debug(f"invocation of '%s' at {self} returned '%s'", cmd, stdout_txt)

        return stdout_txt

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self.ssh_client.disconnect()

    @staticmethod
    def exception_is_recoverable_with_connection_reset(ex):
        from pssh.exceptions import SSHException, SSHError, SFTPError
        if isinstance(ex, SSHException) or \
           isinstance(ex, SFTPError) or \
           isinstance(ex, SSHError):
            return True
        else:
            return False

    def fetch_remote_task_states(self, pipeline):
        with perf_logger_timer("RemoteSSH.fetch_remote_task_states") as t:
            remote_pid_basename = os.path.basename(pipeline.pipeline_instance_dir)

            cmd = f"find {self.remote_base_dir}/{remote_pid_basename}/.drypipe/*/state.* 2>/dev/null || true"
            stdout = self.invoke_remote(cmd)

            return [
                TaskState(f)
                for f in stdout.strip().split("\n")
                if f != ""
            ]

    def fetch_logs_and_history(self, task):

        remote_pid_basename = os.path.basename(task.pipeline_instance.pipeline_instance_dir)

        #Not called because too slow !
        def fetch_remote_state():
            cmd = f"find {self.remote_base_dir}/{remote_pid_basename}/.drypipe/{task.key}/state.* 2>/dev/null"

            stdout = self.invoke_remote(cmd)

            return TaskState(stdout.strip())

        with self.ssh_client.open_sftp() as sftp:

            def file_content_and_last_mod_time(p):
                f = os.path.join(self.remote_base_dir, remote_pid_basename, p)
                s = sftp.stat(f)
                with sftp.open(f) as _f:
                    return [_f.read(), s.st_mtime]

            f_out, t_out, \
            f_err, t_err, \
            f_history_file, t_history_file = flatten(map(file_content_and_last_mod_time, [
                task.out_log(),
                task.err_log(),
                task.history_file()
            ]))

            last_activity_time = max([t_err, t_out, t_history_file])

            return f_out.decode("utf-8"), f_err.decode("utf-8"), f_history_file.decode("utf-8"), last_activity_time

    def _remote_control_dir(self, task):
        return os.path.join(
            self.remote_base_dir,
            os.path.basename(task.pipeline_instance.pipeline_instance_dir),
            ".drypipe",
            task.key
        )

    def _file_in_control_dir(self, task, file):
        return os.path.join(
            self._remote_control_dir(task),
            file
        )

    def clear_remote_task_state(self, task):
        state_file = self._file_in_control_dir(task, "state.*")
        self.invoke_remote(f"rm {state_file}", bash_error_ok=True)

    def kill_slurm_task(self, task):

        slurm_job_id_file = self._file_in_control_dir(task, "slurm_job_id")

        with self.ssh_client.open_sftp() as sftp:

            with sftp.open(slurm_job_id_file) as _f:
                slurm_job_id = _f.read().strip().decode("utf-8")
                self.invoke_remote(f"scancel {slurm_job_id}")

    def delete_remote_state_file(self, file):

        self.invoke_remote(f"rm {file}")

    def _rsync_with_args_and_remote_dir(self):

        ssh_args = f"-e 'ssh -q -i {self.key_filename} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'"

        timeout = f"--timeout={60 * 2}"

        return (
            f"rsync {ssh_args} {timeout}",
            f"{self.ssh_username}@{self.ssh_host}:{self.remote_base_dir}"
        )

    def _launch_command(self, command, exception_func=lambda stderr_text: stderr_text):

        with PortablePopen(command,shell=True) as p:
            p.wait_and_raise_if_non_zero()

    def do_rsync_container(self, image_path):

        rsync_call, remote_dir = self._rsync_with_args_and_remote_dir()

        bn = os.path.basename(image_path)
        dn = os.path.dirname(image_path)

        self.invoke_remote(f"mkdir -p {self.remote_base_dir}/pipeline_code_dir")

        cmd = f"{rsync_call} -az --partial {image_path} {remote_dir}/pipeline_code_dir"

        self._launch_command(
            cmd,
            lambda err: Exception(f"uploading container {bn} failed:\n{cmd}\n{err}")
        )

    def rsync_remote_code_dir_if_applies(self, pipeline, task_conf):

        remote_pipeline_code_dir = task_conf.remote_pipeline_code_dir

        if remote_pipeline_code_dir is None:
            return

        with perf_logger_timer("RemoteSSH.rsync_remote_code_dir_if_applies") as t:

            rsync_call, remote_dir = self._rsync_with_args_and_remote_dir()

            self.invoke_remote(f"mkdir -p {remote_pipeline_code_dir}")

            cmd = f"{rsync_call} -az --partial " + \
                  f"{pipeline.pipeline_code_dir}/ {self.ssh_username}@{self.ssh_host}:{remote_pipeline_code_dir}"

            self._launch_command(
                cmd,
                lambda err: Exception(f"rsync of remote_pipeline_code_dir failed:\n{cmd}\n{err}")
            )

    def upload_task_inputs(self, task_state):
        with perf_logger_timer("RemoteSSH.upload_task_inputs") as t:

            task_control_dir = task_state.control_dir()

            rsync_call, remote_dir = self._rsync_with_args_and_remote_dir()
            pipeline_instance_dir = os.path.dirname(os.path.dirname(task_control_dir))

            remote_pid_basename = os.path.basename(pipeline_instance_dir)

            self.invoke_remote(f"mkdir -p {self.remote_base_dir}/{remote_pid_basename}")

            cmd = f"{rsync_call} -caRz --partial --recursive --files-from={task_control_dir}/local-deps.txt " + \
                  f"{pipeline_instance_dir} {remote_dir}/{remote_pid_basename}"

            self._launch_command(
                cmd,
                lambda err: Exception(f"uploading of task inputs {task_control_dir} failed:\n{cmd}\n{err}")
            )

            # NO LONGER SUPPORTED
            # if self.rsync_containers and task.container is not None:
            #    self.do_rsync_container(task.container.image_path)

    def download_task_results(self, task_state):
        with perf_logger_timer("RemoteSSH.download_task_results") as t:
            task_control_dir = task_state.control_dir()

            rsync_call, remote_dir = self._rsync_with_args_and_remote_dir()
            pipeline_instance_dir = os.path.dirname(os.path.dirname(task_control_dir))
            remote_pid_basename = os.path.basename(pipeline_instance_dir)

            cmd = f"{rsync_call} --dirs -aR --partial --files-from={task_control_dir}/remote-outputs.txt " + \
                  f"{remote_dir}/{remote_pid_basename} {pipeline_instance_dir}"

            def error_msg(err):
                r = f"{task_control_dir}/remote-outputs.txt"
                return Exception(
                    f"downloading of task results {r} from {self.ssh_host} failed:\n{cmd}\n{err}"
                )

            self._launch_command(cmd, error_msg)

    def fetch_remote_file(self, remote_file, local_file):
        with perf_logger_timer("RemoteSSH.fetch_remote_file") as t:
            self.ssh_client.copy_remote_file(remote_file, local_file)

    def fetch_remote_file_content(self, remote_file):
        with tempfile.NamedTemporaryFile() as tmp:
            self.fetch_remote_file(remote_file, tmp.name)
            with open(tmp.name) as _f:
                return _f.read()

    def upload_file(self, local_file, remote_file):
        with perf_logger_timer("RemoteSSH.upload_file") as t:
            self.ssh_client.copy_file(local_file, remote_file)

    def prepare_remote_instance_directory(self, pipeline_instance, task_conf):

        if pipeline_instance.is_remote_instance_directory_prepared(self.server_connection_key()):
            return

        pipeline_instance_dir = pipeline_instance.pipeline_instance_dir
        remote_pid_basename = os.path.basename(pipeline_instance_dir)

        overrides_file_for_host = os.path.join(
            pipeline_instance.work_dir,
            f"pipeline-env-{self.server_connection_key()}.sh"
        )

        logger_ssh.info(f"will upload overrides: '%s'", overrides_file_for_host)

        remote_pipeline_code_dir = task_conf.remote_pipeline_code_dir
        remote_containers_dir = task_conf.remote_containers_dir

        if remote_containers_dir is None:
            if remote_pipeline_code_dir is not None:
                remote_containers_dir = os.path.join(remote_pipeline_code_dir, "containers")

        def gen_remote_overrides():
            yield "__pipeline_code_dir", remote_pipeline_code_dir
            if remote_containers_dir is not None:
                yield "__containers_dir", remote_containers_dir

        remote_overrides = [
            f"export {k}={v}\n"
            for k, v in gen_remote_overrides()
            if v is not None
        ]

        r_work_dir = os.path.join(self.remote_base_dir, remote_pid_basename, ".drypipe")

        if len(remote_overrides) > 0:
            logger_ssh.info(f"no overrides to upload")

            with open(overrides_file_for_host, "w") as _f:
                _f.write(f"{bash_shebang()}\n\n")
                _f.writelines(remote_overrides)
                _f.write("\n")

            r_override_file = os.path.join(
                r_work_dir,
                "pipeline-env.sh"
            )

            self.upload_file(overrides_file_for_host, r_override_file)

        def local_to_remote(file):
            return os.path.join(pipeline_instance.work_dir, file), os.path.join(r_work_dir, file)

        self.upload_file(*local_to_remote('script_lib.py'))
        script_lib_l, script_lib_r = local_to_remote('script_lib')
        self.upload_file(script_lib_l, script_lib_r)
        self.invoke_remote(f"chmod u+x {script_lib_r}")

        pipeline_instance.set_remote_instance_directory_prepared(self.server_connection_key())

    """
        Fetches log lines and history.txt for all tasks, done once every janitor run, instead of before each task
    """
    def fetch_new_log_lines(self, pipeline_instance_dir):
        with perf_logger_timer("RemoteSSH.fetch_new_log_lines") as t:

            rsync_call, remote_dir = self._rsync_with_args_and_remote_dir()
            remote_pid_basename = os.path.basename(pipeline_instance_dir)
            dst = os.path.dirname(pipeline_instance_dir)

            cmd = " ".join([
                f" {rsync_call} -r --partial --append",
                " --filter='+ .drypipe/*/*.log'",
                " --filter='+ .drypipe/*/history.tsv'",
                " --filter='- .drypipe/*/*'",
                " --filter='- publish'",
                " --filter='- *.sh'",
                f" {remote_dir}/{remote_pid_basename} {dst}"
            ])

            logger_ssh.debug("will rsync new log lines \n%s", cmd)

            self._launch_command(
                cmd,
                lambda err: Exception(
                    f"fetching logs failed:\n{cmd}\n{err}")
            )

    def execute(self, task, touch_pid_file_func, wait_for_completion=False, fail_silently=False):

        task_local_logger = create_task_logger(os.path.join(task.v_abs_control_dir()))
        try:

            remote_pid_basename = os.path.basename(task.pipeline_instance.pipeline_instance_dir)

            remote_pipeline_instance_dir = os.path.join(self.remote_base_dir, remote_pid_basename)

            remote_script_lib_path = os.path.join(
                remote_pipeline_instance_dir,
                '.drypipe',
                'script_lib'
            )

            drypipe_task_debug = os.environ.get("DRYPIPE_TASK_DEBUG") == "True"

            cmd = " ".join([
                remote_script_lib_path,
                "launch-task-from-remote",
                task.key,
                "--is-slurm" if task.task_conf.is_slurm() else "",
                "--wait-for-completion" if wait_for_completion else "",
                "--drypipe-task-debug" if drypipe_task_debug else ""
            ])

            task_local_logger.info("will execute")

            task_local_logger.debug(f"will launch task on %s: %s", self.ssh_host, cmd)

            with perf_logger_timer("RemoteSSH.execute") as t:
                res = self.invoke_remote(cmd)
                try:
                    res_num = int(res)
                except Exception as ex:
                    raise Exception(f"remote command {cmd} returned invalid result: {res}")
                if res_num != 0:
                    raise Exception(f"remote command {cmd} failed with return code: {res_num}")
        finally:
            task_local_logger.handlers[0].close()
