import os
import subprocess
import time
from paramiko import SSHClient, AutoAddPolicy
import logging.config

import threading

from dry_pipe import Executor
from dry_pipe.bash import bash_shebang
from dry_pipe.internals import flatten
from dry_pipe.task_state import TaskState
from dry_pipe.utils import perf_logger_timer

SSH_TIMEOUT = 120

logger_ssh = logging.getLogger(f"{__name__}.ssh")


class SSHClientHolder:
    def __init__(self):
        self.ssh_client = SSHClient()
        self.ssh_client.set_missing_host_key_policy(AutoAddPolicy)
        self._remote_overrides_uploaded = False

    def is_remote_overrides_uploaded(self):
        return self._remote_overrides_uploaded

    def set_remote_overrides_uploaded(self):
        self._remote_overrides_uploaded = True


class RemoteSSH(Executor):

    def __init__(
        self, ssh_username, ssh_host, remote_base_dir, key_filename, before_execute_bash
    ):
        self.thread_local_ssh_client = threading.local()
        self.before_execute_bash = before_execute_bash
        self.remote_base_dir = remote_base_dir
        self.ssh_host = ssh_host
        self.ssh_username = ssh_username
        self.key_filename = key_filename
        self.dependent_files = []
        self.rsync_containers = True
        self.slurm = None

    def is_remote(self):
        return True

    def __str__(self):

        return f"{self.ssh_username}@{self.ssh_host}:{self.remote_base_dir}"

    def user_at_host(self):
        return f"{self.ssh_username}@{self.ssh_host}"

    def is_remote_overrides_uploaded(self):
        return self.thread_local_ssh_client.h.is_remote_overrides_uploaded()

    def set_remote_overrides_uploaded(self):
        return self.thread_local_ssh_client.h.set_remote_overrides_uploaded()

    def ensure_connected(self):

        if not hasattr(self.thread_local_ssh_client, 'h'):

            self.thread_local_ssh_client.h = SSHClientHolder()
            connection_was_initialized = False
        else:
            connection_was_initialized = True

        try:
            transport = self.ssh_client().get_transport()

            if transport is None:
                logger_ssh.debug("transport None, connection_was_initialized: %s", connection_was_initialized)
                self.connect()
                transport = self.ssh_client().get_transport()

            transport.send_ignore()
        except EOFError as e:
            logger_ssh.warning("ssh connection dead, will reconnect %s", e)
            self.connect()

    def ssh_client(self):
        return self.thread_local_ssh_client.h.ssh_client

    def add_dependent_file(self, file):
        if file not in self.dependent_files:
            self.dependent_files.append(file)

    def add_dependent_dir(self, dir):
        d = f"{dir}/"
        if d not in self.dependent_files:
            self.dependent_files.append(d)

    def server_connection_key(self):
        return f"{self.ssh_username}-{self.ssh_host}"

    def connect(self):
        try:
            with perf_logger_timer("RemoteSSH.connect") as t:
                self.ssh_client().connect(
                    self.ssh_host,
                    username=self.ssh_username,
                    key_filename=os.path.expanduser(self.key_filename)
                        if self.key_filename.startswith("~")
                        else self.key_filename,
                    timeout=SSH_TIMEOUT
                )
        except Exception as ex:
            logger_ssh.error(f"ssh connect failed: {self}")
            raise ex

    def invoke_remote(self, cmd, bash_error_ok=False):
        logger_ssh.debug("will invoke '%s' at %s", cmd, self.ssh_host)

        with perf_logger_timer("RemoteSSH.invoke_remote", cmd) as t:
            stdin, stdout, stderr = self.ssh_client().exec_command(cmd, timeout=SSH_TIMEOUT)

            stdout_txt = stdout.read().decode("utf-8")

        if not bash_error_ok and stdout.channel.recv_exit_status() != 0:
            stderr = stderr.read().decode("utf-8")
            raise Exception(f"remote call failed '{cmd}'\n{stderr}\non {self}")

        logger_ssh.debug(f"invocation of '%s' at {self} returned '%s'", cmd, stdout_txt)

        return stdout_txt

    def fetch_remote_task_states(self, pipeline):
        with perf_logger_timer("RemoteSSH.fetch_remote_task_states") as t:
            remote_pid_basename = os.path.basename(pipeline.pipeline_instance_dir)

            self.ensure_connected()

            cmd = f"find {self.remote_base_dir}/{remote_pid_basename}/.drypipe/*/state.* 2>/dev/null || true"
            stdout = self.invoke_remote(cmd)

            return [
                TaskState(f)
                for f in stdout.strip().split("\n")
                if f != ""
            ]

    def fetch_logs_and_history(self, task):

        self.ensure_connected()

        remote_pid_basename = os.path.basename(task.pipeline.pipeline_instance_dir)

        #Not called because too slow !
        def fetch_remote_state():
            cmd = f"find {self.remote_base_dir}/{remote_pid_basename}/.drypipe/{task.key}/state.* 2>/dev/null"

            stdout = self.invoke_remote(cmd)

            return TaskState(stdout.strip())

        with self.ssh_client().open_sftp() as sftp:

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
            os.path.basename(task.pipeline.pipeline_instance_dir),
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
        self.ensure_connected()
        self.invoke_remote(f"rm {state_file}", bash_error_ok=True)

    def kill_slurm_task(self, task):

        slurm_job_id_file = self._file_in_control_dir(task, "slurm_job_id")
        self.ensure_connected()

        with self.ssh_client().open_sftp() as sftp:

            with sftp.open(slurm_job_id_file) as _f:
                slurm_job_id = _f.read().strip().decode("utf-8")
                self.invoke_remote(f"scancel {slurm_job_id}")

    def delete_remote_state_file(self, file):

        self.ensure_connected()

        self.invoke_remote(f"rm {file}")

    def _rsync_with_args_and_remote_dir(self):

        ssh_args = f"-e 'ssh -q -i {self.key_filename} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'"

        timeout = f"--timeout={60 * 2}"

        return (
            f"rsync {ssh_args} {timeout}",
            f"{self.ssh_username}@{self.ssh_host}:{self.remote_base_dir}"
        )

    def _launch_command(self, command, exception_func=lambda stderr_text: stderr_text):

        with subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            text=True
        ) as p:
            p.wait()

            if p.returncode != 0:
                raise exception_func(p.stderr.read().strip())

    def do_rsync_container(self, image_path):

        rsync_call, remote_dir = self._rsync_with_args_and_remote_dir()

        bn = os.path.basename(image_path)
        dn = os.path.dirname(image_path)

        self.ensure_connected()

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

            self.ensure_connected()

            self.invoke_remote(f"mkdir -p {remote_pipeline_code_dir}")

            cmd = f"{rsync_call} -az --partial " + \
                  f"{pipeline.pipeline_code_dir}/ {self.ssh_username}@{self.ssh_host}:{remote_pipeline_code_dir}"

            self._launch_command(
                cmd,
                lambda err: Exception(f"rsync of remote_pipeline_code_dir failed:\n{cmd}\n{err}")
            )

    def upload_task_inputs(self, task_state, task):
        with perf_logger_timer("RemoteSSH.upload_task_inputs") as t:

            task_control_dir = task_state.control_dir()

            rsync_call, remote_dir = self._rsync_with_args_and_remote_dir()
            pipeline_instance_dir = os.path.dirname(os.path.dirname(task_control_dir))

            remote_pid_basename = os.path.basename(pipeline_instance_dir)

            self.ensure_connected()

            self.invoke_remote(f"mkdir -p {self.remote_base_dir}/{remote_pid_basename}")

            cmd = f"{rsync_call} -aRz --partial --recursive --files-from={task_control_dir}/local-deps.txt " + \
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

    def upload_overrides(self, pipeline, task_conf):

        if self.is_remote_overrides_uploaded():
            return

        pipeline_instance_dir = pipeline.pipeline_instance_dir
        remote_pid_basename = os.path.basename(pipeline_instance_dir)

        overrides_file_for_host = os.path.join(
            pipeline._work_dir,
            f"pipeline-env-{self.server_connection_key()}.sh"
        )

        remote_pipeline_code_dir = task_conf.remote_pipeline_code_dir
        remote_containers_dir = task_conf.remote_containers_dir

        def gen_remote_overrides():
            yield "__pipeline_code_dir", remote_pipeline_code_dir
            yield "__containers_dir", remote_containers_dir

        remote_overrides = [
            f"export {k}={v}\n"
            for k, v in gen_remote_overrides()
            if v is not None
        ]

        if len(remote_overrides) == 0:
            return

        with open(overrides_file_for_host, "w") as _f:
            _f.write(f"{bash_shebang()}\n\n")
            _f.writelines(remote_overrides)
            _f.write("\n")

        #if not os.path.exists(overrides_file_for_host):
        #    return

        with perf_logger_timer("RemoteSSH.upload_overrides") as t:

            r_work_dir = os.path.join(self.remote_base_dir, remote_pid_basename, ".drypipe")
            r_override_file = os.path.join(
                r_work_dir,
                "pipeline-env.sh"
            )

            self.ensure_connected()

            def upload_overrides_broken_by_paramiko_bug():
                self.invoke_remote(f"mkdir -p {r_work_dir}")
                with self.ssh_client().open_sftp() as sftp:
                    sftp.put(overrides_file_for_host, r_override_file, confirm=False)

            def upload_overrides():

                def lines_in_overrides_file():
                    yield f"mkdir -p {r_work_dir}"
                    yield f"echo '{bash_shebang()}' > {r_override_file}"
                    if remote_pipeline_code_dir is not None:
                        yield f'echo "export __pipeline_code_dir={remote_pipeline_code_dir}" >> {r_override_file}'
                    if remote_containers_dir is not None:
                        yield f'echo "export __containers_dir={remote_containers_dir}" >> {r_override_file}'

                    yield f"chmod u+x {r_override_file}"

                for i in range(1, 3):
                    try:
                        self.invoke_remote("pwd")
                        break
                    except Exception as ex:
                        if i >= 3:
                            raise ex
                        else:
                            time.sleep(2)

                remote_cmd = " && ".join(lines_in_overrides_file())
                self.invoke_remote(remote_cmd)

            upload_overrides()

            self.set_remote_overrides_uploaded()

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
        with perf_logger_timer("RemoteSSH.execute") as t:
            b4_command = ""
            if self.before_execute_bash is not None:
                b4_command = f"{self.before_execute_bash} &&"

            remote_pid_basename = os.path.basename(task.pipeline_instance.pipeline_instance_dir)

            def remote_base_dir(p):
                return os.path.join(self.remote_base_dir, remote_pid_basename, p)

            r_script, r_sbatch_script, r_out, r_err, r_work_dir, r_control_dir = map(remote_base_dir, [
                task.script_file(),
                task.sbatch_launch_script(),
                task.out_log(),
                task.err_log(),
                task.work_dir(),
                task.control_dir()
            ])

            self.ensure_connected()

            self.invoke_remote(f"mkdir -p {r_work_dir}")
            self.invoke_remote(f"mkdir -p {r_control_dir}/out_sigs")

            if task.scratch_dir:
                self.invoke_remote(f"mkdir -p {remote_base_dir(task.scratch_dir())}")

            self.invoke_remote(f"rm {r_control_dir}/state.*", bash_error_ok=True)

            self.invoke_remote(f"touch {r_control_dir}/state.launched.0.0.1")

            if self.slurm is None:

                ampersand_or_not = "&"

                if wait_for_completion:
                    ampersand_or_not = ""

                cmd = f"nohup bash -c '{b4_command} . {r_script}  >{r_out} 2>{r_err}' {ampersand_or_not}"
            else:
                if wait_for_completion:
                    cmd = f"bash -c 'export SBATCH_WAIT=True && {r_sbatch_script}'"
                else:
                    cmd = r_sbatch_script

            self.invoke_remote(cmd)
