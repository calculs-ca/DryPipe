import importlib
import os
import subprocess


class RetryableRsyncException(Exception):
    def __init__(self, message):
        super().__init__(message)


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




def invoke_rsync(command):
    def _rsync_error_codes(code):
        """
        Error codes as documented here: https://download.samba.org/pub/rsync/rsync.1
        :param code rsync error code:
        :return (is_retryable, message):
        """
        if code == 1:
            return False, 'Syntax or usage error'
        elif code == 2:
            return False, 'Protocol incompatibility'
        elif code == 3:
            return True, 'Errors selecting input/output'
        elif code == 4:
            return False, 'Requested action not supported'
        elif code == 5:
            return True, 'Error starting client-serve'
        elif code == 6:
            return True, 'Daemon unable to append to log-file'
        elif code == 10:
            return True, 'Error in socket I/O'
        elif code == 11:
            return True, 'Error in file I/O'
        elif code == 12:
            return True, 'Error in rsync protocol data stream'
        elif code == 13:
            return True, 'Errors with program diagnostics'
        elif code == 14:
            return True, 'Error in IPC code'
        elif code == 20:
            return True, 'Received SIGUSR1 or SIGINT'
        elif code == 21:
            return True, 'Some error returned by waitpid()'
        elif code == 22:
            return True, 'Error allocating core memory'
        elif code == 23:
            return True, 'Partial transfer due to error'
        elif code == 24:
            return True, 'Partial transfer due to vanished source file'
        elif code == 25:
            return True, 'The --max-delete limit stopped deletions'
        elif code == 30:
            return True, 'Timeout in data send/received'
        elif code == 35:
            return True, 'Timeout waiting for daemon'
        else:
            return False, f'unknown error: {code}'

    with PortablePopen(command, shell=True) as p:
        p.popen.wait()
        if p.popen.returncode != 0:
            is_retryable, rsync_message = _rsync_error_codes(p.popen.returncode)
            msg = f"rsync failed: {rsync_message}, \n'{command}' \n{p.safe_stderr_as_string()}"
            if not is_retryable:
                raise Exception(msg)
            else:
                raise RetryableRsyncException(msg)


def exec_remote(user_at_host, cmd):
    with PortablePopen(["ssh", user_at_host, " ".join(cmd)]) as p:
        p.wait_and_raise_if_non_zero()
        return p.stdout_as_string()
