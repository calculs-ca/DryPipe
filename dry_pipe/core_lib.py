import fnmatch
import glob
import json
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import textwrap
import logging
import logging.config
import traceback
from datetime import datetime
from functools import reduce
from pathlib import Path
from threading import Thread

#APPTAINER_COMMAND="apptainer"
APPTAINER_COMMAND="singularity"

APPTAINER_BIND="APPTAINER_BIND"


def init_logger(_logger, filename, logging_level):
    file_handler = logging.FileHandler(filename=filename)
    file_handler.setLevel(logging_level)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S%z')
    )
    _logger.addHandler(file_handler)
    return _logger

def create_task_logger(task_control_dir):
    if os.environ.get("DRYPIPE_TASK_DEBUG") != "True":
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG
    _logger = logging.Logger("task-logger", logging_level)
    return init_logger(
        _logger,
        os.path.join(task_control_dir, "drypipe.log"),
        logging_level
    )


def create_remote_janitor_logger(pipeline_instance_dir, logger_name, is_debug):
    if is_debug:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO
    _logger = logging.Logger(logger_name, logging_level)
    return init_logger(
        _logger,
        os.path.join(pipeline_instance_dir, ".drypipe", f"{logger_name}.log"),
        logging_level
    )

__script_location = os.environ.get("__script_location")


if __script_location is None:
    logger = logging.getLogger(__name__)
else:
    logger = create_task_logger(__script_location)

def parse_ssh_specs(ssh_specs):
    ssh_specs_parts = ssh_specs.split(":")
    if len(ssh_specs_parts) == 2:
        ssh_username_ssh_host, key_filename = ssh_specs_parts
    elif len(ssh_specs_parts) == 1:
        ssh_username_ssh_host = ssh_specs_parts[0]
        key_filename = "~/.ssh/id_rsa"
    else:
        raise Exception(f"bad ssh_specs format: {ssh_specs}")

    ssh_username, ssh_host = ssh_username_ssh_host.split("@")

    return ssh_username, ssh_host, key_filename

def _fail_safe_stderr(process):
    try:
        err = process.stderr.read().decode("utf-8")
    except Exception as _e:
        err = ""
    return err


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


def python_shebang():
    return "#!/usr/bin/env python3"


def write_pipeline_lib_script(file_handle):
    file_handle.write(
        f"{python_shebang()}\n" + textwrap.dedent(f"""            
            import os
            import sys 
            import importlib.machinery
            import importlib.util        

            __script_location = os.path.dirname(os.path.abspath(__file__))
            script_lib_path = os.path.join(__script_location, 'core_lib.py')        
            loader = importlib.machinery.SourceFileLoader('core_lib', script_lib_path)
            spec = importlib.util.spec_from_loader(loader.name, loader)
            core_lib = importlib.util.module_from_spec(spec)
            loader.exec_module(core_lib)
        """)
    )

    file_handle.write(textwrap.dedent(f"""        
    if __name__ == '__main__':        
        core.handle_script_lib_main()
    """))


def task_script_header():

    is_zombi_test_case = os.environ.get("IS_ZOMBI_TEST_CASE") == "True"
    bsheb = python_shebang() if not is_zombi_test_case else "#!/brokenshebang"

    return f"{bsheb}\n" + textwrap.dedent(f"""            
        import os 
        import importlib.machinery
        import importlib.util    
        import logging    

        is_slurm = os.environ.get("__is_slurm") == "True"        
        if is_slurm:
            __script_location = os.environ['__script_location']
        else:
            __script_location = os.path.dirname(os.path.abspath(__file__))
            os.environ["__script_location"] = __script_location
        script_lib_path = os.path.join(os.path.dirname(__script_location), 'core_lib.py')        
        loader = importlib.machinery.SourceFileLoader('core_lib', script_lib_path)
        spec = importlib.util.spec_from_loader(loader.name, loader)
        core_lib = importlib.util.module_from_spec(spec)
        loader.exec_module(core_lib)        
    """)

class UpstreamTasksNotCompleted(Exception):
    def __init__(self, upstream_task_key, msg):
        self.upstream_task_key = upstream_task_key
        self.msg = msg


class TaskProcess:

    @staticmethod
    def run(control_dir, as_subprocess=True, wait_for_completion=False):

        if not as_subprocess:
            TaskProcess(control_dir).launch_task(wait_for_completion, exit_process_when_done=False)
        else:
            task_script = os.path.join(control_dir, "task")
            if wait_for_completion:
                cmd = [task_script, "--wait"]
            else:
                cmd = [task_script]

            logger.debug("will launch task %s", ' '.join(cmd))

            with PortablePopen(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
            ) as p:
                p.wait()
                if p.popen.returncode != 0:
                    logger.warning("task ended with non zero code: %s", p.popen.returncode)
                else:
                    logger.debug("task ended with returncode: %s", p.popen.returncode)

    def __init__(self, control_dir):
        self.control_dir = control_dir
        self.task_key = os.path.basename(control_dir)
        self.pipeline_work_dir = os.path.dirname(control_dir)
        self.pipeline_instance_dir = os.path.dirname(self.pipeline_work_dir)
        self.pipeline_output_dir = os.path.join(self.pipeline_instance_dir, "output")
        self.task_output_dir = os.path.join(self.pipeline_output_dir, self.task_key)
        self.task_conf = None

    def _exit_process(self):
        self._delete_pid_and_slurm_job_id()
        logging.shutdown()
        os._exit(0)

    def call_python(self, mod_func, python_task):

        task_logger = create_task_logger(self.control_dir)

        pythonpath_in_env = os.environ.get("PYTHONPATH")

        if pythonpath_in_env is not None:
            for p in pythonpath_in_env.split(":"):
                if not os.path.exists(p):
                    msg = f"WARNING: path {p} in PYTHONPATH does not exist, if running in apptainer, ensure proper mount"
                    print(msg, file=sys.stderr)
                    task_logger.warning(msg)

        inputs_by_name = {}
        file_outputs_by_name = {}
        # python_calls can access outputs of previous ones with input args
        var_outputs_by_name = {}

        for task_input, k, v in self.resolve_upstream_and_constant_vars():
            inputs_by_name[k] = task_input.parse(v)

        for o, k, f in self.iterate_file_task_outputs():
            file_outputs_by_name[k] = f

        for o in self.task_conf["outputs"]:
            o = TaskOutput.from_json(o)
            if not o.is_file():
                v = os.environ.get(o.name)
                if v is not None:
                    var_outputs_by_name[o.name] = o.parse(v)

        all_function_input_candidates = {
            ** os.environ,
            ** inputs_by_name,
            ** file_outputs_by_name,
            ** var_outputs_by_name
        }

        def get_arg(k):
            v = all_function_input_candidates.get(k)
            if v is None and k != "test":
                tk = self.task_key
                raise Exception(
                    f"Task {tk} called {mod_func} with None assigned to arg {k}\n" +
                    f"make sure task has var {k} declared in it's consumes clause. Ex:\n" +
                    f"  dsl.task(key={tk}).consumes({k}=...)"
                )
            return v

        args_tuples = [
            (k, get_arg(k))
            for k, v in python_task.signature.parameters.items()
            if not k == "kwargs"
        ]

        args = [v for _, v in args_tuples]
        args_names = [k for k, _ in args_tuples]
        task_logger.debug("args list: %s", args_names)

        if "kwargs" not in python_task.signature.parameters:
            kwargs = {}
        else:
            kwargs = {
                k : v
                for k, v in all_function_input_candidates.items()
                if k not in args_names
            }

        task_logger.info("will invoke PythonCall: %s(%s,%s)", mod_func, args, kwargs)

        try:
            out_vars = python_task.func(* args, ** kwargs)
        except Exception:
            traceback.print_exc()
            logging.shutdown()
            exit(1)

        if out_vars is not None and not isinstance(out_vars, dict):
            raise Exception(
                f"function {python_task.mod_func()} called by task {self.task_key} {type(out_vars)}" +
                f"@DryPipe.python_call() can only return a python dict, or None"
            )

        try:
            if out_vars is not None:
                prev_out_vars = dict(self.iterate_out_vars_from())

                for k, v in out_vars.items():
                    try:
                        v = json.dumps(v)
                        prev_out_vars[k] = v
                    except TypeError as ex0:
                        print(
                            f"task call {self.control_dir}.{mod_func} returned var {k} of unsupported type: {type(v)}" +
                            " only primitive types are supported.",
                            file=sys.stderr
                        )
                        logging.shutdown()
                        exit(1)

                self.write_out_vars(prev_out_vars)

            #for pid_file in glob.glob(os.path.join(control_dir, "*.pid")):
            #    os.remove(pid_file)

        except Exception as ex:
            traceback.print_exc()
            logging.shutdown()
            exit(1)

        logging.shutdown()

    def resolve_task_env(self):
        script_location = self.control_dir
        with open(os.path.join(script_location, "task-conf.json")) as _task_conf:
            self.task_conf = json.loads(_task_conf.read())

            self.set_generic_task_env()

            command_before_task = self.task_conf.get("command_before_task")

            if command_before_task is not None:
                self.exec_cmd_before_launch(command_before_task)

            def override_deps(dep_file, bucket):
                dep_file = os.path.join(os.path.join(script_location, dep_file))
                if not os.path.exists(dep_file):
                    return

                file_cache = os.path.join(self.task_conf["remote_base_dir"], "file-cache", bucket)

                logger.debug("will resolve external file deps: %s", file_cache)

                with open(dep_file) as f:
                    deps = {l.strip() for l in f.readlines() if l != ""}
                    for k, v in os.environ.items():
                        if v in deps:
                            if v.startswith("/"):
                                v = v[1:]
                            new_v = os.path.join(file_cache, v)
                            self.env[k] = new_v
                            logger.debug("override var '%s' -> '%s'", k, new_v)

            #override_deps("external-deps.txt", "shared")
            #override_deps("external-deps-pipeline-instance.txt", os.environ["__pipeline_instance_name"])

            #for k, v in resolve_upstream_vars(task_conf_as_json):
            #    if v is not None:
            #        os.environ[k] = v

    def set_generic_task_env(self):
        self.env = {}

        for k, v in self.iterate_task_env():
            v = str(v)
            logger.debug("env var %s = %s", k, v)
            self.env[k] = v

    def resolve_upstream_and_constant_vars(self, log_error_if_upstream_task_not_completed=True):

        for i in self.task_conf["inputs"]:
            i = TaskInput.from_json(i)
            if logger.level == logging.DEBUG:
                logger.debug("%s", i.as_string())
            if i.is_upstream_output():

                def ensure_upstream_task_is_completed():
                    for state_file in glob.glob(
                            os.path.join(self.pipeline_work_dir, i.upstream_task_key, "state.*")):
                        if not "completed" in state_file:
                            msg = f"upstream task {i.upstream_task_key} " + \
                                  f"not completed (state={state_file}), this task " + \
                                  f"dependency on {i.name_in_upstream_task} not satisfied"
                            if not log_error_if_upstream_task_not_completed:
                                logger.error(msg)
                            else:
                                raise UpstreamTasksNotCompleted(i.upstream_task_key, msg)

                ensure_upstream_task_is_completed()

                if i.is_file():
                    yield i, i.name, os.path.join(self.pipeline_output_dir, i.upstream_task_key, i.file_name)
                else:
                    out_vars = dict(self.iterate_out_vars_from(
                        os.path.join(self.pipeline_work_dir, i.upstream_task_key, "output_vars")
                    ))
                    v = out_vars.get(i.name_in_upstream_task)
                    yield i, i.name, v
            elif i.is_constant():
                yield i, i.name, i.value
            elif i.is_file():
                # not is_upstream_output means they have either absolute path, or in pipeline_instance_dir
                if os.path.isabs(i.file_name):
                    yield i, i.name, i.file_name
                else:
                    yield i, i.name, os.path.join(self.pipeline_instance_dir, i.file_name)

    def iterate_out_vars_from(self, file=None):
        if file is None:
            file = self.env["__output_var_file"]
        if os.path.exists(file):
            with open(file) as f:
                for line in f.readlines():
                    var_name, value = line.split("=")
                    yield var_name.strip(), value.strip()

    def iterate_task_env(self):
        pipeline_conf = Path(self.pipeline_work_dir, "conf.json")
        if os.path.exists(pipeline_conf):
            with open(pipeline_conf) as pc:
                pipeline_conf_json = json.loads(pc.read())

                def en_vars_in_pipeline(*var_names):
                    for name in var_names:
                        value = pipeline_conf_json.get(name)
                        if value is not None:
                            value = os.path.expandvars(value)
                            yield name, value


                yield from en_vars_in_pipeline(
                    "__containers_dir",
                    "__pipeline_code_dir"
                )

        yield "__pipeline_instance_dir", self.pipeline_instance_dir
        yield "__pipeline_instance_name", os.path.basename(self.pipeline_instance_dir)
        yield "__control_dir", self.control_dir
        yield "__task_key", self.task_key
        yield "__task_output_dir", self.task_output_dir
        yield "__scratch_dir", os.path.join(self.task_output_dir, "scratch")
        yield "__output_var_file", os.path.join(self.control_dir, "output_vars")
        yield "__out_log", os.path.join(self.control_dir, "out.log")
        yield "__err_log", os.path.join(self.control_dir, "err.log")
        if self.task_conf.get("ssh_specs") is not None:
            yield "__is_remote", "True"
        else:
            yield "__is_remote", "False"

        container = self.task_conf.get("container")
        if container is not None and container != "":
            yield "__is_singularity", "True"
        else:
            yield "__is_singularity", "False"

        logger.debug("extra_env vars from task-conf.json")

        extra_env = self.task_conf["extra_env"]
        if extra_env is not None:
            for k, v in extra_env.items():
                yield k, os.path.expandvars(v)

        logger.debug("resolved and constant input vars")
        for _, k, v in self.resolve_upstream_and_constant_vars():
            yield k, v

        logger.debug("file output vars")
        for _, k, f in self.iterate_file_task_outputs():
            yield k, f


    def iterate_file_task_outputs(self):
        for o in self.task_conf["outputs"]:
            o = TaskOutput.from_json(o)
            if o.is_file():
                yield o, o.name, os.path.join(self.task_output_dir, o.produced_file_name)


    def run_python(self, mod_func, container=None):

        switches = "-u"
        cmd = [
            self.task_conf["python_bin"],
            switches,
            "-m",
            "dry_pipe.cli",
            "call",
            mod_func
        ]

        if container is not None:
            cmd = [
                APPTAINER_COMMAND,
                "exec",
                self.resolve_container_path(container)
            ] + cmd

        env = self.env

        if os.environ.get("__is_slurm"):
            env['__scratch_dir'] = os.environ['SLURM_TMPDIR']

        has_failed = False

        logger.info("run_python: %s", ' '.join(cmd))

        with open(env['__out_log'], 'a') as out:
            with open(env['__err_log'], 'a') as err:
                with PortablePopen(cmd, stdout=out, stderr=err, env={** os.environ, ** env}) as p:
                    try:
                        p.wait()
                        has_failed = p.popen.returncode != 0
                    except Exception as ex:
                        has_failed = True
                        logger.exception(ex)
                    finally:
                        if has_failed:
                            step_number, control_dir, state_file, state_name = self.read_task_state()
                            self._transition_state_file(state_file, "failed", step_number)
        if has_failed:
            self._exit_process()

    def _terminate_descendants_and_exit(self, p1, p2):

        try:
            try:
                logger.info("signal SIGTERM received, will transition to killed and terminate descendants")
                step_number, control_dir, state_file, state_name = self.read_task_state()
                self._transition_state_file(state_file, "killed", step_number)
                logger.info("will terminate descendants")
            except Exception as _:
                pass

            this_pid = str(os.getpid())
            with PortablePopen(
                ['ps', '-opid', '--no-headers', '--ppid', this_pid]
            ) as p:
                p.wait_and_raise_if_non_zero()
                pids = [
                    int(line.decode("utf-8").strip())
                    for line in p.popen.stdout.readlines()
                ]
                pids = [pid for pid in pids if pid != p.popen.pid]
                logger.debug("descendants of %s: %s", this_pid, pids)
                for pid in pids:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except Exception as _:
                        pass
        except Exception as ex:
            logger.exception(ex)
        finally:
            self._exit_process()


    def _delete_pid_and_slurm_job_id(self, sloc=None):
        try:
            if sloc is None:
                sloc = self.control_dir

            def delete_if_exists(f):
                f = os.path.join(sloc, f)
                if os.path.exists(f):
                    os.remove(f)

            delete_if_exists("pid")
            delete_if_exists("slurm_job_id")
        except Exception as ex:
            logger.exception(ex)


    def _env_from_sourcing(self, env_file):

        p = os.path.abspath(sys.executable)

        dump_with_python_script = f'{p} -c "import os, json; print(json.dumps(dict(os.environ)))"'

        logger.debug("will source file: %s", env_file)

        with PortablePopen(['/bin/bash', '-c', f". {env_file} && {dump_with_python_script}"]) as p:
            p.wait_and_raise_if_non_zero()
            out = p.stdout_as_string()
            return json.loads(out)


    def exec_cmd_before_launch(self, command_before_task):

        p = os.path.abspath(sys.executable)

        dump_with_python_script = f'{p} -c "import os, json; print(json.dumps(dict(os.environ)))"'

        logger.info("will execute 'command_before_task': %s", command_before_task)

        out = self.env['__out_log']
        err = self.env['__err_log']

        with PortablePopen([
            '/bin/bash', '-c', f"{command_before_task} 1>> {out} 2>> {err} && {dump_with_python_script}"
        ]) as p:
            p.wait_and_raise_if_non_zero()
            out = p.stdout_as_string()
            env = json.loads(out)
            for k, v in env.items():
                self.env[k] = v

    def read_task_state(self, control_dir=None, state_file=None):

        if control_dir is None:
            control_dir = self.env["__control_dir"]

        if state_file is None:
            glob_exp = os.path.join(control_dir, "state.*")
            state_file = list(glob.glob(glob_exp))

            if len(state_file) == 0:
                raise Exception(f"no state file in {control_dir}, {glob_exp}")
            if len(state_file) > 1:
                raise Exception(f"more than one state file found in {control_dir}, {glob_exp}")

            state_file = state_file[0]

        base_file_name = os.path.basename(state_file)
        name_parts = base_file_name.split(".")
        state_name = name_parts[1]

        if len(name_parts) == 3:
            step_number = int(name_parts[-1])
        else:
            step_number = 0

        return step_number, control_dir, state_file, state_name

    def _append_to_history(self, control_dir, state_name, step_number=None):
        with open(os.path.join(control_dir, "history.tsv"), "a") as f:
            f.write(state_name)
            f.write("\t")
            f.write(datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f'))
            f.write("\t")
            if step_number is None:
                f.write("")
            else:
                f.write(str(step_number))
            f.write("\n")


    def _transition_state_file(self, state_file, next_state_name, step_number=None, this_logger=logger):

        this_logger.debug("_transition_state_file: %s", state_file)

        control_dir = os.path.dirname(state_file)

        if step_number is None:
            next_step_number = None
            next_state_basename = f"state.{next_state_name}"
        else:
            next_step_number = step_number
            next_state_basename = f"state.{next_state_name}.{next_step_number}"

        next_state_file = os.path.join(control_dir, next_state_basename)

        this_logger.info("will transition to: %s", next_state_basename)
        this_logger.debug("next_state_file: %s", next_state_file)

        os.rename(
            state_file,
            next_state_file
        )

        self._append_to_history(control_dir, next_state_name, step_number)

        return next_state_file, next_step_number


    def transition_to_step_started(self, state_file, step_number):
        return self._transition_state_file(state_file, "step-started", step_number)


    def transition_to_step_completed(self, state_file, step_number):
        state_file, step_number = self._transition_state_file(state_file, "step-completed", step_number)
        return state_file, step_number + 1


    def register_signal_handlers(self):

        def timeout_handler(s, frame):
            step_number, control_dir, state_file, state_name = self.read_task_state()
            self._transition_state_file(state_file, "timed-out", step_number)
            self._exit_process()

        logger.debug("will register signal handlers")

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)

        signal.signal(signal.SIGUSR1, timeout_handler)

        def f(p1, p2):
            self._terminate_descendants_and_exit(p1, p2)
        signal.signal(signal.SIGTERM, f)

        logger.debug("signal handlers registered")


    def transition_to_completed(self, state_file):
        return self._transition_state_file(state_file, "completed")


    def write_out_vars(self, out_vars):

        all_vars = [
            f"{k}={v}" for k, v in out_vars.items()
        ]

        output_vars = os.environ.get("__output_var_file")
        if output_vars is None:
            output_vars = self.env["__output_var_file"]

        with open(output_vars, "w") as f:
            f.write("\n".join(all_vars))

        logger.info("out vars written: %s", ",".join(all_vars))

    def resolve_container_path(self, container):

        containers_dir = self.env.get('__containers_dir')

        if containers_dir is None:
            containers_dir = os.path.join(self.env["__pipeline_code_dir"], "containers")

        if os.path.isabs(container):
            if os.path.exists(container):
                resolved_path = container
            else:
                resolved_path = os.path.join(containers_dir, os.path.basename(container))
        else:
            resolved_path = os.path.join(containers_dir, container)

        if not os.path.exists(resolved_path):
            raise Exception(f"container file not found: {resolved_path}, __containers_dir={containers_dir}")

        logger.debug("container: %s resolves to: %s", container, resolved_path)

        return resolved_path

    def run_script(self, script, container=None):

        env = self.env

        script = os.path.join(
            self.env["__control_dir"],
            os.path.basename(script)
        )

        dump_env = f'python3 -c "import os, json; print(json.dumps(dict(os.environ)))"'
        out = env['__out_log']
        err = env['__err_log']

        cmd = ["bash", "-c", f". {script} 1>> {out} 2>> {err} ; {dump_env}"]

        if container is not None:
            cmd = [
                APPTAINER_COMMAND,
                "exec",
                self.resolve_container_path(container),
            ] + cmd

            apptainer_bindings = []

            root_dir_of_script = _root_dir(script)

            if _fs_type(root_dir_of_script) in ["autofs", "nfs", "zfs"]:
                apptainer_bindings.append(f"{root_dir_of_script}:{root_dir_of_script}")

            if os.environ.get("__is_slurm"):
                scratch_dir = os.environ['SLURM_TMPDIR']
                env['__scratch_dir'] = scratch_dir
                root_of_scratch_dir = _root_dir(scratch_dir)
                apptainer_bindings.append(f"{root_of_scratch_dir}:{root_of_scratch_dir}")

            if len(apptainer_bindings) > 0:

                prev_apptainer_bindings = env.get("APPTAINER_BIND")

                if prev_apptainer_bindings is not None and prev_apptainer_bindings != "":
                    bindings_prefix = f"{prev_apptainer_bindings},"
                else:
                    bindings_prefix = ""

                env["APPTAINER_BIND"] = f"{bindings_prefix}{','.join(apptainer_bindings)}"

            new_bind = env.get("APPTAINER_BIND")
            if new_bind is not None:
                logger.info("APPTAINER_BIND not set")
            else:
                logger.info("APPTAINER_BIND=%s", new_bind)


        logger.info("run_script: %s", " ".join(cmd))

        has_failed = False
        try:

            with PortablePopen(cmd, env={** os.environ, ** env}) as p:
                p.wait_and_raise_if_non_zero()
                out = p.stdout_as_string()
                step_output_vars = json.loads(out)
                task_output_vars = dict(self.iterate_out_vars_from())

                with open(os.path.join(env["__control_dir"], "task-conf.json")) as _task_conf:
                    task_conf_as_json = json.loads(_task_conf.read())
                    for o in task_conf_as_json["outputs"]:
                        o = TaskOutput.from_json(o)
                        if o.type != "file":
                            v = step_output_vars.get(o.name)
                            logger.debug("script exported output var %s = %s", o.name, v)
                            task_output_vars[o.name] = v
                            if v is not None:
                                self.env[o.name] = v

                self.write_out_vars(task_output_vars)


        except Exception as ex:
            logger.exception(ex)
            has_failed = True
        finally:
            if has_failed:
                step_number, control_dir, state_file, state_name = self.read_task_state()
                self._transition_state_file(state_file, "failed", step_number)
                self._exit_process()

    def _run_steps(self):

        step_number, control_dir, state_file, state_name = self.read_task_state()
        step_invocations = self.task_conf["step_invocations"]

        for i in range(step_number, len(step_invocations)):

            step_invocation = step_invocations[i]
            state_file, step_number = self.transition_to_step_started(state_file, step_number)

            call = step_invocation["call"]

            if call == "python":
                self.run_python(step_invocation["module_function"], step_invocation.get("container"))
            elif call == "bash":
                self.run_script(os.path.expandvars(step_invocation["script"]),
                           step_invocation.get("container"))
            else:
                raise Exception(f"unknown step invocation type: {call}")

            state_file, step_number = self.transition_to_step_completed(state_file, step_number)

        self.transition_to_completed(state_file)

    def launch_task(self, wait_for_completion, exit_process_when_done=True):

        def task_func_wrapper():
            try:
                self.resolve_task_env()
                logger.debug("task func started")
                self._run_steps()
                logger.info("task completed")
            except Exception as ex:
                if not exit_process_when_done:
                    raise ex
                logger.exception(ex)
            finally:
                if exit_process_when_done:
                    self._exit_process()

        if wait_for_completion:
            task_func_wrapper()
        else:
            slurm_job_id = os.environ.get("SLURM_JOB_ID")
            is_slurm = slurm_job_id is not None
            if (not is_slurm) and os.fork() != 0:
                # launching process, die to let the child run in the background
                exit(0)
            else:
                # forked child, or slurm job
                sloc = os.environ['__script_location']
                if is_slurm:
                    sloc = _script_location_of_array_task_id_if_applies(sloc)
                    logger.info("slurm job started, slurm_job_id=%s", slurm_job_id)
                    with open(os.path.join(sloc, "slurm_job_id"), "w") as f:
                        f.write(str(os.getpid()))
                else:
                    with open(os.path.join(sloc, "pid"), "w") as f:
                        f.write(str(os.getpid()))

                os.setpgrp()
                self.register_signal_handlers()
                Thread(target=task_func_wrapper).start()
                signal.pause()

    def archive_produced_files(self, task_output_dir, exclusion_glob_patterns):

        archive_tar_name = "drypipe-archive.tar.gz"
        archive_tar = os.path.join(task_output_dir, archive_tar_name)

        if os.path.exists(archive_tar):
            return False

        def matches_one_pattern(f_name):
            for p in exclusion_glob_patterns:
                if fnmatch.fnmatch(f_name, p):
                    return True

        def gen_to_archive():
            with os.scandir(task_output_dir) as files_in_output_dir:
                for f in files_in_output_dir:
                    if not (f.name == archive_tar_name or matches_one_pattern(f.name)):
                        yield f.name

        files_to_archive = list(gen_to_archive())

        to_delete = []

        with tarfile.open(archive_tar, "w:gz") as tar:
            for f in files_to_archive:
                f0 = os.path.join(task_output_dir, f)
                tar.add(f0, arcname=f)
                to_delete.append(f0)

        for f in to_delete:
            if os.path.isdir(f):
                shutil.rmtree(f)
            else:
                os.remove(f)

        return True


def handle_main():
    is_kill_command = "kill" in sys.argv
    is_tail_command = "tail" in sys.argv
    wait_for_completion = "--wait" in sys.argv
    is_ps_command = "ps" in sys.argv
    is_env = "env" in sys.argv
    is_with_exports = "--with-exports" in sys.argv

    is_run_command = not (is_tail_command or is_ps_command or is_kill_command or is_env)

    task_runner = TaskProcess(os.environ["__script_location"])

    if is_run_command:
        task_runner.launch_task(wait_for_completion)
    else:

        def quit_command(p1, p2):
            logging.shutdown()
            exit(0)

        signal.signal(signal.SIGINT, quit_command)

        if is_tail_command:
            _tail_command()
        elif is_ps_command:
            _ps_command()
        elif is_kill_command:
            _kill_command()
        elif is_env:
            prefix = "export " if is_with_exports else ""
            for k, v in task_runner.iterate_task_env():
                print(f"{prefix}{k}={v}")
        else:
            raise Exception(f"bad args: {sys.argv}")


def _load_pid_or_job_id(control_dir, pid_or_job_id):
    pid_file = os.path.join(control_dir, pid_or_job_id)
    if not os.path.exists(pid_file):
        return None
    else:
        with open(pid_file) as f:
            return int(f.read())


def load_pid(control_dir):
    return _load_pid_or_job_id(control_dir, "pid")


def load_slurm_job_id(control_dir):
    return _load_pid_or_job_id(control_dir, "slurm_job_id")


def print_pid_not_found(sloc):
    print(f"file {os.path.join(sloc, 'pid')} not found, task not running")


def _kill_command():
    sloc = os.environ["__script_location"]
    pid = load_pid(sloc)
    if pid is None:
        print_pid_not_found(sloc)
    else:
        os.kill(pid, signal.SIGTERM)


def _ps_command(pid=None):

    if pid is None:
        sloc = os.environ["__script_location"]
        pid = load_pid(sloc)
        if pid is None:
            print_pid_not_found(sloc)
            return

    res = list(ps_resources(pid))

    if len(res) == 0:
        return

    header, row = res

    sys.stdout.write("\t".join(header))
    sys.stdout.write("\n")
    sys.stdout.write("\t".join(str(v) for v in row))
    sys.stdout.write("\n")


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def ps_resources(pid):

    ps_format = "size,%mem,%cpu,rss,rsz,etimes"

    def parse_ps_line(r):
        size, pmem, pcpu, rss, rsz, etimes = r.split()
        return int(size), float(pmem), float(pcpu), int(rss), int(rsz), int(etimes)

    with PortablePopen(["ps", f"--pid={pid}", f"--ppid={pid}", "o", ps_format, "--no-header"]) as p:
        p.wait()
        if p.popen.returncode != 0:
            return iter([])

        def f(t1, t2):
            size1, pmem1, pcpu1, rss1, rsz1, etimes1 = t1
            size2, pmem2, pcpu2, rss2, rsz2, etimes2 = t2
            return (
               size1 + size2,
               pmem1 + pmem2,
               pcpu1 + pcpu2,
               rss1 + rss2,
               rsz1 + rsz2,
               max(etimes1, etimes2)
            )

        yield ps_format.split(",")

        out = p.stdout_as_string().strip()
        if out != "":
            yield list(reduce(
                f,
                [parse_ps_line(line) for line in out.split("\n")],
                (0, 0, 0, 0, 0, 0)
            ))


def _tail_command():

    script_location = os.environ["__script_location"]

    all_logs = [
        os.path.join(script_location, log)
        for log in ["drypipe.log", "out.log", "err.log"]
    ]

    tail_cmd = ["tail", "-f"] + all_logs

    with PortablePopen(tail_cmd, stdout=sys.stdout, stderr=sys.stderr, stdin=sys.stdin) as p:
        p.wait()

def _script_location_of_array_task_id_if_applies(sloc):
    slurm_array_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if slurm_array_task_id is None:
        return slurm_array_task_id
    else:
        slurm_array_task_id = int(slurm_array_task_id)
        with open(os.path.join(sloc, "array_children_task_ids.tsv")) as _array_children_task_ids:
            c = 0
            for line in _array_children_task_ids:
                if c == slurm_array_task_id:
                    p = os.path.join(os.path.dirname(sloc), line.strip())
                    os.environ['__script_location'] = p
                    return p
                else:
                    c += 1

        msg = f"Error: no task_key for SLURM_ARRAY_TASK_ID={slurm_array_task_id}"
        raise Exception(msg)



def _root_dir(d):
    p = Path(d)
    return os.path.join(p.parts[0], p.parts[1])


def _fs_type(file):

    stat_cmd = f"stat -f -L -c %T {file}"
    with PortablePopen(stat_cmd.split()) as p:
        p.wait_and_raise_if_non_zero()
        return p.stdout_as_string().strip()


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

class RetryableRsyncException(Exception):
    def __init__(self, message):
        super().__init__(message)

def invoke_rsync(command):
    with PortablePopen(command, shell=True) as p:
        p.popen.wait()
        if p.popen.returncode != 0:
            is_retryable, rsync_message = _rsync_error_codes(p.popen.returncode)
            msg = f"rsync failed: {rsync_message}, \n'{command}' \n{p.safe_stderr_as_string()}"
            if not is_retryable:
                raise Exception(msg)
            else:
                raise RetryableRsyncException(msg)


def terminate_all():
    scancel_all()
    segterm_all()


def scancel_all():
    pass


def segterm_all():
    pass


def handle_script_lib_main():

    if "launch-task-from-remote" in sys.argv:
        task_key = sys.argv[2]
        is_slurm = "--is-slurm" in sys.argv
        wait_for_completion = "--wait-for-completion" in sys.argv
        drypipe_task_debug = "--drypipe-task-debug" in sys.argv
        launch_task_from_remote(task_key, is_slurm, wait_for_completion, drypipe_task_debug)
    elif "terminate-all" in sys.argv:
        terminate_all()
    elif "scancel-all" in sys.argv:
        terminate_all()
    elif "segterm-all" in sys.argv:
        segterm_all()
    elif "detect-slurm-crashes" in sys.argv:
        user = sys.argv[2]
        detect_slurm_crashes(user, "--is-debug" in sys.argv)
        #detect_process_crashes()
    elif "detect-crashes" in sys.argv:
        user = sys.argv[2]
        detect_slurm_crashes(user, "--is-debug" in sys.argv)
        #detect_process_crashes()
    else:
        raise Exception('invalid args')

    logging.shutdown()


class FileCreationDefaultModes:
    pipeline_instance_directories = 0o774
    pipeline_instance_scripts = 0o774


class TaskInput:

    @staticmethod
    def from_json(json_string):
        j = json_string
        return TaskInput(
            j["name"],
            j["type"],
            upstream_task_key=j.get("upstream_task_key"),
            name_in_upstream_task=j.get("name_in_upstream_task"),
            file_name=j.get("file_name"),
            value=j.get("value")
        )

    def _dict(self):
        return {
            "upstream_task_key": self.upstream_task_key,
            "name_in_upstream_task": self.name_in_upstream_task,
            "file_name": self.file_name,
            "value": self.value
        }

    def as_string(self):

        def f(p):
            return f"TaskInput({self.name},{self.type},{p})"

        return f(
            ",".join([
                f"{k}={v}" for k, v in self._dict().items()
            ])
        )


    def __init__(self, name, type, upstream_task_key=None, name_in_upstream_task=None, file_name=None, value=None):

        if type not in ['file', 'str', 'int', 'float']:
            raise Exception(f"invalid type {type}")

        if type != 'file' and file_name is not None:
            raise Exception(f"inputs of type '{type}' can't have file_name: {file_name}")

        if value is not None:
            if name_in_upstream_task is not None or upstream_task_key is not None:
                raise Exception(f"invalid constant {name}")

        self.name = name
        self.type  = type
        self.name_in_upstream_task = name_in_upstream_task
        self.upstream_task_key = upstream_task_key
        self.value = value

        self.file_name = file_name

    def parse(self, v):
        if self.type == "int":
            return int(v)
        elif self.type == "float":
            return float(v)
        elif self.type == "glob_expression":
            class GlobExpression:
                def __call__(self, *args, **kwargs):
                    return glob.glob(os.path.expandvars(v))

                def __str__(self):
                    return os.path.expandvars(v)
            return GlobExpression()
        else:
            return v

    def is_file(self):
        return self.file_name is not None

    def is_upstream_output(self):
        return self.upstream_task_key is not None

    def is_constant(self):
        return self.value is not None

    def _ref(self):
        return f"{self.upstream_task_key}/{self.name_in_upstream_task}"

    def as_json(self):
        return {
            "name": self.name,
            "type":self.type,
            ** self._dict()
        }


class TaskOutput:

    @staticmethod
    def from_json(json_dict):
        j = json_dict
        return TaskOutput(j["name"], j["type"], j.get("produced_file_name"))


    def as_string(self):
        p = "" if self.produced_file_name is None else f",{self.produced_file_name}"
        return f"TaskOutput({self.name},{self.type}{p})"

    def __init__(self, name, type, produced_file_name=None, task_key=None):
        if type not in ['file', 'str', 'int', 'float']:
            raise Exception(f"invalid type {type}")

        if type != 'file' and produced_file_name is not None:
            raise Exception(f"non file output can't have not None produced_file_name: {produced_file_name}")

        self.name = name
        self.type = type
        self.produced_file_name = produced_file_name
        self._resolved_value = None
        self.task_key = task_key

    def set_resolved_value(self, v):
        self._resolved_value = self.parse(v)

    def is_file(self):
        return self.produced_file_name is not None

    def as_json(self):
        r = {
            "name": self.name,
            "type": self.type
        }

        if self.produced_file_name is not None:
            r["produced_file_name"] = self.produced_file_name

        return r

    def parse(self, v):
        if self.type == "int":
            return int(v)
        elif self.type == "float":
            return float(v)

        return v

    def __int__(self):

        if self.type == "float":
            return int(self._resolved_value)

        if self.type != "int":
            raise Exception(f"Task output {self.name} is not of type int, actual type is: {self.type} ")

        return self._resolved_value

    def __float__(self):

        if self.type == "int":
            return float(self._resolved_value)

        if self.type != "float":
            raise Exception(f"Task output {self.name} is not of type float, actual type is: {self.type} ")

        return self._resolved_value

    def __str__(self):
        return str(self._resolved_value)

    def __fspath__(self):

        if self.type != "file":
            raise Exception(f"Task output {self.name} is not of type file, actual type is: {self.type} ")

        return self._resolved_value

def launch_task_from_remote(task_key, is_slurm, wait_for_completion, drypipe_task_debug):

    pipeline_instance_dir = os.path.dirname(os.path.dirname(sys.argv[0]))

    control_dir = os.path.join(pipeline_instance_dir, '.drypipe', task_key)

    if drypipe_task_debug:
        os.environ["DRYPIPE_TASK_DEBUG"] = "True"

    logger = create_task_logger(control_dir)

    out_sigs_dir = os.path.join(control_dir, 'out_sigs')
    touch(os.path.join(control_dir, 'output_vars'))

    work_dir = os.path.join(pipeline_instance_dir, 'output', task_key)
    scratch_dir = os.path.join(work_dir, "scratch")

    for d in [work_dir, out_sigs_dir, scratch_dir]:
        Path(d).mkdir(exist_ok=True, parents=True, mode=FileCreationDefaultModes.pipeline_instance_directories)

    for state_file in glob.glob(os.path.join(control_dir, "state.*")):
        os.remove(state_file)

    touch(os.path.join(control_dir, 'state.launched.0'))

    _delete_pid_and_slurm_job_id(control_dir)

    env = os.environ

    if is_slurm:
        cmd = os.path.join(control_dir, 'sbatch-launcher.sh')
        if wait_for_completion:
            env = {**env, "SBATCH_EXTRA_ARGS": "--wait", "DRYPIPE_TASK_DEBUG": str(drypipe_task_debug)}
            cmd = ["bash", "-c", cmd]
    else:
        if wait_for_completion:
            back_ground = ""
        else:
            back_ground = "&"
        scr = os.path.join(control_dir, 'task')
        cmd = ["nohup", "bash", "-c", f"python3 {scr} {back_ground}"]

    logger.info("launching from remote: %s", ' '.join(cmd) if isinstance(cmd, list) else cmd)

    with open(os.path.join(control_dir, 'out.log'), 'w') as out:
        with open(os.path.join(control_dir, 'err.log'), 'w') as err:
            with PortablePopen(
                cmd,
                stdout=out,
                stderr=err,
                env=env
            ) as p:
                p.wait()
                if p.popen.returncode != 0:
                    logger.error(f"remote task launch failed: %s", sys.argv)
                print(str(p.popen.returncode))
