import argparse
import shutil
import time
import json
import logging
import logging.config
import os
import sys
import textwrap
from os import environ
from pathlib import Path

from dry_pipe import PortablePopen
from dry_pipe.core_lib import func_from_mod_func, is_inside_slurm_job
from dry_pipe.pipeline_instance import Monitor
from dry_pipe.task_process import TaskProcess
from dry_pipe.slurm_array_task import SlurmArrayParentTask
from dry_pipe.reports import timers_for_tasks
from dry_pipe.state_machine import StateFileTracker
from dry_pipe.service import PipelineRunner
from dry_pipe.task_lib import submit_local_array

logger = logging.getLogger(__name__)

def call(mod_func):

    python_task = func_from_mod_func(mod_func)
    control_dir = os.environ["__control_dir"]
    task_process = TaskProcess(control_dir, is_python_call=True)
    task_process.call_python(mod_func, python_task)


def init_logging(logging_conf, verbose=False):

    if logging_conf is not None:

        if not Path(logging_conf).exists():
            raise Exception(f"logging config file '{logging_conf}' refered by env var LOGGING_CONF does not exist")

        with open(logging_conf, "r") as f:
            log_conf_json = json.load(f)

            handlers = log_conf_json["handlers"]

            for k, handler in handlers.items():
                handler_class = handler["class"]
                if handler_class == "logging.FileHandler":
                    filename = handler.get("filename")
                    if filename is None or filename == "":
                        raise Exception(f"logging.FileHandler '{k}' has no filename attribute in {logging_conf}")
                    if "$" in filename:
                        filename = os.path.expandvars(filename)
                        handler["filename"] = os.path.expandvars(filename)

        logger = logging.getLogger(__name__)
        logger.info("using logging config file '%s'", logging_conf)

    else:

        default_level = "DEBUG" if verbose else "INFO"

        log_conf_json = {
          "version": 1,
          "formatters": {
            "simple": {
              "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
          },
          "handlers": {
            "console": {
                "class": "logging.StreamHandler",

                "formatter": "simple",
                "stream": "ext://sys.stdout"
            }
          },
          "root": {
            "level": default_level,
            "handlers": ["console"]
          },
          "dry_pipe": {
              "level": default_level,
              "handlers": ["console"],
              "propagate": 0
          }
        }

    logging.config.dictConfig(log_conf_json)


def setup_cli_logging(logging_level):

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging_level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%H:%M:%S%z'
        )
    )

    def add_handler(name):
        logger = logging.getLogger(name)
        logger.setLevel(logging_level)
        logger.addHandler(handler)

    add_handler("dry_pipe.pipeline_runner")
    add_handler("dry_pipe.pipeline_instance")
    add_handler("dry_pipe.task_process")
    add_handler("dry_pipe.slurm_array_task")
    add_handler(__name__)

    logger.info(f"Logging level: {logging.getLevelName(logging_level)}")


def setup_verbose1():
    setup_cli_logging(logging.INFO)

def setup_verbose2():
    setup_cli_logging(logging.DEBUG)


class EnvDefault(argparse.Action):
    def __init__(self, envvar, env, required=True, default=None, **kwargs):
        if envvar:
            if envvar in env:
                default = env[envvar]
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)

class CliMonitor(Monitor):

    def __init__(self, pipeline_instance, mod_func):
        super().__init__()
        self.pipeline_instance = pipeline_instance
        self.mod_func = mod_func

    def dump(self, state_file_tracker):

        os.system('clear')
        print(f"PipelineInstance({self.mod_func}, {state_file_tracker.pipeline_instance_dir})")
        for task_group, state_counts in self.produce_report(state_file_tracker.all_state_files()):
            dump_counts = ",".join([
                f"{s}: {c}" for s, c in state_counts
            ])
            print(f" - {task_group}: ({dump_counts})")


def _cleanup_args(args):
    """
    Pycharm in debug mode prepends it's debugger, this function removes it
    """
    corrupt = False
    idx = 0
    for a in args:
        if "pydevd.py" in a:
            corrupt = True
            idx = args.index("--file")
            break

    if corrupt:
        return args[idx + 2:]
    else:
        return args


class Cli:

    def __init__(self, args, env=None, test_mode=False):

        self.raw_command_line = " ".join(args)
        self.args = _cleanup_args(args)

        self._has_implicit_generator = False
        self._has_implicit_control_dir = False
        self._has_implicit_pid = False
        self._has_implicit_task_key = False

        self.test_mode = test_mode

        if env is None:
            self.env = os.environ
        else:
            self.env = env

        self.is_dp_func = environ.get("__IS_DRYPIPE_DP_FUNC") == "True"

        self.parser = argparse.ArgumentParser(
            description="DryPipe CLI"
        )

        self.parser.add_argument(
            '--v', '-v',
            action='store_true', default=False, help="verbose (logging_level.INFO)"
        )

        self.parser.add_argument(
            '--vv', '-vv',
            action='store_true', default=False, help="very verbose (logging_level.DEBUG)"
        )

        self.parser.add_argument(
            '--dry-run',
            action='store_true',
            default=False,
            help="don't actualy run, but print what will run (implicit --verbose)",
        )

        self.command_names_to_method = {}

        for command in self._enumerate_commands():
            method_name = command.name.replace("-", "_")
            m = getattr(self, method_name)
            if m is None:
                raise Exception(f"method {method_name} should exist, for command {command.name}")
            else:
                self.command_names_to_method[command.name] = m


    def invoke(self):

        if is_inside_slurm_job():

            tcd = Path(os.environ["DRYPIPE_TASK_CONTROL_DIR"])
            task_key = tcd.name.__str__()
            pid = tcd.parent.parent.__str__()

            self.parsed_args = self.parser.parse_args([
                "task",
                f"--pipeline-instance-dir={pid}",
                f"--task-key={task_key}"
            ])
        else:
            self.parsed_args = self.parser.parse_args(self.args)


        if self.parsed_args.v:
            setup_verbose1()
        elif self.parsed_args.vv:
            setup_verbose2()

        method = self.command_names_to_method.get(self.parsed_args.command)

        if method is None:
            raise Exception(f"method {method} should exist, for command {self.parsed_args.command}")

        method()


    def _enumerate_commands(self):

        class ListOfInts:
            def __call__(self, txt):
                return [int(s) for s in txt.split(",")]



        def pipeline_instance_dir(parser):
            parser.add_argument(
                '--pipeline-instance-dir',
                help='pipeline instance directory, can also be set with environment var DRYPIPE_PIPELINE_INSTANCE_DIR',
                action=EnvDefault,
                envvar="DRYPIPE_PIPELINE_INSTANCE_DIR",
                env=self.env
            )

        def task_key(parser):
            pipeline_instance_dir(parser)
            parser.add_argument('--task-key')

        def limit(parser):
            parser.add_argument(
                '--limit', type=int, help='limit submitted array size to N tasks', metavar='N'
            )

        def by_runner(parser):
            parser.add_argument("--by-runner", dest="by_runner", action="store_true")
            parser.set_defaults(by_runner=False)

        def until(parser):
            parser.add_argument(
                '--until', help='tasks matching PATTERN will not be started',
                action='append',
                metavar='PATTERN'
            )

        def ssh_remote_dest(parser):
            parser.add_argument(
                '--ssh-remote-dest',
                help=textwrap.dedent(
                """
                    example:`me@myhost.example.com:/my-directory`            
                """)
            )

        def tail(parser):
            parser.add_argument("--tail", dest="tail", action="store_true")

        def from_remote(parser):
            parser.add_argument("--from-remote", dest="from_remote", action="store_true")

        def reset(parser):
            parser.add_argument(
                '--reset',
                help='restart the task from the first step, clears the results directory if exists',
                action='store_true'
            )

        def generator(parser):
            parser.add_argument(
                '--generator',
                help='<module>:<function> task generator function, can also be set with environment var DRYPIPE_PIPELINE_GENERATOR',
                action=EnvDefault,
                envvar="DRYPIPE_PIPELINE_GENERATOR",
                metavar="GENERATOR",
                env=self.env
            )

        def at_step(parser):
            parser.add_argument(
                '--at-step',
                type=int,
                help='restarts the task at the specified step (zero based).',
                default=None
            )

        def wait(parser):
            parser.add_argument(
                '--wait',
                dest='wait',
                action='store_true',
                help="wait for task to complete before exiting"
            )
            parser.set_defaults(wait=False)

        def gen_rsync_list(parser):
            parser.add_argument(
                '--gen-rsync-list',
                help='generate rsync list for file sets',
                action='store_true',
                default=False
            )

        def include_pre_launch(parser):
            parser.add_argument(
                '--include-pre-launch',
                help='Also restart tasks that have failed to launch (useful after scancel on an array)',
                action='store_true',
                default=False
            )

        def restart_failed(parser):
            parser.add_argument(
                '--restart-failed',
                help='failed tasks will be restarted',
                action='store_true',
                default=False
            )

        def reset_failed(parser):
            parser.add_argument(
                '--reset-failed',
                help='failed tasks will be reset and then restarted',
                action='store_true',
                default=False
            )

        def sleep_schedule(parser):
            parser.add_argument(
                "--sleep-schedule",
                action=EnvDefault,
                envvar="DRYPIPE_SERVICE_SLEEP_SCHEDULE",
                env=self.env,
                help="a list of sleep times in seconds, for the main loop of the service, can also be set with environment var DRYPIPE_SERVICE_SLEEP_SCHEDULE",
                default="0,1,3,5,10,15,20",
                type=ListOfInts()
            )

        def config_generator(parser):
            parser.add_argument(
                "--config-generator",
                action=EnvDefault,
                envvar="DRYPIPE_SERVICE_CONFIG_GENERATOR",
                env=self.env,
                help="""a function that yields instances of dry_pipe.pipeline.PipelineType, 
                        can also be set with environment var DRYPIPE_SERVICE_CONFIG_GENERATOR""",
            )

        _s = self.parser.add_subparsers(required=True, dest='command')
        self.subparsers = _s

        class Command:
            def __init__(self, name, *args):
                self.name = name
                sub_parser = _s.add_parser(name)
                for a in args:
                    try:
                        a(sub_parser)
                    except Exception as e:
                        raise Exception(f"arg {a.__name__}  on command {name} failed with exception {e}")


        yield Command('run', pipeline_instance_dir, generator, until, restart_failed, reset_failed, sleep_schedule)
        yield Command('prepare', pipeline_instance_dir, generator, until, sleep_schedule)
        yield Command('service', pipeline_instance_dir, config_generator, sleep_schedule)
        yield Command('upgrade-drypipe', pipeline_instance_dir)
        yield Command('restart-failed-array-tasks', pipeline_instance_dir, include_pre_launch)

        yield Command('task', task_key, wait, tail, by_runner, from_remote, ssh_remote_dest)
        yield Command('restart', task_key, at_step, reset, wait, from_remote)
        yield Command('poll-task', task_key)
        yield Command('remote-exec', task_key, wait)
        yield Command("submit-array-from-remote", task_key, wait)
        yield Command("watch-array-from-remote", task_key, wait)
        yield Command('fetch-remote-state', task_key, wait)
        yield Command('upload-drypipe-for-remote-instance', task_key)
        yield Command('sbatch', task_key, wait)
        yield Command('sbatch-gen', task_key)
        yield Command('array-submit', task_key, limit)
        yield Command('array-upload', task_key)
        yield Command('array-download', task_key)
        yield Command('create-array-parent', task_key)
        yield Command('list-states', task_key, gen_rsync_list)
        yield Command('array-rsync-list', task_key)


        def module_function(parser):
            parser.add_argument('module_function', type=str)

        yield Command('call', module_function, task_key)


    def pipeline_instance_from_args(self):

        g = self.parsed_args.generator
        if g is None:
            raise Exception(f"--generator is required")
        pipeline = func_from_mod_func(g)()

        if self.parsed_args.pipeline_instance_dir is None:
            raise Exception(
                f"--pipeline-instance-dir is required, " +
                "or DRYPIPE_PIPELINE_INSTANCE_DIR environment variable must be set"
            )

        return pipeline.create_pipeline_instance(self.parsed_args.pipeline_instance_dir)

    def run(self):
        pipeline_instance = self.pipeline_instance_from_args()
        pipeline_instance.prepare_instance_dir()
        if not self.test_mode:
            pipeline_instance.monitor = CliMonitor(pipeline_instance, self.parsed_args.generator)

        pipeline_instance.run(
            until_patterns=self.parsed_args.until,
            restart_failed=self.parsed_args.restart_failed,
            reset_failed=self.parsed_args.reset_failed,
            sleep_schedule=self.parsed_args.sleep_schedule
        )

    def call(self):
        call(self.parsed_args.module_function)


    def prepare(self):
        pipeline_instance = self.pipeline_instance_from_args()
        pipeline_instance.prepare_instance_dir()
        pipeline_instance.run_sync(["*"], sleep_schedule=self.parsed_args.sleep_schedule)

    def service(self):
        init_logging(self.parsed_args.log_conf, verbose=self.parsed_args.v)

        cg = self.parsed_args.config_generator

        logging.info("will load config %s", cg)

        logging.debug("sleep schedule: %s", self.parsed_args.sleep_schedule)

        g = list(func_from_mod_func(cg)())

        pipeline_runner = PipelineRunner(
            g,
            run_sync=False,
            run_tasks_in_process=False,
            sleep_schedule=self.parsed_args.sleep_schedule
        )

        logging.info("starting drypipe service")

        for suggested_sleep in pipeline_runner.iterate_work():
            if suggested_sleep > 0:
                logging.debug("will sleep for %s", suggested_sleep)
                time.sleep(suggested_sleep)


    def upgrade_drypipe(self):
        StateFileTracker.copy_drypipe_code(Path(self.parsed_args.pipeline_instance_dir).joinpath(".drypipe"))

    def restart_failed_array_tasks(self):
        task_process = TaskProcess(
            os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
            alternate_logger=logger
        )

        if self._wait():
            task_process.wait_for_completion = True

        array_parent_task = SlurmArrayParentTask(task_process)

        array_parent_task.prepare_and_launch_next_array(
            restart_failed=True,
            include_pre_launch=self.parsed_args.include_pre_launch
        )

    def _tail(self):
        return self.parsed_args.tail

    def _wait(self):
        return self.parsed_args.wait

    def task(self):

        #raise Exception(f">>> {self._control_dir()}")
        task_process = TaskProcess(
            self._control_dir(),
            wait_for_completion=self._wait(),
            test_mode=self.test_mode,
            as_subprocess=not self.test_mode,
            tail=self._tail(),
            from_remote=self.parsed_args.from_remote
        )

        if self.parsed_args.ssh_remote_dest is not None:
            task_process.task_conf.ssh_remote_dest = self.parsed_args.ssh_remote_dest
        elif task_process.task_conf.executer_type == "slurm":
            if self.parsed_args.by_runner and not task_process.task_conf.is_slurm_parent:
                task_process.submit_sbatch_task()
                return

        task_process.launch_task()

    def poll_task(self):
        control_dir = self._control_dir()
        task_process = TaskProcess(control_dir, no_logger=True)

        s = list(Path(control_dir).glob("state.*"))

        if len(s) == 0:
            raise Exception(f"no state file in {control_dir}")
        elif len(s) > 1:
            raise Exception(f"multiple state files in {control_dir}")

        state_file = s[0]
        print(f"{state_file.absolute()}")

    def remote_exec(self):
        control_dir = self._control_dir()
        task_process = TaskProcess(
            control_dir,
            wait_for_completion=False,
            test_mode=self.test_mode,
            as_subprocess=not self.test_mode,
        )

        s = list(Path(control_dir).glob("state.*"))

        if len(s) == 0:
            Path(control_dir, "state.waiting").touch(exist_ok=False)
        elif len(s) > 1:
            raise Exception(f"multiple state files in {control_dir}")

        if task_process.task_conf.executer_type == "slurm":
            task_process.submit_sbatch_task()
        else:
            task_process.launch_task()


    def submit_array_from_remote(self):
        control_dir = self._control_dir()
        task_process = TaskProcess(control_dir, use_remote_drypipe_log=True)

        task_process.task_logger.info("raw command line: %s", self.raw_command_line)
        res = submit_local_array.func(task_process)
        # task_process.task_logger.info("submitted array from remote %s", json.dumps(res))
        print(json.dumps(res))

    def watch_array_from_remote(self):

        control_dir = self._control_dir()

        use_remote_drypipe_log = True
        alternate_logger = None
        if self.parsed_args.vv or self.parsed_args.v:
            use_remote_drypipe_log = False
            alternate_logger = logger

        task_process = TaskProcess(
            control_dir, use_remote_drypipe_log=use_remote_drypipe_log,
            for_dry_run=self.parsed_args.dry_run,
            alternate_logger=alternate_logger
        )
        atm = task_process.create_array_task_manager()
        report = atm.manage_auto_restarts_from_remote()
        print(json.dumps(report))

    def fetch_remote_state(self):
        task_process = TaskProcess(
            os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
            wait_for_completion=self._wait(),
            alternate_logger=logger
        )
        task_process.fetch_remote_state()

    def upload_drypipe_for_remote_instance(self):
        task_process = TaskProcess(
            os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
            wait_for_completion=True,
            alternate_logger=logger
        )
        task_process.upload_drypipe_for_remote_instance()

    def sbatch(self):
        task_process = TaskProcess(self.parsed_args.control_dir, wait_for_completion=self._wait())
        task_process.submit_sbatch_task()

    def sbatch_gen(self):
        task_process = TaskProcess(self.parsed_args.control_dir, wait_for_completion=self._wait())
        print(" ".join(task_process.sbatch_cmd_lines()))

    def array_submit(self):
        task_process = TaskProcess(
            self._control_dir(),
            as_subprocess=not self.test_mode,
            test_mode=self.test_mode
        )
        task_process.run(
            array_limit=self.parsed_args.limit
        )


    def array_upload(self):
        task_process = TaskProcess(self._control_dir())

        if self.parsed_args.ssh_remote_dest is not None:
            task_process.task_conf.ssh_remote_dest = self.parsed_args.ssh_remote_dest

        array_parent_task = SlurmArrayParentTask(task_process)

        array_parent_task._upload_array()

    def array_download(self):
        task_process = TaskProcess(
            os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
            alternate_logger=logger
        )

        if self.parsed_args.ssh_remote_dest is not None:
            task_process.task_conf.ssh_remote_dest = self.parsed_args.ssh_remote_dest

        array_parent_task = SlurmArrayParentTask(task_process)

        array_parent_task._download_array()

    def create_array_parent(self):

        new_task_key = self.parsed_args.new_task_key
        matcher = self.parsed_args.matcher

        SlurmArrayParentTask.create_array_parent(
            self.parsed_args.pipeline_instance_dir,
            new_task_key,
            matcher,
            self.parsed_args.slurm_account,
            split_into=self.parsed_args.split,
            extra_env=self.env
        )


    def array_rsync_list(self):
        return

    def list_states(self):


        task_process = TaskProcess(
            self._control_dir(),
            no_logger=True
        )

        if self.parsed_args.gen_rsync_list:
            task_process.generate_rsync_list_for_file_sets()

        def p():
            if task_process.is_slurm_array_parent():
                array_parent_task = SlurmArrayParentTask(task_process)
                for task_key, state in array_parent_task.list_array_states():
                    yield task_key, state

            else:
                state_file_path = StateFileTracker.find_state_file_path_if_exists(task_process.control_dir)
                if state_file_path is not None:
                    yield task_process.task_key, state_file_path.name


        for task_key, state in p():
            print(f"{task_key}/{state}")


    def report_perf(self):

        if self.parsed_args.pipeline_instance_dir is None:
            raise Exception(f"--pipeline-instance-dir must be specified")

        if self.parsed_args.task_key is not None and self.parsed_args.filter == "*":
            f = self.parsed_args.task_key
        else:
            f = self.parsed_args.filter

        for task_key, timer_label, hms, s in timers_for_tasks(self.parsed_args.pipeline_instance_dir, f):
            print(f"{timer_label}\t{task_key}\t{hms}\t{s}")

    def restart(self):

        task_process = TaskProcess(
            self._control_dir(),
            wait_for_completion=self.parsed_args.wait,
            use_remote_drypipe_log=self.parsed_args.from_remote
        )

        task_process.reset_restart_accounting()

        step_number, control_dir, state_file, state_name = task_process.read_task_state()

        if self.parsed_args.reset:
            shutil.rmtree(task_process.task_output_dir)
            task_process.rewind_to_step(0)

        if self.parsed_args.at_step:
            task_process.rewind_to_step(self.parsed_args.at_step)


        if task_process.is_slurm_array_parent():
            if task_process.is_remote_execution_on_master_site() and step_number == 2:
                task_process.task_logger.info("remote array at watch[2] step, will rewind to submit[1]")
                task_process.rewind_to_step(1)
            elif step_number == 1:
                task_process.task_logger.info("local array at watch[1] step, will rewind to submit[0]")
                task_process.rewind_to_step(0)

        for downstream_task_key in task_process.task_conf.downstream_resets:
            to_delete = [
                Path(d, downstream_task_key).__str__()
                for d in [task_process.pipeline_work_dir, task_process.pipeline_output_dir]
            ]
            for d in to_delete:
                shutil.rmtree(d, ignore_errors=True)

        task_process.launch_task()

    def reset_task(self):
        task_process = TaskProcess(self._control_dir())
        if Path(task_process.task_output_dir).exists():
            shutil.rmtree(task_process.task_output_dir)
        task_process.rewind_to_step(0)

    def _control_dir(self):
        return Path(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key).__str__()


def run_cli():
    handle_script_lib_main()

def handle_script_lib_main():
    try:
        cli = Cli(sys.argv[1:])
        cli.invoke()
    except Exception as e:
        logging.exception(e)
        raise
    finally:
        logging.shutdown()

def cli_in_sub_process(args):
    py_file = os.path.abspath(__file__)
    cmd = [sys.executable, py_file] + args
    return PortablePopen(cmd)


if __name__ == '__main__':

    if "SLURM_JOB_ID" in os.environ:
        call(sys.argv[2])
    else:
        handle_script_lib_main()
