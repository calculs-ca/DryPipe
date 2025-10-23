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

from dry_pipe import RemotePipelineSpecs, PortablePopen
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

    def __init__(self, args, invocation_script=None, env=None):

        args = _cleanup_args(args)

        self._has_implicit_generator = False
        self._has_implicit_control_dir = False
        self._has_implicit_pid = False
        self._has_implicit_task_key = False

        if env is None:
            self.env = os.environ
        else:
            self.env = env

        self.is_dp_func = environ.get("__IS_DRYPIPE_DP_FUNC") == "True"

        self.parser = argparse.ArgumentParser(
            description="DryPipe CLI"
        )

        self._add_pipeline_instance_dir_arg(self.parser)

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
            help="don't actualy run, but print what will run (implicit --verbose)",
        )

        self._sub_parsers()

        if is_inside_slurm_job():
            args = ["task", os.environ["DRYPIPE_TASK_CONTROL_DIR"]]

        self.parsed_args = self.parser.parse_args(args)

    def _add_pipeline_instance_dir_arg(self, parser):

        default_pid = self.env.get("DRYPIPE_PIPELINE_INSTANCE_DIR")

        if default_pid is None:
            control_dir = self._guess_control_dir_from_cwd()

            if control_dir is not None:
                default_pid = os.path.dirname(os.path.dirname(control_dir))

        if default_pid is None:
            if Path(__file__).name == "cli":
                default_pid = Path(__file__).parent.parent.parent
            elif Path(__file__).name == "cli.py" and Path(__file__).parent.parent.name == ".drypipe":
                default_pid = Path(__file__).parent.parent.parent

        if default_pid is not None:
            self._has_implicit_pid = True

        parser.add_argument(
            '--pipeline-instance-dir',
            help='pipeline instance directory, can also be set with environment var DRYPIPE_PIPELINE_INSTANCE_DIR',
            default=default_pid
        )

    def _guess_control_dir_from_cwd(self):
        cwd = Path.cwd()
        task_conf = os.path.join(cwd, "task-conf.json")
        if os.path.exists(task_conf):
            return cwd
        else:
            return None

    def _implicit_control_dir(self):
        DRYPIPE_DP_HINT_DIR = os.environ.get("DRYPIPE_DP_HINT_DIR")
        if DRYPIPE_DP_HINT_DIR is not None:
            icd = Path(DRYPIPE_DP_HINT_DIR)
            if icd.parent.name == ".drypipe":
                self._has_implicit_control_dir = True
                return icd

        return None

    def _add_task_key_parser_arg(self, parser):

        icd = self._implicit_control_dir()

        if icd is not None:
            implicit_task_key = icd.name
            self._has_implicit_task_key = True
        else:
            control_dir = self._guess_control_dir_from_cwd()
            if control_dir is not None:
                implicit_task_key = os.path.basename(control_dir)
                self._has_implicit_task_key = True
            else:
                implicit_task_key = None

        parser.add_argument(
            '--task-key',
            default=implicit_task_key
        )

    def _control_dir(self):
        pipeline_instance_dir = self.parsed_args.pipeline_instance_dir
        task_key = self.parsed_args.task_key
        return os.path.join(pipeline_instance_dir, ".drypipe", task_key)

    def _complete_control_dir(self, maybe_partial_control_dir):
        if os.path.exists(maybe_partial_control_dir):
            return os.path.abspath(maybe_partial_control_dir)

        cd = os.path.join(os.getcwd(), maybe_partial_control_dir)

        if os.path.exists(cd):
            return cd

        raise Exception(f"directory not found {cd}")

    def _wait(self):
        return self.parsed_args.wait

    def _tail(self):
        return self.parsed_args.tail

    def _tail_all(self):
        return self.parsed_args.tail_all

    def get_ssh_remote_dest_or_none(self, task_conf):
        ssh_remote_dest = task_conf.get("ssh_remote_dest")
        if ssh_remote_dest is None:
            if self.parsed_args.ssh_remote_dest is None:
                raise Exception(
                    f"--ssh-remote-dest is required for 'array-upload', OR must be defined with " +
                    " .task(task_conf=TaskConf(ss_remote_dest=...)"
                )
            else:
                ssh_remote_dest = self.parsed_args.ssh_remote_dest

    def invoke(self, test_mode=False):

        if self._has_implicit_pid:
            print(f"implicit --pipeline-instance-dir={self.parsed_args.pipeline_instance_dir}")
        if self._has_implicit_task_key:
            print(f"implicit --task-key={self.parsed_args.task_key}")
        if self._has_implicit_generator:
            if hasattr(self.parsed_args, "generator"):
                print(f"implicit --generator={self.parsed_args.generator}")


        if self.parsed_args.v:
            setup_verbose1()
        elif self.parsed_args.vv:
            setup_verbose2()

        def pipeline_instance_from_args():
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

        if self.parsed_args.command == 'array-submit':
            task_process = TaskProcess(
                self._control_dir(),
                as_subprocess=not test_mode,
                test_mode=test_mode
            )
            task_process.run(
                array_limit=self.parsed_args.limit
            )
        elif self.parsed_args.command == 'run':
            pipeline_instance = pipeline_instance_from_args()
            pipeline_instance.prepare_instance_dir()
            if not test_mode:
                pipeline_instance.monitor = CliMonitor(pipeline_instance, self.parsed_args.generator)

            pipeline_instance.run(
                until_patterns=self.parsed_args.until,
                restart_failed=self.parsed_args.restart_failed,
                reset_failed=self.parsed_args.reset_failed,
                sleep_schedule=self.parsed_args.sleep_schedule
            )
        elif self.parsed_args.command == 'service':


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

        elif self.parsed_args.command == 'prepare':
            pipeline_instance = pipeline_instance_from_args()
            pipeline_instance.prepare_instance_dir()
            pipeline_instance.run_sync(["*"], sleep_schedule=self.parsed_args.sleep_schedule)
        elif self.parsed_args.command == 'call':

            call(self.parsed_args.module_function)

        elif self.parsed_args.command == 'task':
            task_process = TaskProcess(
                self._complete_control_dir(self.parsed_args.control_dir),
                wait_for_completion=self._wait(),
                test_mode=test_mode,
                as_subprocess=not test_mode,
                tail=self._tail(),
                tail_all=self._tail_all(),
                from_remote=self.parsed_args.from_remote
            )

            if self.parsed_args.ssh_remote_dest is not None:
                task_process.task_conf.ssh_remote_dest = self.parsed_args.ssh_remote_dest
            elif task_process.task_conf.executer_type == "slurm":
                if self.parsed_args.by_runner and not task_process.task_conf.is_slurm_parent:
                    task_process.submit_sbatch_task()
                    return

            task_process.launch_task()

        elif self.parsed_args.command == 'remote-exec':
            control_dir = self._control_dir()
            task_process = TaskProcess(
                control_dir,
                wait_for_completion=False,
                test_mode=test_mode,
                as_subprocess=not test_mode,
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

        elif self.parsed_args.command == 'submit-array-from-remote':
            control_dir = self._control_dir()
            task_process = TaskProcess(control_dir, use_remote_drypipe_log=True)
            res = submit_local_array.func(task_process)
            #task_process.task_logger.info("submitted array from remote %s", json.dumps(res))
            print(json.dumps(res))
        elif self.parsed_args.command == 'watch-array-from-remote':
            control_dir = self._control_dir()
            task_process = TaskProcess(control_dir, use_remote_drypipe_log=True, for_dry_run=self.parsed_args.dry_run)
            atm = task_process.create_array_task_manager()
            report = atm.manage_auto_restarts_from_remote()
            print(json.dumps(report))
        elif self.parsed_args.command == 'poll-task':
            control_dir = self._control_dir()
            task_process = TaskProcess(control_dir, no_logger=True)

            s = list(Path(control_dir).glob("state.*"))

            if len(s) == 0:
                raise Exception(f"no state file in {control_dir}")
            elif len(s) > 1:
                raise Exception(f"multiple state files in {control_dir}")

            state_file = s[0]
            print(f"{state_file.absolute()}")

        elif self.parsed_args.command == 'sbatch':
            task_process = TaskProcess(self.parsed_args.control_dir, wait_for_completion=self._wait())
            task_process.submit_sbatch_task()

        elif self.parsed_args.command == 'fetch-remote-state':
            task_process = TaskProcess(
                os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
                wait_for_completion=self._wait(),
                alternate_logger=logger
            )
            task_process.fetch_remote_state()
        elif self.parsed_args.command == 'sbatch-gen':
            task_process = TaskProcess(self.parsed_args.control_dir, wait_for_completion=self._wait())
            print(" ".join(task_process.sbatch_cmd_lines()))

        elif self.parsed_args.command == 'array-upload':

            task_process = TaskProcess(self._control_dir())

            if self.parsed_args.ssh_remote_dest is not None:
                task_process.task_conf.ssh_remote_dest = self.parsed_args.ssh_remote_dest

            array_parent_task = SlurmArrayParentTask(task_process)

            array_parent_task._upload_array()

        elif self.parsed_args.command == 'array-download':

            task_process = TaskProcess(
                os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
                alternate_logger=logger
            )

            if self.parsed_args.ssh_remote_dest is not None:
                task_process.task_conf.ssh_remote_dest = self.parsed_args.ssh_remote_dest

            array_parent_task = SlurmArrayParentTask(task_process)

            array_parent_task._download_array()

        elif self.parsed_args.command == 'reconcile-with-squeue':

            task_process = TaskProcess(
                os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
                alternate_logger=logger
            )

            array_parent_task = SlurmArrayParentTask(task_process)

            for k, v in array_parent_task.compare_and_reconcile_squeue_with_state_files().items():
                print(f"{k}: {v}")


        elif self.parsed_args.command == 'create-array-parent':

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
        elif self.parsed_args.command == 'list-states':
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

        elif self.parsed_args.command == 'restart':
            self.restart_task()
        elif self.parsed_args.command == 'reset':
            self.reset_task()
        elif self.parsed_args.command == 'restart-failed-array-tasks':
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

        elif self.parsed_args.command == 'report-perf':

            if self.parsed_args.pipeline_instance_dir is None:
                raise Exception(f"--pipeline-instance-dir must be specified")


            if self.parsed_args.task_key is not None and self.parsed_args.filter == "*":
                f = self.parsed_args.task_key
            else:
                f = self.parsed_args.filter

            for task_key, timer_label, hms, s in timers_for_tasks(self.parsed_args.pipeline_instance_dir, f):
                print(f"{timer_label}\t{task_key}\t{hms}\t{s}")

    def _sub_parsers(self):


        self.subparsers = self.parser.add_subparsers(required=True, dest='command')
        self.add_run_args(self.subparsers.add_parser('run'))
        self.add_service_args(self.subparsers.add_parser('service'))
        self.add_report_args(self.subparsers.add_parser('report-perf'))
        prepare_parser = self.subparsers.add_parser('prepare')
        self.add_generator_arg(prepare_parser)
        self._add_sleep_schedule_args(prepare_parser)
        self.add_call_args(self.subparsers.add_parser('call'))
        self.add_task_args(self.subparsers.add_parser('task'))
        self.add_task_args(self.subparsers.add_parser('reset'))
        restart_cmd = self.subparsers.add_parser('restart')
        self.add_task_args(restart_cmd)
        restart_cmd.add_argument(
            '--reset',
            help='restart the task from the first step, clears the results directory if exists',
            action='store_true'
        )
        restart_cmd.add_argument(
            '--at-step',
            type=int,
            help='restarts the task at the specified step (zero based).',
            default=None
        )


        self.add_task_args(self.subparsers.add_parser('poll-task'))
        self.add_task_args(self.subparsers.add_parser('remote-exec'))
        self.add_task_args(self.subparsers.add_parser("submit-array-from-remote"))
        self.add_task_args(self.subparsers.add_parser("watch-array-from-remote"))

        fetch_remote_state = self.subparsers.add_parser('fetch-remote-state')
        self._add_task_key_parser_arg(fetch_remote_state)
        self.__wait_arg(fetch_remote_state)

        self.add_sbatch_args(self.subparsers.add_parser('sbatch'))
        self.add_sbatch_args(self.subparsers.add_parser('sbatch-gen'))
        self.add_array_args(self.subparsers.add_parser('array-submit'))
        self.add_upload_download_array_args(self.subparsers.add_parser('array-upload'))
        self.add_upload_download_array_args(self.subparsers.add_parser('array-download'))
        self.add_create_array_parent_args(self.subparsers.add_parser('create-array-parent'))
        self.add_upload_download_array_args(self.subparsers.add_parser('array-zombies'))
        list_state_parser = self.subparsers.add_parser('list-states')
        self._add_task_key_parser_arg(list_state_parser)

        list_state_parser.add_argument(
            '--gen-rsync-list',
            help='generate rsync list for file sets',
            action='store_true',
            default=False
        )

        restart_array = self.subparsers.add_parser('restart-failed-array-tasks')
        gen_array_rsync_list = self.subparsers.add_parser('array-rsync-list')
        self._add_task_key_parser_arg(gen_array_rsync_list)

        #self._add_task_key_parser_arg(restart_array)
        self.add_array_args(restart_array)

        restart_array.add_argument(
            '--include-pre-launch',
            help='Also restart tasks that have failed to launch (useful after scancel on an array)',
            action='store_true',
            default=False
        )


    def add_status_args(self):
        pass

    def add_generator_arg(self, parser):

        ig = self.env.get("DRYPIPE_PIPELINE_GENERATOR")

        if ig is not None:
            self._has_implicit_generator = True

        parser.add_argument(
            '--generator',
            help='<module>:<function> task generator function, can also be set with environment var DRYPIPE_PIPELINE_GENERATOR',
            metavar="GENERATOR",
            default=ig
        )

    def add_report_args(self, report_parser):
        report_parser.add_argument(
            '--filter',
            help='glob expression to filter tasks',
            default='*'
        )

        self._add_task_key_parser_arg(report_parser)

        self._add_pipeline_instance_dir_arg(report_parser)


    def add_run_args(self, run_parser):

        self.add_generator_arg(run_parser)

        run_parser.add_argument(
            '--until', help='tasks matching PATTERN will not be started',
            action='append',
            metavar='PATTERN'
        )

        self._add_task_key_parser_arg(run_parser)

        self._add_restart_failed_args(run_parser)
        self._add_sleep_schedule_args(run_parser)


    def _add_sleep_schedule_args(self, parser):

        class ListOfInts:
            def __call__(self, txt):
                return [int(s) for s in txt.split(",")]

        parser.add_argument(
            "--sleep-schedule",
            action=EnvDefault,
            envvar="DRYPIPE_SERVICE_SLEEP_SCHEDULE",
            env=self.env,
            help="a list of sleep times in seconds, for the main loop of the service, can also be set with environment var DRYPIPE_SERVICE_SLEEP_SCHEDULE",
            default="0,1,3,5,10,15,20",
            type=ListOfInts()
        )

    def add_service_args(self, service_parser):
        service_parser.add_argument(
            "--config-generator",
            action=EnvDefault,
            envvar="DRYPIPE_SERVICE_CONFIG_GENERATOR",
            env=self.env,
            help="""a function that yields instances of dry_pipe.pipeline.PipelineType, 
                    can also be set with environment var DRYPIPE_SERVICE_CONFIG_GENERATOR""",
        )

        self._add_sleep_schedule_args(service_parser)

        service_parser.add_argument(
            "--log-conf",
            action=EnvDefault,
            envvar="DRYPIPE_LOGGING_CONF",
            env=self.env,
            help="the path to a logging configuration file, can also be set with environment var DRYPIPE_LOGGING_CONF",
            required=False
        )

    def add_upload_download_array_args(self, upload_array_parser):

        self.upload_array_parser = upload_array_parser

        self.add_ssh_remote_dest_arg(upload_array_parser)
        self._add_task_key_parser_arg(upload_array_parser)

    def add_ssh_remote_dest_arg(self, parser):
        parser.add_argument(
            '--ssh-remote-dest',
            help=textwrap.dedent(
            """
                example:`me@myhost.example.com:/my-directory`            
            """)
        )

    def add_array_args(self, run_parser):
        run_parser.add_argument(
            '--filter',
            help=textwrap.dedent(
            """
            reduce the set of task that will run, with task-key match pattern: TASK_KEY(:STEP_NUMBER)?
            ex:
                --filter=my_taskABC
                --filter=my_taskABC:3            
            """)
        )

        run_parser.add_argument(
            '--limit', type=int, help='limit submitted array size to N tasks', metavar='N'
        )

        self._add_slurm_account_arg(run_parser)

        self._add_task_key_parser_arg(run_parser)

        run_parser.add_argument(
            '--slurm-args',
            help="string that will be passed as argument to the sbatch invocation"
        )

        run_parser.add_argument(
            '--restart-at-step',
            help='task key',
        )

        self._add_restart_failed_args(run_parser)

        self.__wait_arg(run_parser)

    def _add_restart_failed_args(self, parser):
        parser.add_argument(
            '--restart-failed',
            action='store_true', default=False,
            help='re submit failed tasks in array, restart from last failed step, keep previous output'
        )

        parser.add_argument(
            '--reset-failed',
            action='store_true', default=False,
            help='delete and re submit failed tasks in array'
        )


    def add_create_array_parent_args(self, parser):
        parser.add_argument('new_task_key', type=str)
        parser.add_argument(
            'matcher', type=str,
            help="a glob expression to match the tasks that will become children of created parent"
        )

        parser.add_argument(
            '--split', type=int, default=1,
            help="create N parent Tasks, and distribute the children evenly tasks among parents"
        )

        parser.add_argument('--force', action='store_true')

        self._add_slurm_account_arg(parser)

    def _add_slurm_account_arg(self, parser):
        parser.add_argument(
            '--slurm-account'
        )

    def __wait_arg(self, parser):
        parser.add_argument(
            '--wait',
            dest='wait',
            action='store_true',
            help="wait for task to complete before exiting"
        )
        parser.set_defaults(wait=False)

    def add_task_args(self, parser):
        parser.add_argument('control_dir', type=str, nargs='?', default=str(self._implicit_control_dir()))
        self._add_task_key_parser_arg(parser)
        self.__wait_arg(parser)
        parser.add_argument("--by-runner", dest="by_runner", action="store_true")
        parser.set_defaults(by_runner=False)
        self.add_ssh_remote_dest_arg(parser)
        parser.add_argument("--tail", dest="tail", action="store_true")
        parser.add_argument("--tail-all", dest="tail_all", action="store_true")
        parser.add_argument("--from-remote", dest="from_remote", action="store_true")


    def add_sbatch_args(self, parser):
        self.add_task_args(parser)

    def add_call_args(self, parser):
        parser.add_argument('module_function', type=str)
        self._add_task_key_parser_arg(parser)

    def restart_task(self):

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
