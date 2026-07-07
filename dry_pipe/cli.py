import argparse
import ast
import ctypes
import ctypes.util
import fnmatch
import inspect
from itertools import groupby
import shutil
import signal
import subprocess
import tarfile
import tempfile
import time
import json
import logging
import logging.config
import os
import sys
import textwrap
from io import StringIO
from os import environ
from pathlib import Path

from dry_pipe import PortablePopen, DryPipe
from dry_pipe.core_lib import func_from_mod_func, is_inside_slurm_job, create_instance_logger
from dry_pipe.pipeline_instance import Monitor, PipelineInstance
from dry_pipe.task_process import TaskPackExchausted, TaskProcess, TaskFailedException
from dry_pipe.slurm_array_task import SlurmArrayParentTask
from dry_pipe.reports import timers_for_tasks
from dry_pipe.state_machine import StateFileTracker
from dry_pipe.service import PipelineRunner
from dry_pipe.task_lib import submit_local_array, upload_task_inputs_rsync


logger = logging.getLogger(__name__)

def call(mod_func):

    python_task = func_from_mod_func(mod_func)
    control_dir = os.environ["__control_dir"]
    task_process = TaskProcess(control_dir, is_python_call=True, no_logger=True)
    try:
        task_process._create_task_logger()
        task_process.call_python(mod_func, python_task)
    except TaskFailedException:
        if task_process.as_subprocess:
            os._exit(1)
    except Exception:
        if task_process.as_subprocess:
            os._exit(1)        



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


def _parse_func_filter_spec(spec):
    """
    Parses a --func-filter value into (module_func_reference, args, kwargs).

    Accepts a bare reference ("a.b.c:f" or "f") or a call form ("a.b.c:f('z', threshold=3)").
    The reference part is returned verbatim (the ':' and '.' make it invalid python, so it is not
    parsed); the call arguments are parsed as python literals (ast.literal_eval, no code executed).
    """
    paren = spec.find("(")
    if paren == -1:
        return spec.strip(), [], {}

    if not spec.rstrip().endswith(")"):
        raise Exception(
            f"--func-filter '{spec}' is malformed, expected 'module:func' or \"module:func(args...)\""
        )

    reference = spec[:paren].strip()
    call_source = spec[paren:].strip()

    try:
        call_node = ast.parse(f"__f__{call_source}", mode="eval").body
    except SyntaxError:
        raise Exception(f"--func-filter '{spec}' has an invalid argument list")

    if not isinstance(call_node, ast.Call):
        raise Exception(f"--func-filter '{spec}' has an invalid argument list")

    try:
        args = [ast.literal_eval(a) for a in call_node.args]
        kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in call_node.keywords}
    except ValueError:
        raise Exception(
            f"--func-filter '{spec}' arguments must be python literals (str, int, ...), not expressions"
        )

    return reference, args, kwargs


class Cli:

    @staticmethod
    def invoke_and_get_str(*args, **kwargs):
        out = StringIO()
        kwargs = {
            **kwargs,
            **{"output": out}
        }
        cli = Cli(args, **kwargs)
        cli.invoke()
        return out.getvalue()

    @staticmethod
    def invoke_and_iterate_lines(*args, **kwargs):
        s = Cli.invoke_and_get_str(*args, **kwargs)
        for line in  s.split("\n"):
            line = line.strip()
            if line != "":
                yield line

    def __init__(self, args, env=None, test_mode=False, output=sys.stdout):

        self.raw_command_line = " ".join(args)
        self.args = _cleanup_args(args)
        self.output = output

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

        self.parse_args()

        if is_inside_slurm_job():
            # this process is a slurm job (or a step of one) that the CLI itself submitted;
            # its logging belongs in the task's own drypipe.log, not instance.log:
            # instance.log should read like what --verbose would show a user, and concurrent
            # slurm jobs writing to one shared instance.log is a concurrency hazard.
            self.instance_logger = None
        else:
            pid = getattr(self.parsed_args, 'pipeline_instance_dir', None)
            if pid:
                self.instance_logger = create_instance_logger(
                    pid, level=logging.DEBUG if self.parsed_args.vv else logging.INFO
                )
                self.instance_logger.info("cli invoked: %s", self.raw_command_line)
            else:
                self.instance_logger = None

    def create_logger(self, logging_level):

        logger = logging.getLogger("cli")
        logger.setLevel(logging_level)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging_level)

        class F(logging.Formatter):
            def format(self, record):
                s = super().format(record)
                srz = record.__dict__.get("srz")
                if srz is None:
                    return s
                else:
                    return f"{srz}: {s}"

        handler.setFormatter(F())

        logger.addHandler(handler)

        logger.debug(f"Logging level: {logging.getLevelName(logging_level)}")

        return logger
    
    def parse_args(self):

        if is_inside_slurm_job():

            tcd = Path(os.environ["DRYPIPE_TASK_CONTROL_DIR"])
            task_key = tcd.name.__str__()
            pid = tcd.parent.parent.__str__()

            is_packed_job = "DRYPIPE_TASKS_PER_JOB" in os.environ

            if is_packed_job:
                cmd = "run-from-slurm-packed-job"
            else:
                cmd = "run-from-slurm-job"

            self.parsed_args = self.parser.parse_args([
                cmd,
                f"--pipeline-instance-dir={pid}",
                f"--task-key={task_key}"
            ])
        else:
            self.parsed_args = self.parser.parse_args(self.args)

    def invoke(self):

        if self.parsed_args.dry_run:
            print(f"DRY RUN {self.parsed_args.command}")

        if self.parsed_args.v or self._tail():
            self.logger = self.create_logger(logging.INFO)
        elif self.parsed_args.vv:
            self.logger = self.create_logger(logging.DEBUG)
        else:
            self.logger = logger

        method = self.command_names_to_method.get(self.parsed_args.command)

        if method is None:
            raise Exception(f"method {method} should exist, for command {self.parsed_args.command}")

        method()


    def invoke_in_subprocess(self):
        return cli_in_sub_process(self.args)

    def _enumerate_commands(self):

        class ListOfFloats:
            def __call__(self, txt):
                return [float(s) for s in txt.split(",")]



        def pipeline_instance_dir(parser):
            parser.add_argument(
                '--pipeline-instance-dir', '-pid',
                help='pipeline instance directory, can also be set with environment var DRYPIPE_PIPELINE_INSTANCE_DIR',
                action=EnvDefault,
                envvar="DRYPIPE_PIPELINE_INSTANCE_DIR",
                env=self.env
            )

        def pipeline_instances_dir(parser):
            parser.add_argument(
                '--pipeline-instances-dir',
                help='parent dir of all pipeline instance directories, can also be set with environment var DRYPIPE_PIPELINE_INSTANCES_DIR',
                action=EnvDefault,
                envvar="DRYPIPE_PIPELINE_INSTANCES_DIR",
                env=self.env
            )

        def task_key(parser):
            pipeline_instance_dir(parser)
            parser.add_argument('--task-key', '-k', required=True, help="task key")

        def task_key_optional(parser):
            pipeline_instance_dir(parser)
            parser.add_argument('--task-key', '-k', required=False, default=None)

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
            parser.add_argument("--tail", dest="tail", action="store_true", help="tail the task output file ./drypipe/<task_key>/out.log")
            parser.add_argument(
                "--tail-all", dest="tail_all", action="store_true",
                help="same as --tail, plus drypipe internal task log i.e. .drypipe/<task_key>/drypipe.log, and logs emitted in the task generator function"
            )

        def sbatch_options(parser):
            parser.add_argument(
                '--sbatch-options',
                type=str,
                help="a list of space separated options that will be appended directly to sbatch",
                required=False
            )

        def from_remote(parser):
            parser.add_argument("--from-remote", dest="from_remote", action="store_true")

        def filter(parser):
            parser.add_argument(
                '--filter', '--key-filter',
                help='''
                glob expression applied to task keys, ex: "t_a*".

                HOW FILTERS COMBINE
                Filters come in three independent groups. A task is INCLUDED only when it passes ALL of the
                filters that are set (logical AND); it is EXCLUDED as soon as it fails any one of them.
                A group that is not set matches everything (it never excludes on its own).

                  1. key       : --filter / --key-filter   (glob on the task key)
                  2. state/step: --py-filter, --filter-completed, --filter-not-completed, --filter-failed
                  3. function  : --func-filter              (a python function returning a boolean)

                Within group 2 the options are mutually exclusive: if several are given, exactly one wins,
                in this precedence order: --filter-completed > --filter-not-completed > --filter-failed > --py-filter.

                So --filter and --func-filter (and one option from group 2) INTERSECT: e.g.
                    --filter=t_a* --filter-failed --func-filter=mod:f
                selects the tasks whose key matches t_a*, AND are failed, AND for which f(...) returns True.

                With no filter set, all tasks are selected. Filters can be tested with "low consequence"
                commands such as list-keys or summary before more consequential ones such as array-submit.
                ''',
                default='*'
            )

        def py_filter(parser):
            parser.add_argument(
                '--py-filter',
                help='''
                python boolean expression with {state} or {step} variables available, ex:                                 
                   "{step} > 3",                    
                   "{step} in [1,4]" 
                   "{step} == 2 and {state} in ['failed', 'timed-out']"                                      
                    {step} will be replaced by step number, then will be evaluated as a python boolean expression.
                    
                Note: a --py-filter can be tested with "low consequence" commands such as list-keys or summary, 
                before more consequential commands such as array-submit                                 
                '''
            )

        def func_filter(parser):
            parser.add_argument(
                '--func-filter',
                help='''
                a factory function imported from a module on the PYTHONPATH, using the "module:function"
                object-reference format (as in setuptools entry points / gunicorn), ex: a.b.c:f meaning
                "from a.b.c import f". As a shortcut, a bare function name (no module) is looked up in the
                --generator module.

                The referenced function is a FACTORY: it is invoked (with a call syntax, arguments are python
                literals) and must RETURN the filter function, which is then called with (key, state_name, step)
                for each task and returns a boolean indicating whether the task is selected, ex:

                   --func-filter="a.b.c:make_filter()"                # zero-arg factory
                   --func-filter="a.b.c:make_filter(threshold=3)"     # with arguments

                   def make_filter(threshold=0):
                       def f(key, state_name, step):
                           return step > threshold
                       return f
                '''
            )

        def filter_not_completed(parser):
            parser.add_argument(
                '--filter-not-completed',
                help='filters all incomplete tasks',
                action='store_true',
                default=False
            )
        
        def filter_completed(parser):
            parser.add_argument(
                '--filter-completed',
                help='filters all incomplete tasks',
                action='store_true',
                default=False
            )            

        def filter_failed(parser):
            parser.add_argument(
                '--filter-failed',
                help='filters failed tasks',
                action='store_true',
                default=False
            )
            

        def grep_expr(parser):
            parser.add_argument(
                '--grep-expr', '-e',
                help="grep expression, uses fgrep as backend"
            )

        def reset(parser):
            parser.add_argument(
                '--reset',
                help='restart the task from the first step, clears the results directory if exists',
                action='store_true'
            )

        def regen(parser):
            parser.add_argument(
                '--regen',
                help='rewrites all generated files for the task before running (task-conf.json, bash snippets)'+\
                     'note: this flag is neither necessary or available for "run" and "prepare, because these commands detect changes and updates task config accordingly"',
                action='store_true',
                default=False
            )

        def generator(parser):
            return parser.add_argument(
                '-g', '--generator',
                help='<module>:<function> task generator function (a function that yields tasks, see "generator function"), can also be set with environment var DRYPIPE_PIPELINE_GENERATOR',
                action=EnvDefault,
                envvar="DRYPIPE_PIPELINE_GENERATOR",
                metavar="GENERATOR",
                env=self.env
            )

        def generator_optional(parser):
            arg = generator(parser)
            arg.required = False

        def at_step(parser):
            parser.add_argument(
                '--at-step',
                type=int,
                help='restarts the task at the specified step (zero based).',
                default=None
            )

        def state(parser):
            parser.add_argument(
                '--state',
                type=str,
                help='state of a task (ready.0, failed.3, etc)',
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

        def include_all_incompleted_tasks(parser):
            parser.add_argument(
                '--include-all-incompleted-tasks',
                help='all incimpleted tasks (failed, timed-out, and zombie (started status but no actual running task), typically used to recover from a crash',
                action='store_true',
                default=False
            )

        def exit_on_parent_death(parser):
            parser.add_argument(
                '--exit-on-parent-death',
                help='ensure the process dies when the parent process exits',
                type=str,
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
                type=ListOfFloats()
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

        def log_conf(parser):
            parser.add_argument(
                "--log-conf",
                action=EnvDefault,
                envvar="DRYPIPE_LOGGING_CONF",
                env=self.env,
                help="the path to a logging configuration file, can also be set with environment var DRYPIPE_LOGGING_CONF",
                required=False
            )


        _s = self.parser.add_subparsers(required=True, dest='command')
        self.subparsers = _s

        class Command:
            def __init__(self, name, *args, **kwargs):
                self.name = name
                sub_parser = _s.add_parser(name, help=kwargs.get("help"))
                for a in args:
                    try:
                        a(sub_parser)
                    except Exception as e:
                        raise Exception(f"arg {a.__name__}  on command {name} failed with exception {e}")

        def all_filters():
            return [filter, py_filter, func_filter, filter_completed, filter_not_completed, filter_failed]

        yield Command('run', pipeline_instance_dir, generator, until, restart_failed, reset_failed, sleep_schedule,
                      help="generate tasks and run the pipeline")

        yield Command('rewind', pipeline_instance_dir, generator, at_step, *all_filters(),
                      help="transition all selected tasks (--filter and --py-filter) to step X sepcified by --at-step=X")
        
        yield Command('set-state', pipeline_instance_dir, generator, state, *all_filters(),
                      help="transition all selected tasks (--filter and --py-filter) to state S sepcified by --state")        

        yield Command('list-keys', pipeline_instance_dir, generator, *all_filters(),
                      help="list keys in the pipeline instance that can possibly generated by the DAG")

        yield Command('list', pipeline_instance_dir, generator, *all_filters(),
                      help="list keys, states, and step in the pipeline instance that can possibly generated by the DAG")


        yield Command('status', pipeline_instance_dir, *all_filters(), generator_optional,
                      help="list the states of tasks that exist so far in the pipeline instance")
        
        yield Command('summary', pipeline_instance_dir, *all_filters(), generator_optional,
                      help="last aggregate counts of (state, step)")

        yield Command('prepare', pipeline_instance_dir, generator, until, sleep_schedule,
                      help="generate tasks, WITHOUT running the pipeline")

        yield Command('service', pipeline_instances_dir, config_generator, sleep_schedule, log_conf, exit_on_parent_death,
                      help="run as service")

        yield Command('upgrade-drypipe', pipeline_instance_dir,
                      help="upgrade drypipe version for the specified pipeline instance")

        #yield Command('restart-failed-array-tasks', task_key, include_pre_launch, wait,
        #              help="restart failed array tasks, of specified array task")

        def include_steps(parser):
            parser.add_argument(
                '--include-steps',
                help='include time for all steps',
                action='store_true',
                default=False
            )

        yield Command('report-execution-times', task_key_optional, include_steps, *all_filters(),
                      help="execute time for all tasks, or all tasks matching filter expression")

        yield Command('run-from-slurm-job', task_key)

        yield Command('run-from-slurm-packed-job', task_key)

        yield Command('task', task_key, wait, tail, by_runner, from_remote, ssh_remote_dest, regen, generator_optional, at_step, reset,
                      help="run specified task, or restarts it if in failed state (see restart command)")

        yield Command('restart', task_key, at_step, reset, wait, tail, from_remote, regen,
                      help="restart specified task --task-key, at last unsuccessful step. WARNING: if task is completed, will restart from first step")

        yield Command('instance-info', help="dumps basic information about the pipeline instance")

        yield Command('poll-task', task_key, help="return state of task (used for polling remote tasks)")

        yield Command('remote-exec', task_key, wait, help="execute remote tasks")


        yield Command('fetch-remote-state', task_key, wait)
        yield Command('upload-drypipe-for-remote-instance', task_key)
        yield Command('upload-task-inputs', task_key)

        yield Command('sbatch', task_key_optional, wait, regen, generator_optional, sbatch_options, reset, at_step, *all_filters(),
                      help="launch task (specified by --task-key, or by combination of --filter --py-filter) with sbatch")

        yield Command('sbatch-gen', task_key, regen, generator_optional, sbatch_options, reset,
                      help="print sbatch command for launching task, without invoking it")

        yield Command('dump-env', task_key, help="dump all environment variables of specified task")


        def tasks_per_job(parser):
            parser.add_argument(
                '--tasks-per-job',
                type=int,
                help='group N child tasks into each slurm array job, running them sequentially',
                default=None
            )

        def slurm_max_jobs(parser):
            parser.add_argument(
                '--slurm-max-jobs',
                type=int,
                help='adds %N at the end of the sbatch array spec, ex: --array-2000%N',
                default=None
            )


        yield Command('array-submit',
                      task_key, limit, regen, generator_optional, tail, wait, include_all_incompleted_tasks,
                      sbatch_options, reset, tasks_per_job, slurm_max_jobs, *all_filters(),
                      help="submit array")

        yield Command('array-upload', task_key, help="upload array task to remote location")
        yield Command('array-download', task_key, help="download all array tasks results (rsync or Globus fetch all __task_output_dir of child tasks)")
        yield Command("array-submit-from-remote", task_key, wait, help="submit array task to remote location")
        yield Command("array-watch-from-remote", task_key, wait, help="watch (poll and fetch children status) of array task at remote location")
        yield Command('array-create-parent', task_key, help="create a parent array task with matching tasks")
        yield Command('list-states', task_key, gen_rsync_list)
        yield Command('array-rsync-list', task_key)

        yield Command('reset', task_key_optional, generator, *all_filters(),)

        yield Command( 'grep-logs', pipeline_instance_dir, grep_expr, generator, *all_filters(), help="applies fgrep on out.log files of matching tasks")

        def tar_file(parser):
            parser.add_argument(
                '--name',
                type=str,
                help='basename of tar.gz file, if it does not end with .tar.gz, this ending will be appended to the name, if not absolute, will be relative to $PWD',
                required=True
            )

        def tar_file_tail_log(parser):
            parser.add_argument(
                '--tail-logs',
                action='store_true',
                default=False,
                help='include a tail of all out.log'
            )

        def tail_n(parser):
            parser.add_argument(
                '--n', '-n',
                help="number of lines for tail n",
                type=int,
                default=50,
            )

        yield Command(
            'tar-gz', task_key_optional, tar_file, generator, tar_file_tail_log, tail_n, *all_filters(),
            help="creates a .tar.gz with .drypipe/<task_key>/* and output/<task_key>/*"
        )

        yield Command('tail-logs', tail_n, generator, pipeline_instance_dir, *all_filters(),
                      help="applies tail on out.log files of matching tasks")


        def module_function(parser):
            parser.add_argument('module_function', type=str)

        yield Command('call', module_function, task_key)


    def pipeline_instance_from_args(self):

        generator_mod_func = self.parsed_args.generator
        if generator_mod_func is None:
            raise Exception(f"--generator is required")

        generator_func = func_from_mod_func(generator_mod_func)

        sig = inspect.signature(generator_func)


        def gen_mandatory_params():
            i = 0
            for name, param in sig.parameters.items():
                is_optional = param.default is not inspect.Parameter.empty
                if not is_optional:
                    yield i, param
                i += 1

        mandatory_params = list(gen_mandatory_params())

        def create_pipeline():
            if len(mandatory_params) == 1:

                idx, p0 = mandatory_params[0]
                if idx == 0 and p0.name == "dsl":
                    return DryPipe.create_pipeline(generator_func)

            return generator_func()

        pipeline = create_pipeline()
        pipeline.generator_mod_func = generator_mod_func

        if self.parsed_args.pipeline_instance_dir is None:
            raise Exception(
                f"--pipeline-instance-dir is required, " +
                "or DRYPIPE_PIPELINE_INSTANCE_DIR environment variable must be set"
            )

        self.logger.info("will prepare instance %s", self.parsed_args.pipeline_instance_dir)

        return pipeline.create_pipeline_instance(
            self.parsed_args.pipeline_instance_dir,
            None,
            instance_log_level="DEBUG" if self.parsed_args.vv else "INFO"
        )

    def run(self):
        pipeline_instance = self.pipeline_instance_from_args()
        pipeline_instance.prepare_instance_dir()
        if not self.test_mode and not self.parsed_args.vv:
            pipeline_instance.monitor = CliMonitor(pipeline_instance, self.parsed_args.generator)

        pipeline_instance.for_dry_run = self.parsed_args.dry_run

        def f():
            pipeline_instance.run(
                until_patterns=self.parsed_args.until,
                restart_failed=self.parsed_args.restart_failed,
                reset_failed=self.parsed_args.reset_failed,
                sleep_schedule=self.parsed_args.sleep_schedule
            )
            return pipeline_instance

        self._run_in_rich(f, pipeline_instance)

    def install_rich(self):
        subprocess.check_call([sys.executable, "-m", "pip", "install", "rich==14.3.3"])

    def _is_rich_installed(self):
        try:
            from rich.console import Console
            return True
        except ModuleNotFoundError:

            if self.query_yes_no("This function requires the Rich library, do you want to install it in the current virtual environment ?"):
                self.install_rich()
                return True
            else:
                return False

    def query_yes_no(self, question, default="yes"):
        valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
        if default is None:
            prompt = " [y/n] "
        elif default == "yes":
            prompt = " [Y/n] "
        elif default == "no":
            prompt = " [y/N] "
        else:
            raise ValueError("invalid default answer: '%s'" % default)

        while True:
            sys.stdout.write(question + prompt)
            choice = input().lower()
            if default is not None and choice == "":
                return valid[default]
            elif choice in valid:
                return valid[choice]
            else:
                sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")

    def _run_in_rich(self, f, pipeline_instance):
        if self._is_rich_installed():
            from rich.console import Console
            console = Console()
            try:
                pipeline_instance.catch_exception = False
                f()
            except Exception:
                suppress_list = [
                    "state_machine",
                    "pipeline_instance",
                    "pipeline",
                    "cli"
                ]
                console.print_exception(show_locals=True, suppress=[f"dry_pipe/{f}" for f in suppress_list], max_frames=0, extra_lines=5)
        else:
            f()

    def call(self):
        call(self.parsed_args.module_function)


    def prepare(self):

        pipeline_instance = self.pipeline_instance_from_args()

        def f():
            pipeline_instance.prepare_instance_dir()
            pipeline_instance.regen_matching_tasks()

        self._run_in_rich(f, pipeline_instance)


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

        def work():
            for suggested_sleep in pipeline_runner.iterate_work():
                if suggested_sleep > 0:
                    logging.debug("will sleep for %s", suggested_sleep)
                    time.sleep(suggested_sleep)

                    #mini_sleep = suggested_sleep / 30.0
                    #for i in range(0, 30):
                    #    logging.debug("sleep %s", i)
                    #    time.sleep(mini_sleep)

        if self.parsed_args.exit_on_parent_death is not None:
            pipeline_runner.stop_instances_when_non_runnable = False
            self._work_until_parent_death(work, self.parsed_args.exit_on_parent_death)
        else:
            work()

    def _work_until_parent_death(self, work_func, pid_or_none):

        PR_SET_PDEATHSIG = 1

        def set_parent_death_signal():
            libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
            result = libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0)
            if result != 0:
                errno = ctypes.get_errno()
                raise OSError(errno, f"prctl failed with error {errno}")

        set_parent_death_signal()
        parent_pid = int(pid_or_none) if pid_or_none is not None else os.getppid()

        def parent_is_alive(pid=None):
            if pid is None:
                pid = os.getppid()
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False

        if not parent_is_alive(parent_pid):
            self.logger.info("launching parent process no longer alive, will exit")
            sys.exit(0)

        def handle_sigterm(signum, frame):
            self.logger.info("SIGTERM received")
            sys.exit(0)

        signal.signal(signal.SIGTERM, handle_sigterm)

        work_func()


    def upgrade_drypipe(self):
        PipelineInstance.upgrade_drypipe_in(Path(self.parsed_args.pipeline_instance_dir))

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
        if hasattr(self.parsed_args, 'tail') and self.parsed_args.tail:
            return True

        return hasattr(self.parsed_args, 'tail_all') and self.parsed_args.tail_all

    def _wait(self):
        return self.parsed_args.wait

    def poll_task(self):
        control_dir = self._control_dir()
        task_process = TaskProcess(control_dir, no_logger=True)

        s = list(Path(control_dir).glob("state.*"))

        if len(s) == 0:
            raise Exception(f"no state file in {control_dir}")
        elif len(s) > 1:
            raise Exception(f"multiple state files in {control_dir}")

        state_file = s[0]
        print(f"{state_file.absolute()}", file=self.output)

    def remote_exec(self):
        control_dir = self._control_dir()
        task_process = TaskProcess(
            control_dir,
            wait_for_completion=False,
            test_mode=self.test_mode,
            as_subprocess=not self.test_mode,
            use_remote_drypipe_log=True
        )

        s = list(Path(control_dir).glob("state.*"))

        if len(s) == 0:
            Path(control_dir, "state.waiting").touch(exist_ok=False)
        elif len(s) > 1:
            raise Exception(f"multiple state files in {control_dir}")

        if task_process.task_conf.executer_type == "slurm":
            task_process.submit_sbatch_task(instance_logger=self.instance_logger)
        else:
            task_process.launch_task()

    def sbatch_options_overrider_func(self, original_options):

        if self.parsed_args.sbatch_options is None:
            return original_options
        
        def options_to_dict(options):

            def g():
                for o in options:
                    k, v = o.split("=")
                    yield k, v

            return dict(g())
            
            
            
        d1 = options_to_dict(original_options)
        d2 = options_to_dict(self.parsed_args.sbatch_options.strip().split(" "))


        res = {** d1, ** d2}

        return [
            f"{k}={v}"
            for k, v in res.items()
        ]
                

    def array_submit(self):

        cli_tail_logger = None
        if self._tail():
            cli_tail_logger = self.logger

        if self.parsed_args.reset:
            if self.parsed_args.dry_run:
                print(f"--reset has no effect with --dry-run")
            else:
                pass

        self.prepare()

        task_process = TaskProcess(
            self._control_dir(),
            wait_for_completion=self._wait() or self._tail(),
            test_mode=self.test_mode,
            as_subprocess=not self.test_mode,
            tail=self._tail(),
            tail_all=self.parsed_args.tail_all,
            cli_tail_logger=cli_tail_logger,
            for_dry_run=self.parsed_args.dry_run,
            tasks_per_job=self.parsed_args.tasks_per_job
        )

        if not task_process.is_slurm_array_parent():
            raise Exception(f"task {self.parsed_args.task_key} is not a slurm array")

        array_task_manager = task_process.create_array_task_manager(
            self.parsed_args.slurm_max_jobs, instance_logger=self.instance_logger
        )

        if not self.has_filters():
            array_task_manager.invoke_sacct()

            is_restart = len(array_task_manager.arrays_submitted_sacct_info) > 0

            if is_restart:
                # task_process.rewind_to_step(0)
                task_process.task_logger.info(f"submit_local_array is a restart")
                if not task_process.for_dry_run:
                    for restart_file in Path(task_process.pipeline_work_dir).glob("*/restarts.tsv"):
                        with open(restart_file, "a") as f:
                            f.write("RESET\n")
                else:
                    task_process.task_logger.info(f"no file changed, because it's a dry_run")
        else:
            is_restart = False

        launch_count = 0

        def set_of_task_keys_if_has_filter():
            if not self.has_filters():
                return None

            def g():
                for key, _, _, _ in self.filter_key_state_step(array_task_manager.children_task_keys()):
                    yield key

            return set(g())
        
        set_of_task_keys = set_of_task_keys_if_has_filter()


        for submit in array_task_manager.next_submits(
            restart_failed=is_restart,
            include_all_incompleted=self.parsed_args.include_all_incompleted_tasks,
            set_of_task_keys=set_of_task_keys,
            sbatch_option_overrider=lambda o: self.sbatch_options_overrider_func(o)
        ):

            submit.invoke()
            launch_count += len(submit.task_keys)
        

    def tar_gz(self):
        tar_file = Path(self.parsed_args.name)

        if tar_file.suffix != '.tar.gz':
            tar_file = tar_file.with_suffix(tar_file.suffix + '.tar.gz')

        if not tar_file.is_absolute():
            tar_file = Path.cwd() / tar_file

        tar_file.parent.mkdir(parents=True, exist_ok=True)

        pid = self.parsed_args.pipeline_instance_dir

        def g():
            def g0(k):
                yield Path(pid, f".drypipe/{k}"), f".drypipe/{k}"
                yield Path(pid, f"output/{k}"), f"output/{k}"


            if self.parsed_args.task_key is not None:
                yield from g0(self.parsed_args.task_key)
            else:
                for task_key, state, step, sf in self.filter_key_state_step():
                    yield from g0(task_key)

        sources = list(g())

        existing_sources = [(src, name) for src, name in sources if src.exists()]

        if len(existing_sources) == 0:
            raise FileNotFoundError(
                f"No source directories found. Tried: {', '.join(str(s) for s in sources)}"
            )

        with tarfile.open(tar_file, "w:gz") as tar:

            if self.parsed_args.tail_logs:
                print(f"tailing all logs")
                with tempfile.TemporaryFile(mode='w+t') as temp_file:
                    self.tail_logs(file=temp_file)
                    temp_file.flush()
                    tar.add(tar_file.absolute(), arcname="all-log-tails.txt")

            for src, arc_name in existing_sources:
                tar.add(src, arcname=arc_name)
                print(f"Added {arc_name}")

        print(f"Successfully created {tar_file}")

    def array_submit_from_remote(self):
        control_dir = self._control_dir()
        task_process = TaskProcess(control_dir, use_remote_drypipe_log=True)

        task_process.task_logger.info("raw command line: %s", self.raw_command_line)
        res = submit_local_array.func(task_process)
        # task_process.task_logger.info("submitted array from remote %s", json.dumps(res))
        print(json.dumps(res), file=self.output)

    def array_watch_from_remote(self):

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
        atm = task_process.create_array_task_manager(instance_logger=self.instance_logger)
        report = atm.manage_auto_restarts_from_remote()
        print(json.dumps(report), file=self.output)

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

    def upload_task_inputs(self):
        task_process = TaskProcess(
            os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
            wait_for_completion=True,
            alternate_logger=logger
        )
        upload_task_inputs_rsync.func(task_process)
        task_process.upload_drypipe_for_remote_instance()

    def _extra_sbatch_options_if_any(self):
        if self.parsed_args.sbatch_options is not None:
            return self.parsed_args.sbatch_options.split(" ")
        else:
            return None


    def sbatch(self):

        def submit_one(key):

            self._maybe_regen_task(key)
            
            control_dir = Path(self.parsed_args.pipeline_instance_dir, ".drypipe", key).__str__()

            task_process = TaskProcess(
                control_dir, wait_for_completion=self._wait(), no_logger=True
            )

            if self.parsed_args.at_step is not None:
                task_process.rewind_to_step(self.parsed_args.at_step)
            if self.parsed_args.reset:
                task_process.rewind_to_step(0)                

            task_process.submit_sbatch_task(self._extra_sbatch_options_if_any(), instance_logger=self.instance_logger)

        if self.has_filters():
            for key, _, _, _ in self.filter_key_state_step():
                print(f"will submit {key}")
                submit_one(key)
        else:
            submit_one(self.parsed_args.task_key)


    def sbatch_gen(self):
        self._maybe_regen_task()
        task_process = TaskProcess(self._control_dir())
        print(" ".join(task_process.sbatch_cmd_lines(self._extra_sbatch_options_if_any())), file=self.output)

    def dump_env(self):
        task_process = TaskProcess(self._control_dir(), no_logger=True)

        for k, v in task_process.env.items():
            print(f"export {k}='{v}'", file=self.output)

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

    def array_create_parent(self):

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
            print(f"{task_key}/{state}", file=self.output)


    def report_execution_times(self):

        if self.parsed_args.task_key is not None and self.parsed_args.filter == "*":
            f = self.parsed_args.task_key
        else:
            f = self.parsed_args.filter

        for task_key, timer_label, hms, s in timers_for_tasks(
            self.parsed_args.pipeline_instance_dir,
            f,
            include_steps=self.parsed_args.include_steps,
        ):
            print(f"{timer_label}\t{task_key}\t{hms}\t{s}", file=self.output)

    def grep_logs(self):
        for key, state, step, sf in self.filter_key_state_step():
            out_log = Path(self.parsed_args.pipeline_instance_dir, ".drypipe", key, "out.log").absolute()
            if out_log.exists():
                cmd = ["grep", "-F", self.parsed_args.grep_expr, str(out_log)]
                subprocess.check_call(cmd)


    def tail_logs(self, file=sys.stdout):
        n = self.parsed_args.n
        i = 1
        for key, state, step, sf in self.filter_key_state_step():
            out_log = Path(self.parsed_args.pipeline_instance_dir, ".drypipe", key, "out.log")
            if out_log.exists():
                print(f"============================= {i} step: {step}, key: {key} =====================================", file=file)
                i += 1
                cmd = ["tail", f"-{n}", str(out_log)]
                print(" ".join(cmd), file=file)
                file.flush()
                subprocess.check_call(cmd, stdout=file, stderr=file)
                file.flush()
                print("", file=file)                


    def complain_if_no_generator(self, msg):
        g = self.parsed_args.generator
        if g is None:
            raise Exception(f"--generator is required {msg}")

    def _tail_all(self):
        b = getattr(self.parsed_args, 'tail_all', False)
        return b is not None and b
    
    def _key(self, k=None):
        if k is not None:
            return k
        return self.parsed_args.task_key
    
    def reset(self):
        for key, state, step, sf in self.filter_key_state_step():
            self._maybe_reset(key, force=True)

    def _maybe_reset(self, key=None, force=False):

        if not force:
            if not self.parsed_args.reset:
                return
        
        k = self._key(key)
    
        dirz = [
            Path(self.parsed_args.pipeline_instance_dir, d, k).__str__()
            for d in ["output", ".drypipe"]
        ]                
        for d in dirz:
            if self.parsed_args.dry_run:
                print(f"DRY RUN rm -Rf {d}")
            else:
                shutil.rmtree(d, ignore_errors=True)

    def _maybe_regen_task(self, key=None):

        k = self._key(key)

        def do_regen():
            pipeline_instance = self.pipeline_instance_from_args()
            pipeline_instance.prepare_instance_dir()
            self._maybe_reset(k)

            pipeline_instance.regen_task(
                k,
                self.logger if self._tail_all() else None
            )

        if self.parsed_args.regen or self.parsed_args.reset:
            self.complain_if_no_generator("with --regen or --reset flags")
            do_regen()
        elif not Path(self._control_dir()).exists():
            self.complain_if_no_generator(
                'when the pipeline was never run, either specify --generator, or use "run" or "prepare"'
            )
            do_regen()

    def create_filter_chain(self):

        def tautology(key, step, state_name):
            return True

        def create_glob_filter():
            if self.parsed_args.filter == "*":
                return tautology

            def f(key, state_name, step):
                return fnmatch.fnmatch(key, self.parsed_args.filter)

            return f

        def create_py_filter():

            if self.parsed_args.filter_completed:
                return lambda key, state_name, step: state_name == 'completed'
            
            if self.parsed_args.filter_not_completed:
                return lambda key, state_name, step: state_name != 'completed'
            
            if self.parsed_args.filter_failed:
                return lambda key, state_name, step: state_name.startswith("failed")

            if self.parsed_args.py_filter is None:
                return tautology

            expr = self.parsed_args.py_filter

            def f(key, state_name, step):
                resolved_expr = expr.format(**{"step": step, "state_name": f"'{state_name}'", "key": f"'{key}'"})
                try:
                    return eval(resolved_expr)
                except SyntaxError:
                    print(f"--py-filter '{expr}' has a syntax error")
                    sys.exit(1)
            return f

        def create_func_filter():
            if self.parsed_args.func_filter is None:
                return tautology

            spec = self.parsed_args.func_filter
            reference, call_args, call_kwargs = _parse_func_filter_spec(spec)

            # special case: a bare function name (no module, i.e. no ":") is looked up in the --generator module
            if ":" not in reference:
                generator = self.parsed_args.generator
                if generator is None:
                    raise Exception(
                        f"--func-filter '{spec}' has no module, and can't be resolved against "
                        f"the --generator module because --generator is not set"
                    )
                generator_module = generator.split(":")[0]
                reference = f"{generator_module}:{reference}"

            factory = func_from_mod_func(reference)

            # the referenced function is a factory: it receives the --func-filter args and returns the filter
            try:
                inspect.signature(factory).bind(*call_args, **call_kwargs)
            except TypeError as e:
                raise Exception(f"--func-filter '{spec}': arguments don't match {reference}: {e}")

            filter_func = factory(*call_args, **call_kwargs)
            if not callable(filter_func):
                raise Exception(
                    f"--func-filter '{spec}': {reference} must return a filter function(key, state_name, step), "
                    f"got a {type(filter_func).__name__}"
                )

            def f(key, state_name, step):
                return filter_func(key, state_name, step)

            return f

        def g():
            yield create_glob_filter()
            yield create_py_filter()
            yield create_func_filter()

        return list(g())


    def filter_key_state_step(self, key_universe=None):
        pipeline_instance = self.pipeline_instance_from_args()
        pipeline_instance.prepare_instance_dir()

        filter_chain = self.create_filter_chain()

        def accept(key, state, step):
            for f in filter_chain:
                if not f(key, state, step):
                    return False
            return True

        for state_file in pipeline_instance.iterate_key_state_steps(key_universe):
            key, state, step = state_file.key_state_step()
            if accept(key, state, step):
                yield key, state, step, state_file

    def list(self):
        for key, state, step, sf in self.filter_key_state_step():
            s = "" if step is None else f"{step}"
            print(f"{key}\t{state}\t{step}")

    def has_filters(self):
        if self.parsed_args.py_filter is not None or self.parsed_args.filter != "*":
            return True
        if self.parsed_args.func_filter is not None:
            return True
        if self.parsed_args.filter_completed:
            return True
        
        if self.parsed_args.filter_not_completed:
            return True
        
        if self.parsed_args.filter_failed:
            return True
        
        return False


    def list_keys(self):
        for key, _, _, _ in self.filter_key_state_step():
            print(key, file=self.output)

    def run_from_slurm_job(self):
        task_process = TaskProcess(
            self._control_dir()
        )

        if task_process.is_array_child_task():
            task_process._delete_array_child_launch_log_if_empty()        

        task_process.launch_task()

    def run_from_slurm_packed_job(self):

        slurm_array_task_id = int(os.environ.get("SLURM_ARRAY_TASK_ID"))

        TaskProcess.delete_array_child_launch_log_if_empty()

        tasks_per_job = int(os.environ["DRYPIPE_TASKS_PER_JOB"])

        last_task = None

        idx = 0

        for packed_array_index in range(
            slurm_array_task_id * tasks_per_job, 
            slurm_array_task_id * tasks_per_job + tasks_per_job
        ):

            try:

                idx += 1

                task_process = TaskProcess(
                    self._control_dir(),
                    packed_array_index=packed_array_index,
                    tasks_per_job=tasks_per_job
                )

                last_task_msg = ""
                if last_task is not None:
                   last_task.task_logger.info(f"will execute next task in pack: .drypipe/{task_process.task_key}")                
                   last_task_msg = f", continued from .drypipe/{last_task.task_key}"

                try:
                    task_process.task_logger.info(f"packed task {idx} / {tasks_per_job}{last_task_msg}")
                    task_process.launch_task()
                    task_process.task_logger.info(f"packed task {task_process.packed_task_id()} ended")
                except Exception as ex:
                    task_process.task_logger.info(f"packed task {task_process.packed_task_id()} had unhandled exception")
                    task_process.task_logger.exception(ex)

                last_task = task_process                
            except TaskPackExchausted:                
                break

        # last task of the pack
        task_process.task_logger.info(f"job pack completed")


    def task(self):
        cli_tail_logger = None
        if self._tail():
            cli_tail_logger = self.logger

        c = Path(self._control_dir())
        if not c.exists():
            self.parsed_args.regen = True
            c.mkdir(parents=True, exist_ok=True)


        self._maybe_regen_task()

        task_process = TaskProcess(
            self._control_dir(),
            wait_for_completion=self._wait() or self._tail(),
            test_mode=self.test_mode,
            as_subprocess=not self.test_mode,
            tail=self._tail(),
            tail_all=self.parsed_args.tail_all,
            cli_tail_logger=cli_tail_logger,
            for_dry_run=self.parsed_args.dry_run
        )

        if self.parsed_args.reset:
            if Path(task_process.task_output_dir).exists():
                shutil.rmtree(task_process.task_output_dir)
            task_process.rewind_to_step(0)

        if self.parsed_args.at_step is not None:
            task_process.rewind_to_step(self.parsed_args.at_step)


        if self.parsed_args.ssh_remote_dest is not None:
            task_process.task_conf.ssh_remote_dest = self.parsed_args.ssh_remote_dest
        elif task_process.task_conf.executer_type == "slurm":
            if task_process.is_remote_execution_on_master_site():
                task_process.launch_task()
                return

            if self.parsed_args.by_runner and not task_process.task_conf.is_slurm_parent:
                task_process.submit_sbatch_task(instance_logger=self.instance_logger)
                return

        task_process.launch_task()

    def restart(self):

        task_process = TaskProcess(
            self._control_dir(),
            wait_for_completion=self._wait() or self._tail(),
            use_remote_drypipe_log=self.parsed_args.from_remote,
            tail=self._tail()
        )

        task_process.reset_restart_accounting()

        step_number, control_dir, state_file, state_name = task_process.read_task_state()

        if self.parsed_args.reset:
            shutil.rmtree(task_process.task_output_dir)
            task_process.rewind_to_step(0)

        if self.parsed_args.at_step is not None:
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


    def rewind(self):
        for key, state, step, state_file in self.filter_key_state_step():
            state_file.rewind_to_step(self.parsed_args.at_step)            

    def set_state(self):

        if "." in self.parsed_args.state:
            state, step = self.parsed_args.state.split(".")
            step = int(step)
        else:
            state = self.parsed_args.state
            step = None

        for key, _, _, state_file in self.filter_key_state_step():
            state_file.transition_to_state(state, step)

    def instance_info(self):
        pid = os.environ.get("DRYPIPE_PIPELINE_INSTANCE_DIR")
        print(f"DRYPIPE_PIPELINE_INSTANCE_DIR={pid}")
        gen = os.environ.get("DRYPIPE_PIPELINE_GENERATOR")

        def dump_gen():
            print(f"DRYPIPE_PIPELINE_GENERATOR={gen}")

        conf = Path(pid, ".drypipe", "conf.json")
        if conf.exists():
            with open(conf, "r") as f:
                conf = json.load(f)
                gen = conf.get("__generator")
                dump_gen()
                __pipeline_code_dir = conf.get("__pipeline_code_dir")
                print(f"$__pipeline_code_dir={__pipeline_code_dir}")

    def status(self):
        for key, state, step, _ in self.filter_key_state_step():
            print(f"{key}\t{state}\t{step}")
            

    def summary(self):

        all = [
            (state, step, key)
            for key, state, step, _ in self.filter_key_state_step()
        ]                     

        def k(t):
            return t[0], t[1]
    
        for state_step, tuples in groupby(sorted(all, key=k), key=k):
            state, step = state_step
            cnt = len(list(tuples))
            step = "" if step is None else step
            print(f"{state}\t{step}\t{cnt}")

def run_cli():
    handle_script_lib_main()

def setup_file_creation_mask():
    os.umask(0o007)

def handle_script_lib_main():
    try:
        setup_file_creation_mask()
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


def cli_argument_parser():
    cli = Cli([])
    return cli.parser

if __name__ == '__main__':

    if "SLURM_JOB_ID" in os.environ:
        setup_file_creation_mask()
        call(sys.argv[2])
    else:
        handle_script_lib_main()
