import argparse
import collections
import ast
import ctypes
import ctypes.util
import fnmatch
import grp
import inspect
import itertools
from itertools import groupby
import shutil
import signal
import subprocess
import tempfile
import time
import json
import logging
import logging.config
import os
import re
import shlex
import sys
import textwrap
import traceback

from io import StringIO
from os import environ
from pathlib import Path

from dry_pipe import PortablePopen, DryPipe
from dry_pipe.core_lib import func_from_mod_func, is_inside_slurm_job, create_instance_logger, read_last_lines
from dry_pipe.frozen_dag_generator import FrozenDAGGenerator
from dry_pipe.task_classifier import TaskClassifier, TaskClassifyContext
from dry_pipe.pipeline_instance import Monitor, PipelineInstance
from dry_pipe.task_process import TaskPackExchausted, TaskProcess, TaskFailedException
from dry_pipe.slurm_array_task import SlurmArrayParentTask
from dry_pipe.slurm_codes import SlurmJobStateCodes
from dry_pipe.reports import timers_for_tasks
from dry_pipe.state_machine import StateFileTracker
from dry_pipe.service import PipelineRunner
from dry_pipe.task_lib import submit_local_array, upload_task_inputs_rsync


logger = logging.getLogger(__name__)

# squeue can block for a long time when slurmctld is backed up, don't let array-summary hang on it
SQUEUE_TIMEOUT_SECS = 30

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
        traceback.print_exc(file=sys.stdout)
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


def _load_task_keys(file):
    """one task key per line, first column of a tsv, other columns ignored, keys can't contain spaces"""

    def key(line_number, line):
        k = line.split("\t")[0].strip()
        if re.search(r"\s", k):
            raise Exception(f"{file}:{line_number}: task keys can't contain spaces, got '{k}'")
        return k

    with open(file) as f:
        return {key(line_number, line) for line_number, line in enumerate(f, start=1) if line.strip() != ""}


class TaskSet:
    """the task keys allowed by the rules of a --task-set file"""

    def __init__(self, rules_file):
        rules_dir = Path(rules_file).resolve().parent

        def parse_rule(line_number, line):
            rule = re.fullmatch(r"([+-])\s+(\S+)", line)
            if rule is None:
                raise Exception(
                    f"{rules_file}:{line_number}: expected '+ <glob>', '- <glob>', '+ @<file>' or '- @<file>', got '{line}'"
                )
            sign, pattern = rule.groups()
            if not pattern.startswith("@"):
                return sign == "+", lambda key: fnmatch.fnmatch(key, pattern), frozenset()
            keys_file = rules_dir / pattern[1:]
            if not keys_file.exists():
                raise Exception(f"{rules_file}:{line_number}: {keys_file} does not exist")
            keys = frozenset(_load_task_keys(keys_file))
            return sign == "+", keys.__contains__, keys

        with open(rules_file) as f:
            lines = [(line_number, line.split("#")[0].strip()) for line_number, line in enumerate(f, start=1)]

        self.rules = [parse_rule(line_number, line) for line_number, line in lines if line != ""]
        self.keys_of_files = frozenset(k for _, _, keys in self.rules for k in keys)

    def __contains__(self, key):
        """the last matching rule wins, keys matched by no rule are in the task set"""
        return next((is_added for is_added, matches, _ in reversed(self.rules) if matches(key)), True)


def _signatures_of_analysis_file(path, lines):
    """
    (task count, signature cell) of each row of the error signatures table of an analyze-logs file,
    in signature number order, raises when the table is malformed
    """
    separator_of_header = {
        "| # | tasks | signature |": "|---:|---:|---|",
        "| # | tasks | signature | keys |": "|---:|---:|---|---|",
    }

    header_index = next((i for i, line in enumerate(lines) if line in separator_of_header), None)
    if header_index is None:
        raise Exception(f"{path}: not an analyze-logs file, it has no error signatures table")

    separator = separator_of_header[lines[header_index]]
    if lines[header_index + 1:header_index + 2] != [separator]:
        raise Exception(f"{path}:{header_index + 2}: not an analyze-logs file, expected {separator}")

    rows = itertools.takewhile(lambda l: l.startswith("|"), lines[header_index + 2:])
    for number, row in enumerate(rows, start=1):
        cells = re.fullmatch(rf"\| {number} \| (\d+) \| (`.*`) \|(?: [^|]* \|)?", row)
        if cells is None:
            raise Exception(
                f"{path}:{header_index + 2 + number}: not an analyze-logs file, "
                f"expected | {number} | <task count> | `<signature>` |"
            )
        yield int(cells.group(1)), cells.group(2)


def _keys_and_signature_numbers_of_analysis_file(path, lines):
    """
    from the "### i. <key> (signature <n>)" headings of an analyze-logs file, skipping the fenced tails,
    raises when a heading is malformed
    """
    fence = None
    for line_number, line in enumerate(lines, start=1):
        if fence is not None:
            if line == fence:
                fence = None
        elif line.startswith("```"):
            fence = re.match(r"`+", line).group()
        elif line.startswith("### "):
            heading = re.fullmatch(r"### \d+\. (\S+) \(signature (\d+)\)", line)
            if heading is None:
                raise Exception(
                    f"{path}:{line_number}: not an analyze-logs file, expected ### <i>. <task-key> (signature <n>)"
                )
            yield heading.group(1), int(heading.group(2))


def _signature_numbers(text, signature_count):
    """parses "1,3" into {1, 3}, the numbers must be in 1..signature_count"""
    try:
        numbers = {int(n) for n in text.split(",")}
    except ValueError:
        raise Exception(f"expected comma separated signature numbers, got '{text.strip()}'")
    invalid = sorted(n for n in numbers if not 1 <= n <= signature_count)
    if invalid:
        raise Exception(f"no signature numbered {invalid}, valid numbers are 1 to {signature_count}")
    return numbers


def _load_filter_from_keys(spec):
    """
    an analyze-logs file: "failed.2-5.md" for all its tasks, "failed.2-5.md:1,3" for the tasks of signatures 1 and 3,
    else a file of keys (see _load_task_keys)
    """
    analysis_file = re.fullmatch(r"(.+\.md)(?::(.*))?", spec)
    if analysis_file is None:
        return _load_task_keys(spec)

    path, numbers_text = analysis_file.groups()
    lines = Path(path).read_text().splitlines()
    signatures = list(_signatures_of_analysis_file(path, lines))
    keys_and_numbers = _keys_and_signature_numbers_of_analysis_file(path, lines)
    if numbers_text is None:
        return {key for key, _ in keys_and_numbers}

    numbers = _signature_numbers(numbers_text, len(signatures))
    return {key for key, number in keys_and_numbers if number in numbers}


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

    def __init__(self, args, env=None, test_mode=False, output=sys.stdout, logger=None, parse_args=True):

        self.raw_command_line = " ".join(args)
        self.args = _cleanup_args(args)
        self.output = output

        self._has_implicit_generator = False
        self._has_implicit_control_dir = False
        self._has_implicit_pid = False
        self._has_implicit_task_key = False

        self.task_process = None 
        self.array_task_manager = None

        self.test_mode = test_mode        
        self.logger = logger

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
            m = getattr(self, method_name, None)
            if m is not None:
                self.command_names_to_method[command.name] = m

        if not parse_args:
            return

        self.parse_args()
        
        if is_inside_slurm_job():
            # this process is a slurm job (or a step of one) that the CLI itself submitted;
            # its logging belongs in the task's own drypipe.log, not instance.log:
            # instance.log should read like what --verbose would show a user, and concurrent
            # slurm jobs writing to one shared instance.log is a concurrency hazard.
            self.instance_logger = None
        else:
            pid = getattr(self.parsed_args, 'pipeline_instance_dir', None)
            # the instance logger creates .drypipe, status-db must report it as missing
            if pid and self.parsed_args.command == "status-db" and not Path(pid, ".drypipe").exists():
                self.instance_logger = None
            elif pid:
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
            raise Exception(f"command {self.parsed_args.command} is not yet implemented")

        method()


    def invoke_in_subprocess(self):
        return cli_in_sub_process(self.args)

    def _enumerate_commands(self):

        class ListOfFloats:
            def __call__(self, txt):
                return [float(s) for s in txt.split(",")]



        def pipeline_instance_dir(parser):
            return parser.add_argument(
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
            parser.add_argument('--task-key', '-k', required=True, help="task key",
                action=EnvDefault,
                envvar="DRYPIPE_TASK_KEY",
                env=self.env
            )

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
                help="a list of space separated options that will be appended directly to the sbatch command",
                required=False,
                action=EnvDefault,
                envvar="DRYPIPE_SBATCH_OPTIONS",
                env=self.env
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

                  1. key       : --filter / --key-filter   (glob on the task key), --filter-from (file of keys)
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

        def filter_from(parser):
            parser.add_argument(
                '--filter-from',
                help="file of task keys, one per line (first column of a tsv), ex: written by extract-keys. "
                     "Or a markdown file written by analyze-logs: --filter-from=failed.2-5.md selects all its tasks, "
                     "--filter-from=failed.2-5.md:1,3 the tasks of its signatures 1 and 3",
                default=None
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
        def filter_timed_out(parser):
            parser.add_argument(
                '--filter-timed-out',
                help='filters all timed out tasks',
                action='store_true',
                default=False
            )            

        def filter_ready(parser):
            parser.add_argument(
                '--filter-ready',
                help='filters all ready tasks',
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

        def filter_unhealthy(parser):
            parser.add_argument(
                '--filter-unhealthy',
                help='filters tasks that ended without completing: failed, timed-out, killed or crashed',
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


            parser.add_argument(
                '--task-set',
                help="""A rules file defining the task set: the tasks yielded by the generator that make up the pipeline instance.
                Drypipe treats tasks not in the task set (ignored tasks) as if they were never emitted by the DAG generator.
                Rules are applied in order, starting from all the tasks yielded by the generator, one rule per line:
                    - <glob>    removes the tasks with a key matching the glob
                    + <glob>    adds them back
                    - @<file>   removes the tasks with a key in a tsv file (first col is task key, remaining cols ignored),
                                a relative path is relative to the directory of the rules file
                    + @<file>   adds them back
                '#' starts a comment. Ex: all tasks except those in forget.tsv, unless they are in tests.tsv:
                    - @forget.tsv
                    + @tests.tsv
                Note1: commands garbage-collect and purge-tasks-not-in-task-set purge ignored tasks if invoked
                Note2: a file named $PIPELINE_INSTANCE_DIR/drypipe-task-set.rules is treated as if referred by --task-set. If --task-set is defined and $PIPELINE_INSTANCE_DIR/drypipe-task-set.rules also exists, --task-set wins
                """,
                action=EnvDefault,
                envvar="DRYPIPE_TASK_SET",
                env=self.env,
                required=False
            )
            
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

        def fs_generator(parser):
            parser.add_argument(
                '--fs-generator', '--fs-gen',
                action='store_true',
                default=False,
                help="read the tasks from the pipeline instance's .drypipe/ dir instead of running --generator: "
                     "no DAG code runs, only tasks already created (by run or prepare) are seen. "
                     "Functions given to options like --func-filter must be in module:function format"
            )

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
                sub_parser = _s.add_parser(name, help=kwargs.get("help"), description=kwargs.get("help"))
                for a in args:
                    try:
                        a(sub_parser)
                    except Exception as e:
                        raise Exception(f"arg {a.__name__}  on command {name} failed with exception {e}")

        def all_filters():
            return [filter, filter_from, py_filter, func_filter, filter_completed, filter_not_completed, filter_failed, filter_unhealthy, filter_timed_out, filter_ready]

        # argparse dests of the filter options, their functions are named after them
        self.filter_dests = [f.__name__ for f in all_filters()]

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

        yield Command('prepare', pipeline_instance_dir, generator, until, sleep_schedule, *all_filters(),
                      help="generate tasks, WITHOUT running the pipeline")

        yield Command('service', pipeline_instances_dir, config_generator, sleep_schedule, log_conf, exit_on_parent_death,
                      help="run as service")

        yield Command('upgrade-drypipe', pipeline_instance_dir,
                      help="upgrade drypipe version for the specified pipeline instance")

        def tsv(parser):
            parser.add_argument(
                "--tsv",
                help='create tsv files that can be inserted in a db, instead of creating sqlite files',
                action='store_true',
                default=False
            )

        def instance_name(parser):
            parser.add_argument(
                "--instance-name",
                help='replaces the name of pipeline_instance_dir in the columns with specified name',
                type=str,
                default=None
            )

        def lean(parser):
            parser.add_argument(
                "--lean",
                help='produces smaller database (or tsv) by storing out.log and drypipe.log of only non completed tasks, the last step of out.log '
                     '(its banner and at most its last 1000 lines), and of drypipe.log (at most its last 100 lines). '
                     'WARNING: the logs of completed tasks are not read, the classifier (see --log-classifier) sees them as missing, '
                     'the default classifier gives them the signature "<no out.log>". '
                     'A --log-classifier can change what is read and stored, see reads_logs, extract_out_log and '
                     'extract_drypipe_log in dry_pipe/task_classifier.py',
                action='store_true',
                default=False
            )


        def log_classifier(parser):
            parser.add_argument(
                '--log-classifier',
                type=str,
                action=EnvDefault,
                envvar="DRYPIPE_LOG_CLASSIFIER",
                env=self.env,
                required=False,
                default=None,
                help='''
                customizes the error signatures, by default the universal classifier is used.
                The value is a function in "module:function" format, or a bare function name looked up in the
                --generator module. The function receives the default TaskClassifier and returns a classifier:
                the default with its regex lists augmented, or a replacement, ex:
                "def my_classifier(c): c.error_words.append(r'stopped unexpectedly'); return c"
                A signature of None leaves the task out of analyze-logs, and is null in status-db.
                Can also be set with environment var DRYPIPE_LOG_CLASSIFIER.
                '''
            )

        def pipeline_instance_dir_optional(parser):
            arg = pipeline_instance_dir(parser)
            arg.required = False

        def empty_db(parser):
            parser.add_argument(
                '--empty-db',
                help='only create an sqlite db with the status-db schema at the specified path, ex: to aggregate --tsv files',
                metavar="PATH",
                default=None
            )

        yield Command(
            'status-db', pipeline_instance_dir_optional, generator_optional, tsv, instance_name, empty_db, lean, log_classifier, *all_filters(),
            help="""
                creates an sqlite3 database with tables : 

                create table task_status (
                    instance_name text,
                    key text,
                    state text,
                    step int,
                    drypipe_log text,
                    out_log text,
                    log_signature text
                );

                create table instance_status (
                    instance_name text,
                    state text,
                    error text
                );

                task_status contains a row for each task (accepted by the filter if filters are specified),
                instance_name is the folder name of --pid OR value given by argument --instance-name

                (key, state, step) is what would be returned by the "status" command.

                log_signature is the error signature of out.log, as computed by analyze-logs, see --log-classifier

                The instance_status table has a single row

                if command completed without problems : 
                    instance_status.state = 'ok'

                if --pid has no .drypipe folder :
                
                instance_status.state = 'missing .drypipe'

                else : 
                
                instance_status.state = 'digest failed'
                instance_status.error = the stack dump of the fail


                if --tsv is specified, two tsv files (in the .drypipe folder) are created instead of an sqlite3 database:
                task_status.tsv instance_status.tsv
                The files are formatted such that if they can be inserted (via sqlite3 tsv bulk insertion) in the tables.
                
                The --tsv option allows aggregating multiple pipeline instances in a single sqlite database.
            """
        )

        #yield Command('restart-failed-array-tasks', task_key, include_pre_launch, wait,
        #              help="restart failed array tasks, of specified array task")

        def include_steps(parser):
            parser.add_argument(
                '--include-steps',
                help='include time for all steps',
                action='store_true',
                default=False
            )

        yield Command('times', task_key_optional, include_steps, generator_optional, *all_filters(),
                      help="execute time for all tasks, or all tasks matching filter expression")
        
        def stop_after_step(parser):
            parser.add_argument(
                "--stop-after-step",
                action=EnvDefault,
                envvar="DRYPIPE_STOP_AFTER_STEP",
                type=int,
                default=None,
                env=self.env,
                required=False,
                metavar='N',
                help="stops the task after step, the task state will be at ready.S (where S = N + 1)",
            )

        yield Command('run-from-slurm-job', task_key, stop_after_step)

        yield Command('run-from-slurm-packed-job', task_key, stop_after_step)

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

        yield Command('sbatch', task_key_optional, wait, regen, generator_optional, sbatch_options, reset, at_step, stop_after_step, *all_filters(),
                      help="launch task (specified by --task-key, or by combination of --filter --py-filter) with sbatch")

        yield Command('sbatch-gen', task_key, regen, generator_optional, sbatch_options, reset, stop_after_step,
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
                help='adds %%N at the end of the sbatch array spec, ex: --array-2000%%N',
                default=None
            )


        def yes(parser):
            parser.add_argument(
                '--yes', '-y',
                action='store_true',
                default=False,
                help="submit without asking for confirmation, for scripts"
            )

        yield Command('array-submit',
                      task_key, limit, regen, generator_optional, tail, wait, include_all_incompleted_tasks,
                      sbatch_options, reset, tasks_per_job, slurm_max_jobs, stop_after_step, yes, *all_filters(),
                      help="submit array, after a confirmation (see --yes)")

        yield Command('array-upload', task_key, help="upload array task to remote location")
        yield Command('array-download', task_key, help="download all array tasks results (rsync or Globus fetch all __task_output_dir of child tasks)")
        yield Command("array-submit-from-remote", task_key, wait, help="submit array task to remote location")
        yield Command("array-watch-from-remote", task_key, wait, help="watch (poll and fetch children status) of array task at remote location")
        yield Command('array-create-parent', task_key, help="create a parent array task with matching tasks")
        yield Command('list-states', task_key, gen_rsync_list)
        yield Command('array-rsync-list', task_key)

        yield Command('array-squeue', task_key,
                      help="dump squeue's output for each job id submitted by an array task")

        yield Command('array-summary', task_key, generator_optional, *all_filters(),
                      help="running and queued task counts of each submitted array job, followed "
                           "by summary (state and step counts) of the array task's children")

        yield Command('reset', task_key_optional, generator, *all_filters(),)

        yield Command( 'grep-logs', pipeline_instance_dir, grep_expr, generator, *all_filters(), help="applies fgrep on out.log files of matching tasks")

        def tail_n(parser):
            parser.add_argument(
                '--n', '-n',
                help="number of lines for tail n",
                type=int,
                default=50,
            )

        yield Command('tail-logs', tail_n, generator, pipeline_instance_dir, *all_filters(),
                      help="applies tail on out.log files of matching tasks")

        def analyze_logs_dir(parser):
            parser.add_argument(
                '--dir',
                type=str,
                default=os.getcwd(),
                help="destination directory of the markdown files, created if missing, defaults to $PWD"
            )

        def full(parser):
            parser.add_argument(
                '--full',
                action='store_true',
                default=False,
                help="add a keys column to the error signatures table, listing all task keys of each signature"
            )

        def all_tasks(parser):
            parser.add_argument(
                '--all-tasks',
                action='store_true',
                default=False,
                help="by default, only tasks that ended without completing are analyzed, as if --filter-unhealthy "
                     "was given (failed, timed-out, killed, crashed). --all-tasks analyzes all tasks that match the filters"
            )

        yield Command('analyze-logs', tail_n, generator_optional, fs_generator, pipeline_instance_dir, analyze_logs_dir, log_classifier, full, all_tasks, *all_filters(),
                      help="writes one markdown file per (state, step) of matching tasks, named <state>.<step>-<N>.md "
                           "where N is the number of tasks in the file (ex: failed.3-12.md), <state>-<N>.md "
                           "when the task has no step. Only tasks that ended without completing are analyzed, as if "
                           "--filter-unhealthy was given, other filters narrow the selection further, see --all-tasks. Each file starts with a table that groups its tasks by error "
                           "signature, followed by the tail of each task's out.log. The signature of a task is its last "
                           "error line in the last step of out.log, with variable parts (paths, numbers, quoted strings, etc) masked, "
                           "see dry_pipe/task_classifier.py and --log-classifier")

        def analysis_file(parser):
            parser.add_argument('analysis_file', help="a markdown file written by analyze-logs")

        def dest(parser):
            parser.add_argument(
                '--dest',
                type=str,
                default=None,
                help="file where the keys are written, one per line, instead of stdout"
            )

        yield Command('extract-keys', analysis_file, dest,
                      help="lists the error signatures of a file written by analyze-logs, asks which ones to select "
                           "(ex: 1,3), and outputs the keys of the tasks having a selected signature. The keys can be "
                           "used with --filter-from, ex: array-submit --filter-from=keys.txt")

        def tags(parser):
            parser.add_argument(
                '--tags',
                help="comma separated list of tags, outputs having at least one of them are selected, all outputs if omitted",
                type=str
            )

        def exclude_tags(parser):
            parser.add_argument(
                '--exclude-tags',
                help="comma separated list of tags, outputs having none of them are selected (untagged outputs included), "
                     "cannot be combined with --tags",
                type=str
            )

        """        def all_output_dir(parser):
                    parser.add_argument(
                        '--all-outputs',
                        help='include all files in task output dirs, not just the ones declared in task output clauses',
                        action='store_true',
                        default=False
                    )
        """
        def dest(parser):
            parser.add_argument(
                '--dest',
                help="rsync destination, ex: my-host:/a/b/my-pipeline/",
                required=True
            )

        def include_drypipe_files(parser):
            parser.add_argument(
                '--include-drypipe-files',
                help="adds drypipe working files to the selected files: (all files in .drypipe/<task-key>/*). If given 'minimal' as value, will only include out.log, drypipe.log and the tasks state files (default: %(default)s)",
                nargs='?',
                const='all',
                choices=['all', 'minimal', 'none'],
                default='none'
            )
        

        def no_confirm(parser):
            parser.add_argument(
                '--no-confirm',
                help="don't prompt for confirmation after printing the summary",
                action='store_true',
                default=False
            )

        def info_progress2(parser):
            parser.add_argument(
                '--info-progress2',
                help="rsync reports overall progress (rsync --info=progress2) instead of end of transfer statistics (rsync --stats)",
                action='store_true',
                default=False
            )

        def exhaustive(parser):
            parser.add_argument(
                '--exhaustive',
                action='store_true',
                default=False,
                help="select everything of matching tasks: all files in output/<task-key>/ (declared outputs or not) "
                     "and in .drypipe/<task-key>/, cannot be combined with --tags, --exclude-tags or --include-drypipe-files"
            )

        yield Command('rsync-push', pipeline_instance_dir, generator, tags, dest, include_drypipe_files, exhaustive, no_confirm, info_progress2, *all_filters(),
                      help="rsync output files having at least one of the --tags, of matching tasks, to --dest")

        def source(parser):
            parser.add_argument(
                '--source',
                help="rsync source, ex: my-host:/a/b/my-pipeline/",
                required=True
            )

        def state_files_only(parser):
            parser.add_argument(
                '--state-files-only',
                action='store_true',
                default=False,
                help="only pull the state files of matching tasks, local state files are renamed according to remote ones, "
                     "cannot be combined with --tags, --include-drypipe-files or --exhaustive"
            )

        yield Command('rsync-pull', pipeline_instance_dir, generator, tags, source, include_drypipe_files, exhaustive, state_files_only, no_confirm, info_progress2, *all_filters(),
                      help="rsync output files having at least one of the --tags, of matching tasks, from --source"
                      " note: unless --include-drypipe-files or --state-files-only option, only output (task produces) files get rsynced."
                )

        def archive_name(parser):
            parser.add_argument(
                '--name',
                type=str,
                help="archive file name, used as given (no extension is appended), relative to $PWD if not absolute",
                required=True
            )

        def archive_command(parser):
            parser.add_argument(
                '--archive-command',
                type=str,
                default="tar -czf {archive} .",
                help="command that creates the archive, run inside the staging directory, "
                     "{archive} is replaced by the absolute path of --name, "
                     "ex: 'tar --zstd -cf {archive} .', 'zip -qr {archive} .' (default: '%(default)s')"
            )

        yield Command('archive', pipeline_instance_dir, generator, tags, exclude_tags, include_drypipe_files, exhaustive,
                      archive_name, archive_command, no_confirm, *all_filters(),
                      help="archive output files having at least one of the --tags (or none of the --exclude-tags), of matching tasks, with --archive-command")

        yield Command('purge-outputs', pipeline_instance_dir, generator, tags, exclude_tags, no_confirm, *all_filters(),
                      help="deletes output files having at least one of the --tags (or none of the --exclude-tags), of matching tasks, warning: purging files can break some pipelines")


        yield Command('garbage-collect', pipeline_instance_dir, generator, no_confirm,
                      help="deletes directories K (in ./output/<K> and .drypipe/<K> where K is not in the set of task keys yielded by the generator")

        yield Command('purge-tasks-not-in-task-set', pipeline_instance_dir, generator, no_confirm,
                      help="deletes directories K (in ./output/<K> and .drypipe/<K> where K is a task key yielded by the generator and not in the task set, see --task-set")


        def module_function(parser):
            parser.add_argument('module_function', type=str)

        yield Command('call', module_function, task_key)


    def _task_set_file(self):
        explicit_file = self.parsed_args.task_set
        if explicit_file is not None:
            if not os.path.exists(explicit_file):
                raise Exception(f"--task-set file {explicit_file} does not exist")
            return explicit_file
        implicit_file = Path(self.parsed_args.pipeline_instance_dir, "drypipe-task-set.rules")
        if implicit_file.is_symlink() and not implicit_file.exists():
            raise Exception(f"{implicit_file} is a broken symlink")
        if implicit_file.exists():
            return implicit_file
        return None

    def _load_task_set(self):
        self.task_set_file = self._task_set_file()
        self.task_set = None if self.task_set_file is None else TaskSet(self.task_set_file)

    def _is_ignored(self, key):
        return self.task_set is not None and key not in self.task_set

    def _uses_fs_generator(self):
        return getattr(self.parsed_args, "fs_generator", False)

    def frozen_dag_generator_from_args(self):
        self._load_task_set()
        return FrozenDAGGenerator(self.parsed_args.pipeline_instance_dir, self._is_ignored)

    def pipeline_instance_from_args(self):

        generator_mod_func = self.parsed_args.generator
        if generator_mod_func is None:
            if hasattr(self.parsed_args, "fs_generator"):
                raise Exception(f"--generator or --fs-generator is required")
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

        self._load_task_set()
        if self.task_set is not None:
            # for reporting, ex: rsync summary
            self.yielded_task_keys = set()
            self.yielded_ignored_task_keys = set()
            task_generator = pipeline.task_generator

            def without_ignored_array_children(task):
                if task.is_slurm_parent:
                    children_tasks = task.inputs.children_tasks
                    children_tasks.value = [t for t in children_tasks.value if not self._is_ignored(t.key)]
                    if len(children_tasks.value) == 0:
                        raise Exception(
                            f"all children tasks of slurm array parent task {task.key} are ignored " +
                            f"by {self.task_set_file}, ignore {task.key} as well"
                        )
                return task

            def task_generator_without_ignored_tasks(dsl):
                for task in task_generator(dsl):
                    self.yielded_task_keys.add(task.key)
                    if self._is_ignored(task.key):
                        self.yielded_ignored_task_keys.add(task.key)
                    else:
                        yield without_ignored_array_children(task)

            pipeline.task_generator = task_generator_without_ignored_tasks

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

    def sbatch_options_overrider_func_if_option_exists(self):

        def func(original_options):

            def options_to_dict(options):
                def g():
                    for o in options:
                        k, v = o.split("=")
                        yield k.strip(), v.strip()

                return dict(g())
                
            d1 = options_to_dict(original_options)
            d2 = options_to_dict(self.parsed_args.sbatch_options.strip().split(" "))

            res = {** d1, ** d2}

            return [
                f"{k}={v}"
                for k, v in res.items()
            ]
        
        if self.parsed_args.sbatch_options is None:
            return None
        else:
            return func
                

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

        self.task_process = TaskProcess(
            self._control_dir(),
            wait_for_completion=self._wait() or self._tail(),
            test_mode=self.test_mode,
            as_subprocess=not self.test_mode,
            tail=self._tail(),
            tail_all=self.parsed_args.tail_all,
            cli_tail_logger=cli_tail_logger,
            for_dry_run=self.parsed_args.dry_run,
            tasks_per_job=self.parsed_args.tasks_per_job,
            stop_after_step=self.parsed_args.stop_after_step
        )        

        if not self.task_process.is_slurm_array_parent():
            raise Exception(f"task {self.parsed_args.task_key} is not a slurm array")

        self.array_task_manager = self.task_process.create_array_task_manager(
            self.parsed_args.slurm_max_jobs, instance_logger=self.instance_logger
        )

        if not self.has_filters():
            self.array_task_manager.invoke_sacct()
            is_restart = len(self.array_task_manager.arrays_submitted_sacct_info) > 0
        else:
            is_restart = False

        def reset_restart_counts():
            # task_process.rewind_to_step(0)
            self.task_process.task_logger.info(f"submit_local_array is a restart")
            if not self.task_process.for_dry_run:
                for restart_file in Path(self.task_process.pipeline_work_dir).glob("*/restarts.tsv"):
                    with open(restart_file, "a") as f:
                        f.write("RESET\n")
            else:
                self.task_process.task_logger.info(f"no file changed, because it's a dry_run")

        launch_count = 0

        def set_of_task_keys_if_has_filter():
            if not self.has_filters():
                return None

            def g():
                for key, _, _, _ in self.filter_key_state_step(self.array_task_manager.children_task_keys()):
                    yield key

            return set(g())
        
        set_of_task_keys = set_of_task_keys_if_has_filter()


        submits = self.array_task_manager.next_submits(
            restart_failed=is_restart,
            include_all_incompleted=self.parsed_args.include_all_incompleted_tasks,
            set_of_task_keys=set_of_task_keys,
            sbatch_option_overrider=self.sbatch_options_overrider_func_if_option_exists()
        )

        def is_confirmed():
            """each submit writes its task keys in an array.<n>.tsv file, and submits it as one slurm array"""
            for submit in submits:
                print(f"Will submit slurm array with {len(submit.task_keys)} tasks, sbatch options:", file=sys.stderr)
                for o in submit.sbatch_options:
                    print(f"  {o}", file=sys.stderr)
            print("submit ? [y/N] ", end="", file=sys.stderr, flush=True)
            return sys.stdin.readline().strip().lower() in ("y", "yes")

        if len(submits) == 0:
            print("warning: no tasks to submit, the array is empty", file=sys.stderr)
            return

        needs_confirmation = not self.parsed_args.yes and not self.parsed_args.dry_run
        if needs_confirmation and not is_confirmed():
            print("nothing submitted", file=sys.stderr)
            return

        if is_restart:
            reset_restart_counts()

        for submit in submits:
            submit.invoke()
            launch_count += len(submit.task_keys)
        

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

    def sbatch(self):

        def submit_one(key):

            self._maybe_regen_task(key)
            
            control_dir = Path(self.parsed_args.pipeline_instance_dir, ".drypipe", key).__str__()

            self.task_process = TaskProcess(
                control_dir, wait_for_completion=self._wait(), no_logger=True,
                stop_after_step=self.parsed_args.stop_after_step
            )

            if self.parsed_args.at_step is not None:
                self.task_process.rewind_to_step(self.parsed_args.at_step)
            if self.parsed_args.reset:
                self.task_process.rewind_to_step(0)

            self.task_process.submit_sbatch_task(
                self.sbatch_options_overrider_func_if_option_exists(),
                instance_logger=self.instance_logger
            )

        if self.has_filters():
            for key, _, _, _ in self.filter_key_state_step():
                print(f"will submit {key}")
                submit_one(key)
        else:
            submit_one(self.parsed_args.task_key)


    def sbatch_gen(self):
        self._maybe_regen_task()
        self.task_process = TaskProcess(
            self._control_dir(), stop_after_step=self.parsed_args.stop_after_step
        )
        print(" ".join(self.task_process.sbatch_cmd_lines(self.sbatch_options_overrider_func_if_option_exists())), file=self.output)

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

    def _array_manage(self):
        task_process = TaskProcess(self._control_dir(), no_logger=True)

        if not task_process.is_slurm_array_parent():
            raise Exception(f"task {self.parsed_args.task_key} is not a slurm array")

        return task_process.create_array_task_manager(instance_logger=self.instance_logger)        

    def _has_squeue(self):
        """
        squeue is absent when the instance is inspected from a machine that is not a slurm
        submit host, which is not fatal for the commands that report on arrays
        """
        if shutil.which("squeue") is not None:
            return True
        print("squeue is not installed on this machine", file=self.output)
        return False

    def array_squeue(self):
        m = self._array_manage()        
        self._array_squeue(m)

    def _array_squeue(self, array_task_manager):
        """
        a line per submitted array job (array.<n>), with how many of its tasks are in each
        slurm state, out of the total it was submitted with
        """        

        # the counted columns, in print order, the other states slurm can report for a task
        # (completing, cancelled, suspended, ...) have no column of their own
        counted_codes = [
            SlurmJobStateCodes.PENDING,
            SlurmJobStateCodes.RUNNING,
            SlurmJobStateCodes.FAILED,
            SlurmJobStateCodes.COMPLETED,
            SlurmJobStateCodes.TIMEOUT
        ]

        def task_count_in_array_file(array_n):
            # the total is the number of tasks the array was submitted with, NOT the number of
            # rows squeue returns: squeue only reports what is still in the queue
            array_file = array_task_manager.array_files_sequence.file_name(array_n)
            with open(array_file) as f:
                return len([line for line in f if line.strip() != ""])

        # without squeue there is no slurm view to report, but the state counts below still stand
        submitted_arrays = list(array_task_manager.submitted_arrays_files()) if self._has_squeue() else []

        def rows():

            if len(submitted_arrays) == 0:
                return

            yield ["array_n", "job_id", *squeue_columns, "filters"]

            for array_n, job_id, job_file in submitted_arrays:
                yield row_of_array(array_n, job_id, job_file)

        squeue_columns = [c.long_code.lower() for c in counted_codes] + ["total", "ram", "cpus", "walltime", "account"]

        def row_of_array(array_n, job_id, job_file):

            def row(cells):
                # error rows have a single cell, pad them so that filters stay in the last column
                padding = [""] * (len(squeue_columns) - len(cells))
                return [f"array.{array_n}", job_id, *cells, *padding, self._filters_of_submit(job_file)]

            # -r expands the array into one line per task, the resources (tres-alloc), the wall
            # time and the account are the same for all tasks of an array
            squeue_cmd = [
                "squeue", "-r", "--noheader", "--Format=JobArrayID:|,StateCompact:|,tres-alloc:|,TimeLimit:|,Account:|",
                "--jobs", job_id
            ]

            with PortablePopen(squeue_cmd) as p:
                try:
                    # communicate(), NOT wait(): squeue on a large array writes more than the
                    # pipe buffer can hold, and wait() deadlocks, since it never drains stdout
                    stdout, stderr = p.communicate(timeout=SQUEUE_TIMEOUT_SECS)
                except subprocess.TimeoutExpired:
                    p.popen.kill()
                    return row([f"squeue timed out after {SQUEUE_TIMEOUT_SECS} seconds"])

            if p.popen.returncode != 0:
                if "Invalid job id" in stderr:
                    # slurm has purged the job, it remembers nothing of its tasks anymore
                    return row(["inactive"])
                # any other squeue failure, ex: slurmctld unreachable, report it and
                # carry on with the other arrays
                return row([stderr.strip()])

            squeue_rows = [line.strip().split("|") for line in stdout.split("\n") if line.strip() != ""]

            count_per_short_code = {c.short_code: 0 for c in counted_codes}

            for squeue_row in squeue_rows:
                short_code = squeue_row[1]
                if short_code in count_per_short_code:
                    count_per_short_code[short_code] += 1

            def ram_cpus_walltime_account():
                if len(squeue_rows) == 0:
                    return ["", "", "", ""]
                _, _, tres_alloc, walltime, account = squeue_rows[0][:5]
                tres = dict(t.split("=", 1) for t in tres_alloc.split(",") if "=" in t)
                return [tres.get("mem", ""), tres.get("cpu", ""), walltime, account]

            return row(
                [str(count_per_short_code[c.short_code]) for c in counted_codes] +
                [str(task_count_in_array_file(array_n))] +
                ram_cpus_walltime_account()
            )

        self._print_table(list(rows()))

    def _filters_of_submit(self, job_file):
        """
        parses the command line that submitted an array, on the 2nd line of its job file
        """

        lines = Path(job_file).read_text().splitlines()

        # job files written before the command line was recorded have only the sbatch command
        if len(lines) < 2 or lines[1] == "":
            return ""

        try:
            submit_args = self.parser.parse_args(shlex.split(lines[1])[1:])
        except SystemExit:
            return "unparsable submit command"

        subparser = self.subparsers.choices[submit_args.command]

        def given_filters():
            for dest in self.filter_dests:
                value = getattr(submit_args, dest, None)
                if value != subparser.get_default(dest):
                    option = "--" + dest.replace("_", "-")
                    yield option if value is True else f"{option}={value}"

        return shlex.join(given_filters())

    def array_summary(self):

        m = self._array_manage()        
        self._array_squeue(m)
        self._dump_state_step_counts(m.children_task_keys())

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


    def times(self):

        # --task-key restricts the universe the filters (--filter, --py-filter, --func-filter, ...)
        # are applied to, so that this command selects tasks the same way status, summary,
        # tail-logs, etc do
        if self.parsed_args.task_key is not None:
            key_universe = {self.parsed_args.task_key}
        else:
            key_universe = None

        task_keys = (
            key for key, _, _, _ in self.filter_key_state_step(key_universe)
        )

        for task_key, timer_label, hms, s in timers_for_tasks(
            self.parsed_args.pipeline_instance_dir,
            task_keys,
            include_steps=self.parsed_args.include_steps,
        ):
            print(f"{timer_label}\t{task_key}\t{hms}\t{s}", file=self.output)

    def grep_logs(self):
        for key, state, step, sf in self.filter_key_state_step():
            out_log = Path(self.parsed_args.pipeline_instance_dir, ".drypipe", key, "out.log").absolute()
            if out_log.exists():
                cmd = ["grep", "-F", self.parsed_args.grep_expr, str(out_log)]
                subprocess.check_call(cmd)


    def rsync_push(self):
        self._rsync_selected_files(
            f"{self.parsed_args.pipeline_instance_dir}/", self.parsed_args.dest, f"dest: {self.parsed_args.dest}"
        )

    def rsync_pull(self):
        self._rsync_selected_files(
            self.parsed_args.source, f"{self.parsed_args.pipeline_instance_dir}/", f"source: {self.parsed_args.source}"
        )

    def archive(self):
        archive_file = Path(self.parsed_args.name).absolute()
        pid = Path(self.parsed_args.pipeline_instance_dir).absolute()

        with tempfile.TemporaryDirectory(dir=pid / ".drypipe", prefix="archive-staging-") as staging_dir:

            # hard links instead of copies, the staging dir being on the same file system as the pipeline instance
            link_dest = f"--link-dest={pid}/"

            if not self._rsync_selected_files(f"{pid}/", f"{staging_dir}/", f"archive: {archive_file}", [link_dest]):
                return

            command = self.parsed_args.archive_command.replace("{archive}", shlex.quote(str(archive_file)))

            if self.parsed_args.dry_run:
                print(f"DRY RUN {command}", file=self.output)
                return

            subprocess.run(command, shell=True, cwd=staging_dir, check=True)

        print(f"created {archive_file}", file=self.output)

    def _rsync_selected_files(self, source, dest, transfer_description, extra_rsync_args=()):

        def ensure_rsync_version_at_least(minimal_version):
            out = subprocess.run(["rsync", "--version"], capture_output=True, text=True, check=True).stdout
            m = re.search(r"version (\d+)\.(\d+)\.(\d+)", out)
            version = tuple(int(n) for n in m.groups()) if m else None
            if version is None or version < minimal_version:
                raise Exception(
                    f"rsync requires rsync >= {'.'.join(map(str, minimal_version))}, got: {out.splitlines()[0]}"
                )

        ensure_rsync_version_at_least((3, 2, 7))

        include_drypipe_files = self.parsed_args.include_drypipe_files

        exhaustive = self.parsed_args.exhaustive

        if exhaustive and (
            self.parsed_args.tags is not None or getattr(self.parsed_args, "exclude_tags", None) is not None or
            include_drypipe_files != "none"
        ):
            raise Exception("--exhaustive cannot be combined with --tags, --exclude-tags or --include-drypipe-files")

        state_files_only = getattr(self.parsed_args, "state_files_only", False)

        if state_files_only and (self.parsed_args.tags is not None or include_drypipe_files != "none" or exhaustive):
            raise Exception("--state-files-only cannot be combined with --tags, --include-drypipe-files or --exhaustive")

        tasks_and_file_outputs = self._tasks_and_selected_file_outputs()

        if state_files_only:
            print(f"state files of {len(tasks_and_file_outputs)} tasks", file=self.output)
        else:
            self._print_selected_file_outputs_summary(tasks_and_file_outputs)
        if include_drypipe_files != "none":
            print(f"drypipe files ({include_drypipe_files}) of {len(tasks_and_file_outputs)} tasks", file=self.output)
        if exhaustive:
            print(f"exhaustive: all files of {len(tasks_and_file_outputs)} tasks", file=self.output)
        self._print_ignored_tasks_summary()
        print(transfer_description, file=self.output)

        if not self.parsed_args.no_confirm and not self.query_yes_no("continue ?", default="no"):
            return False

        def control_dirs():
            for task, _ in tasks_and_file_outputs:
                yield f".drypipe/{task.key}"

        def output_and_control_dirs():
            for task, _ in tasks_and_file_outputs:
                yield f"output/{task.key}"
                yield f".drypipe/{task.key}"

        def tagged_files_and_control_dirs():
            for task, file_outputs in tasks_and_file_outputs:
                for o in file_outputs:
                    yield task.outputs.rsync_path(o)
                if include_drypipe_files != "none":
                    # listed as a dir, so that --delete removes stale state files at dest
                    yield f".drypipe/{task.key}"

        def selected_paths():
            if state_files_only:
                return control_dirs()
            elif exhaustive:
                return output_and_control_dirs()
            else:
                return tagged_files_and_control_dirs()

        def drypipe_files_filters():
            # excluded files are also protected from --delete
            if include_drypipe_files == "minimal":
                yield "--include=/.drypipe/*/out.log"
                yield "--include=/.drypipe/*/drypipe.log"
            if include_drypipe_files == "minimal" or state_files_only:
                yield "--include=/.drypipe/*/state.*"
                yield "--exclude=/.drypipe/*/*"

        def reporting_and_dry_run_options():
            yield "--info=progress2" if getattr(self.parsed_args, "info_progress2", False) else "--stats"
            if self.parsed_args.dry_run:
                yield "--dry-run"

        rsync = subprocess.run(
            [
                "rsync", "-a", "-r", "--delete", "--ignore-missing-args", *reporting_and_dry_run_options(),
                *drypipe_files_filters(), *extra_rsync_args,
                "--files-from=-", source, dest
            ],
            input="\n".join(selected_paths()),
            text=True
        )

        # 24: files vanished during transfer, ex: state file renamed by a running task
        if rsync.returncode not in (0, 24):
            raise subprocess.CalledProcessError(rsync.returncode, rsync.args)

        return True

    def purge_outputs(self):

        tasks_and_file_outputs = self._tasks_and_selected_file_outputs()

        self._print_selected_file_outputs_summary(tasks_and_file_outputs)
        self._print_ignored_tasks_summary()

        if self._selected_tags() == (None, None):
            print(
                "warning: neither --tags nor --exclude-tags given, ALL output files of matching tasks will be deleted",
                file=self.output
            )

        if not self.parsed_args.no_confirm and not self.query_yes_no("delete them ?", default="no"):
            return

        def existing_output_files():
            for task, file_outputs in tasks_and_file_outputs:
                for o in file_outputs:
                    f = Path(self.parsed_args.pipeline_instance_dir, task.outputs.rsync_path(o))
                    if f.exists():
                        yield f

        if self.parsed_args.dry_run:
            for f in existing_output_files():
                print(f"DRY RUN rm {f}", file=self.output)
            return

        deleted_count = 0
        for f in existing_output_files():
            f.unlink()
            deleted_count += 1

        print(f"deleted {deleted_count} output files", file=self.output)

    def _selected_tags(self):

        def tag_set(arg):
            return None if arg is None else set(arg.split(","))

        tags = tag_set(self.parsed_args.tags)
        excluded_tags = tag_set(getattr(self.parsed_args, "exclude_tags", None))

        if tags is not None and excluded_tags is not None:
            raise Exception("--tags cannot be combined with --exclude-tags")

        return tags, excluded_tags

    def _tasks_and_selected_file_outputs(self):
        tags, excluded_tags = self._selected_tags()
        # materialized: iterated once for the summary, once for the action on the files
        return [
            (task, list(task.outputs.file_outputs_selected_by_tags(tags, excluded_tags)))
            for task, _ in self.filter_tasks()
        ]

    def _print_selected_file_outputs_summary(self, tasks_and_file_outputs):

        tags, excluded_tags = self._selected_tags()

        file_count_per_tag = collections.Counter(
            tag
            for _, file_outputs in tasks_and_file_outputs
            for o in file_outputs
            for tag in (o.tags or ["(untagged)"])
        )

        # requested tags are all shown, a 0 count reveals a misspelled tag
        shown_tags = sorted(file_count_per_tag) if tags is None else sorted(tags)

        print(f"tasks: {len(tasks_and_file_outputs)}", file=self.output)
        if excluded_tags is not None:
            print(f"excluded tags: {', '.join(sorted(excluded_tags))}", file=self.output)
        print("output files per tag (a file with many tags is counted for each):", file=self.output)
        for tag in shown_tags:
            print(f"  {tag}: {file_count_per_tag[tag]}", file=self.output)
        print(f"total output files: {sum(len(file_outputs) for _, file_outputs in tasks_and_file_outputs)}", file=self.output)

    def _print_ignored_tasks_summary(self):
        if self.task_set is not None:
            unmatched_keys = sorted(self.task_set.keys_of_files - self.yielded_task_keys)
            examples = f" (ex: {', '.join(repr(k) for k in unmatched_keys[:5])})" if unmatched_keys else ""
            print(f"ignored tasks: {len(self.yielded_ignored_task_keys)} (from {self.task_set_file})", file=self.output)
            print(f"keys of task set files not yielded by the generator: {len(unmatched_keys)}{examples}", file=self.output)

    def garbage_collect(self):

        pid = self.parsed_args.pipeline_instance_dir

        task_keys = {task.key for task, _ in self.pipeline_instance_from_args().iterate_key_state_steps()}

        def task_control_dirs():
            # .drypipe also holds non task dirs (dry_pipe code, messages, etc), a task dir has a state file
            for d in Path(pid, ".drypipe").iterdir():
                if d.is_dir() and StateFileTracker.find_state_file_path_if_exists(d) is not None:
                    yield d

        def task_output_dirs():
            yield from (d for d in Path(pid, "output").iterdir() if d.is_dir())

        orphan_dirs = [
            d for d in [*task_control_dirs(), *task_output_dirs()]
            if d.name not in task_keys
        ]

        self._delete_dirs_after_confirmation(orphan_dirs, "tasks not yielded by the generator")

    def purge_tasks_not_in_task_set(self):

        pid = self.parsed_args.pipeline_instance_dir

        pipeline_instance = self.pipeline_instance_from_args()

        if self.task_set is None:
            raise Exception(
                f"no ignored tasks: --task-set is not given and {pid}/drypipe-task-set.rules does not exist"
            )

        # the generator yields all tasks, ignored ones are collected in self.yielded_ignored_task_keys
        for _ in pipeline_instance.iterate_key_state_steps():
            pass

        self._print_ignored_tasks_summary()

        ignored_task_dirs = [
            d
            for key in sorted(self.yielded_ignored_task_keys)
            for d in [Path(pid, ".drypipe", key), Path(pid, "output", key)]
            if d.is_dir()
        ]

        self._delete_dirs_after_confirmation(ignored_task_dirs, "ignored tasks")

    def _delete_dirs_after_confirmation(self, dirs, description):

        for d in dirs:
            print(d, file=self.output)
        print(f"{len(dirs)} directories of {description}", file=self.output)

        if len(dirs) == 0:
            return

        if not self.parsed_args.no_confirm and not self.query_yes_no("delete them ?", default="no"):
            return

        for d in dirs:
            shutil.rmtree(d)


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


    def analyze_logs(self):

        def state_step(key_state_step_sf):
            _, state, step, _ = key_state_step_sf
            return state, -1 if step is None else step

        def file_name(state, step, count):
            if step == -1:
                return f"{state}-{count}.md"
            return f"{state}.{step}-{count}.md"

        dest_dir = Path(self.parsed_args.dir)
        dest_dir.mkdir(parents=True, exist_ok=True)

        classifier = self._log_classifier()

        def is_selected(key_state_step_sf):
            _, state, _, _ = key_state_step_sf
            return self.parsed_args.all_tasks or self._is_unhealthy(state)

        tasks = sorted(filter(is_selected, self.filter_key_state_step()), key=state_step)
        for (state, step), group in groupby(tasks, key=state_step):
            group = list(group)
            signature_groups = self._signature_groups(group, classifier)
            classified_keys = {key for _, sig_keys in signature_groups for key in sig_keys}
            keys = [key for key, _, _, _ in group if key in classified_keys]
            if len(keys) == 0:
                continue
            with open(dest_dir / file_name(state, step, len(keys)), "w") as f:
                self._write_error_signatures_table(signature_groups, f)
                self._write_markdown_tails(keys, signature_groups, f)

    @staticmethod
    def _is_unhealthy(state_name):
        """ended without completing"""
        return state_name.startswith(("failed", "timed-out", "killed", "crashed"))

    def _log_classifier(self):
        spec = self.parsed_args.log_classifier
        if spec is None:
            return TaskClassifier().compile()

        customize = func_from_mod_func(self._resolve_against_generator_module(spec, "--log-classifier"))
        classifier = customize(TaskClassifier())
        missing_methods = [
            m for m in ("signature", "reads_logs", "extract_out_log", "extract_drypipe_log") if not hasattr(classifier, m)
        ]
        if missing_methods:
            raise Exception(
                f"--log-classifier '{spec}' must return a classifier with the methods of TaskClassifier, "
                f"got a {type(classifier).__name__} without {', '.join(missing_methods)}"
            )
        return classifier.compile() if hasattr(classifier, "compile") else classifier

    def _out_log(self, key):
        return Path(self.parsed_args.pipeline_instance_dir, ".drypipe", key, "out.log")

    @staticmethod
    def _last_lines(out_log, n):
        text = read_last_lines(out_log, n).decode(errors="replace")
        # the newlines of a file read in text mode
        return text.replace("\r\n", "\n").replace("\r", "\n")

    def _classify_context(self, key, state, step, classifier, is_lean):
        return TaskClassifyContext(
            Path(self.parsed_args.pipeline_instance_dir, ".drypipe", key), classifier.reads_logs(key, state, step, is_lean)
        )

    def _log_signature(self, key, state, step, classifier):
        return classifier.signature(key, state, step, self._classify_context(key, state, step, classifier, False))

    def _signature_groups(self, tasks, classifier):
        """(signature, keys) pairs, largest group first, their rank is the signature number, None signatures are left out"""

        keys_by_signature = collections.defaultdict(list)
        for key, state, step, _ in tasks:
            signature = self._log_signature(key, state, step, classifier)
            if signature is not None:
                keys_by_signature[signature].append(key)
        return sorted(keys_by_signature.items(), key=lambda i: -len(i[1]))

    def _write_error_signatures_table(self, signature_groups, file):

        def markdown_code(text):
            text = text.replace("|", "\\|")
            return f"`` {text} ``" if "`" in text else f"`{text}`"

        def table_rows():
            if self.parsed_args.full:
                yield "| # | tasks | signature | keys |"
                yield "|---:|---:|---|---|"
            else:
                yield "| # | tasks | signature |"
                yield "|---:|---:|---|"

            for number, (sig, sig_keys) in enumerate(signature_groups, start=1):
                keys_cell = f" {' '.join(sig_keys)} |" if self.parsed_args.full else ""
                yield f"| {number} | {len(sig_keys)} | {markdown_code(sig)} |{keys_cell}"

        n_tasks = sum(len(sig_keys) for _, sig_keys in signature_groups)
        print("## Error signatures\n", file=file)
        print(f"{n_tasks} tasks, {len(signature_groups)} signatures\n", file=file)
        for row in table_rows():
            print(row, file=file)
        print("", file=file)

    def extract_keys(self):

        path = self.parsed_args.analysis_file
        lines = Path(path).read_text().splitlines()

        def ask_selected_numbers(table):
            for number, (count, signature_cell) in enumerate(table, start=1):
                print(f"{number:4}  {count:6}  {signature_cell}", file=sys.stderr)
            print("signatures to extract (ex: 1,3): ", end="", file=sys.stderr, flush=True)
            return _signature_numbers(sys.stdin.readline(), len(table))

        selected = ask_selected_numbers(list(_signatures_of_analysis_file(path, lines)))
        # a list: a malformed file must fail before a partial list of keys is written
        keys = [key for key, number in _keys_and_signature_numbers_of_analysis_file(path, lines) if number in selected]

        if self.parsed_args.dest is None:
            for key in keys:
                print(key, file=self.output)
        else:
            with open(self.parsed_args.dest, "w") as f:
                for key in keys:
                    print(key, file=f)

    def _write_markdown_tails(self, keys, signature_groups, file):

        def code_fence(text):
            """longer than any backtick run in text, so that the text can't close the code block"""
            longest_run = max((len(run) for run in re.findall(r"`+", text)), default=0)
            return "`" * max(3, longest_run + 1)

        signature_number_of_key = {
            key: number
            for number, (_, sig_keys) in enumerate(signature_groups, start=1)
            for key in sig_keys
        }

        print("## Tails\n", file=file)
        for i, key in enumerate(keys, start=1):
            print(f"### {i}. {key} (signature {signature_number_of_key[key]})\n", file=file)
            out_log = self._out_log(key)
            if not out_log.exists():
                print("no out.log\n", file=file)
                continue
            text = self._last_lines(out_log, self.parsed_args.n)
            fence = code_fence(text)
            print(f"tail -{self.parsed_args.n} .drypipe/{key}/out.log\n", file=file)
            print(f"{fence}text\n{text.rstrip()}\n{fence}\n", file=file)

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

            if self.parsed_args.filter_timed_out:
                return lambda key, state_name, step: state_name == 'timed-out'

            if self.parsed_args.filter_ready:
                return lambda key, state_name, step: state_name == 'ready'
            
            if self.parsed_args.filter_not_completed:
                return lambda key, state_name, step: state_name != 'completed'
            
            if self.parsed_args.filter_failed:
                return lambda key, state_name, step: state_name.startswith("failed")

            if self.parsed_args.filter_unhealthy:
                return lambda key, state_name, step: self._is_unhealthy(state_name)

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

            factory = func_from_mod_func(self._resolve_against_generator_module(reference, "--func-filter"))

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

        def create_key_file_filter():
            if self.parsed_args.filter_from is None:
                return tautology
            keys = _load_filter_from_keys(self.parsed_args.filter_from)
            return lambda key, state_name, step: key in keys

        def g():
            yield create_glob_filter()
            yield create_key_file_filter()
            yield create_py_filter()
            yield create_func_filter()

        return list(g())


    def filter_tasks(self, key_universe=None):

        def dag_from_args():
            if self._uses_fs_generator():
                return self.frozen_dag_generator_from_args()
            pipeline_instance = self.pipeline_instance_from_args()
            pipeline_instance.prepare_instance_dir()
            return pipeline_instance

        dag = dag_from_args()

        filter_chain = self.create_filter_chain()

        stop_after_step = getattr(self.parsed_args, "stop_after_step", None)

        def accept(key, state, step):
            if stop_after_step is not None and step is not None and step > stop_after_step:
                return False
            for f in filter_chain:
                if not f(key, state, step):
                    return False
            return True

        for task, state_file in dag.iterate_key_state_steps(key_universe):
            if accept(*state_file.key_state_step()):
                yield task, state_file

    def _resolve_against_generator_module(self, reference, option_name):
        """a bare function name (no module, i.e. no ":") is looked up in the --generator module"""
        if ":" in reference:
            return reference

        if self._uses_fs_generator():
            raise Exception(f"{option_name} '{reference}' must be given as module:function with --fs-generator")

        generator = self.parsed_args.generator
        if generator is None:
            raise Exception(
                f"{option_name} '{reference}' has no module, and can't be resolved against "
                f"the --generator module because --generator is not set"
            )
        generator_module = generator.split(":")[0]
        return f"{generator_module}:{reference}"

    def filter_key_state_step(self, key_universe=None):
        for _, state_file in self.filter_tasks(key_universe):
            key, state, step = state_file.key_state_step()
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
        if self.parsed_args.filter_from is not None:
            return True
        if self.parsed_args.filter_completed:
            return True
        
        if self.parsed_args.filter_not_completed:
            return True
        
        if self.parsed_args.filter_failed:
            return True

        if self.parsed_args.filter_timed_out:
            return True

        if self.parsed_args.filter_ready:
            return True

        if self.parsed_args.filter_unhealthy:
            return True
        
        return False


    def list_keys(self):
        for key, _, _, _ in self.filter_key_state_step():
            print(key, file=self.output)

    def run_from_slurm_job(self):
        task_process = TaskProcess(
            self._control_dir(),
            stop_after_step=self.parsed_args.stop_after_step
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
                    tasks_per_job=tasks_per_job,
                    stop_after_step=self.parsed_args.stop_after_step
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
        self._dump_state_step_counts()

    def _print_table(self, rows):
        """
        prints rows (lists of strings, the first one being the header), tab separated, or
        aligned in columns when the output is a terminal: aligned is what reads well in a
        shell, tab separated is what cut, awk, and the tests can parse
        """

        if len(rows) == 0:
            return

        if not self.output.isatty():
            for row in rows:
                print("\t".join(row), file=self.output)
            return

        column_count = max([len(row) for row in rows])

        def cells_of_column(i):
            return [row[i] for row in rows if i < len(row)]

        widths = [max([len(c) for c in cells_of_column(i)]) for i in range(column_count)]

        # numbers read better right aligned, ex: 1000 lining up under 999, labels left aligned.
        # the choice is per cell, NOT per column, so that a count keeps its alignment in a
        # column where another row has text, ex: "inactive"
        for row in rows:
            line = "  ".join([
                cell.rjust(widths[i]) if cell.isdigit() else cell.ljust(widths[i])
                for i, cell in enumerate(row)
            ])
            print(line.rstrip(), file=self.output)

    def _dump_state_step_counts(self, key_universe=None):

        all = [
            (state, step, key)
            for key, state, step, _ in self.filter_key_state_step(key_universe)
        ]                     

        def k(t):
            return t[0], t[1]


        def rows():
            yield ["state", "step", "count"]        
            for state_step, tuples in groupby(sorted(all, key=k), key=k):
                state, step = state_step
                cnt = len(list(tuples))
                step = "" if step is None else step
                #print(f"{state}\t{step}\t{cnt}", file=self.output)
                yield [state, str(step), str(cnt)]

        self._print_table(list(rows()))


    def status_db(self):

        if self.parsed_args.empty_db is not None:
            if self.parsed_args.tsv:
                raise Exception("--empty-db creates an sqlite db, it can't be combined with --tsv")
            PipelineInstance.create_empty_status_db(Path(self.parsed_args.empty_db)).close()
            return

        if self.parsed_args.pipeline_instance_dir is None:
            raise Exception("--pipeline-instance-dir is required unless --empty-db is specified")

        self.complain_if_no_generator("unless --empty-db is specified")

        def iterate_key_state_steps():
            for key, state, step, _ in self.filter_key_state_step():
                yield key, state, step

        classifier = self._log_classifier()
        is_lean = self.parsed_args.lean

        def logs_and_signature(key, state, step):
            context = self._classify_context(key, state, step, classifier, is_lean)
            return (
                classifier.extract_drypipe_log(context, is_lean),
                classifier.extract_out_log(context, is_lean),
                classifier.signature(key, state, step, context)
            )

        PipelineInstance.write_status_db(
            self.parsed_args.pipeline_instance_dir,
            iterate_key_state_steps,
            logs_and_signature,
            self.parsed_args.instance_name,
            self.parsed_args.tsv
        )


def run_cli():
    handle_script_lib_main()

def setup_file_creation_mask():
    os.umask(0o007)

def enforce_primary_group():
    """
    When DRYPIPE_PRIMARY_GROUP is set, require that the process' primary group
    (the group stamped on every newly created file/dir) matches it. This lets
    several unix users collaborate under DRYPIPE_PIPELINE_INSTANCE_DIR: files
    created by any of them are owned by the shared project group, so the others
    (members of that group) can read, write and delete them.

    An unprivileged process cannot switch its primary group to a supplementary
    group, only newgrp/sg can, so on mismatch we refuse and tell the user how.
    """
    group_name = os.environ.get("DRYPIPE_PRIMARY_GROUP")
    if not group_name:
        return

    try:
        expected_gid = grp.getgrnam(group_name).gr_gid
    except KeyError:
        raise Exception(f"DRYPIPE_PRIMARY_GROUP={group_name} is not a known group")

    current_gid = os.getegid()
    if current_gid == expected_gid:
        return

    try:
        current_group_name = grp.getgrgid(current_gid).gr_name
    except KeyError:
        current_group_name = str(current_gid)

    raise Exception(
        f"DRYPIPE_PRIMARY_GROUP={group_name}, and current group is "
        f"{current_group_name}, please run newgrp {group_name}"
    )

def handle_script_lib_main():
    try:
        setup_file_creation_mask()
        enforce_primary_group()
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
    cli = Cli([], parse_args=False)
    return cli.parser

if __name__ == '__main__':

    if "SLURM_JOB_ID" in os.environ:
        setup_file_creation_mask()
        enforce_primary_group()
        call(sys.argv[2])
    else:
        handle_script_lib_main()
