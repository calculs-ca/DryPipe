import argparse
import json
import logging
import os
import sys
import textwrap
from pathlib import Path

from dry_pipe.core_lib import func_from_mod_func, StateFileTracker, is_inside_slurm_job
from dry_pipe.task_process import TaskProcess, SlurmArrayParentTask


def call(mod_func):

    python_task = func_from_mod_func(mod_func)
    control_dir = os.environ["__control_dir"]
    task_runner = TaskProcess(control_dir)
    task_runner.call_python(mod_func, python_task)


class Cli:

    def __init__(self, args, invocation_script=None, env={}):
        self.env = env
        self.parser = argparse.ArgumentParser(
            description="DryPipe CLI"
        )

        self._add_pipeline_instance_dir_arg(self.parser)

        self.parser.add_argument(
            '--verbose'
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
            default_pid = Path(__file__).parent.parent

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
    def _add_task_key_parser_arg(self, parser):

        control_dir = self._guess_control_dir_from_cwd()

        if control_dir is not None:
            default_task_key = os.path.basename(control_dir)
        else:
            default_task_key = None

        parser.add_argument(
            '--task-key',
            default=default_task_key
        )

    def _control_dir(self):
        pipeline_instance_dir = self.parsed_args.pipeline_instance_dir
        task_key = self.parsed_args.task_key
        return os.path.join(pipeline_instance_dir, ".drypipe", task_key)

    def _complete_control_dir(self, maybe_partial_contro_dir):
        if os.path.exists(maybe_partial_contro_dir):
            return os.path.abspath(maybe_partial_contro_dir)

        cd = os.path.join(os.getcwd(), maybe_partial_contro_dir)

        if os.path.exists(cd):
            return cd

        raise Exception(f"directory not found {cd}")

    def _wait(self):
        return self.parsed_args.wait

    def get_ssh_remote_dest_or_none(self, task_conf):
        ssh_remote_dest = task_conf.get("ssh_remote_dest")
        if ssh_remote_dest is None:
            if self.parsed_args.ssh_remote_dest is None:
                raise Exception(
                    f"--ssh-remote-dest is required for 'upload-array', OR must be defined with " +
                    " .task(task_conf=TaskConf(ss_remote_dest=...)"
                )
            else:
                ssh_remote_dest = self.parsed_args.ssh_remote_dest

    def invoke(self, test_mode=False):

        if self.parsed_args.command == 'submit-array':
            tp = TaskProcess(
                os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key),
                as_subprocess=not test_mode,
                test_mode=test_mode
            )
            tp.run(
                array_limit=self.parsed_args.limit
            )
        elif self.parsed_args.command == 'run':
            pipeline = func_from_mod_func(self.parsed_args.generator)()
            pipeline_instance = pipeline.create_pipeline_instance(self.parsed_args.pipeline_instance_dir)
            pipeline_instance.run_sync(until_patterns=self.parsed_args.until)
        elif self.parsed_args.command == 'task':

            tp = TaskProcess(self._complete_control_dir(self.parsed_args.control_dir))

            if self.parsed_args.ssh_remote_dest is not None:
                tp.task_conf["ssh_specs"] = self.parsed_args.ssh_remote_dest
            elif tp.task_conf["executer_type"] == "slurm":
                if self.parsed_args.by_runner:
                    tp.submit_sbatch_task(self._wait())
                    return

            tp.launch_task(self._wait(), exit_process_when_done=not test_mode)

        elif self.parsed_args.command == 'sbatch':
            tp = TaskProcess(self.parsed_args.control_dir)
            tp.submit_sbatch_task(self._wait())

        elif self.parsed_args.command == 'upload-array':

            tp = TaskProcess(
                os.path.join(self.parsed_args.pipeline_instance_dir, ".drypipe", self.parsed_args.task_key)
            )

            array_parent_task = SlurmArrayParentTask(
                tp.task_key,
                StateFileTracker(tp.pipeline_instance_dir),
                task_conf=tp.task_conf,
                logger=tp.task_logger
            )

            ssh_remote_dest = self.get_ssh_remote_dest_or_none(array_parent_task.task_conf)

            array_parent_task.upload_array(ssh_remote_dest)

        elif self.parsed_args.command == 'create-array-parent':

            new_task_key = self.parsed_args.new_task_key
            matcher = self.parsed_args.matcher

            if self.parsed_args.slurm_account is not None:
                executer_type = "slurm"
                slurm_account = self.parsed_args.slurm_account
            else:
                executer_type = "process"
                slurm_account = None

            split_into = self.parsed_args.split

            state_file_tracker = StateFileTracker(self.parsed_args.pipeline_instance_dir)

            control_dir = Path(state_file_tracker.pipeline_work_dir, new_task_key)

            if os.path.exists(control_dir):
                if not self.parsed_args.force:
                    raise Exception(
                        f"Directory {control_dir} already exists, use --force to overwrite"
                    )

            control_dir.mkdir(exist_ok=True)

            not_ready_task_keys = []

            with open(os.path.join(control_dir, "task-conf.json"), "w") as f:
                f.write(json.dumps({
                  "executer_type": executer_type,
                  "slurm_account": slurm_account,
                  "is_slurm_parent": True,
                  "inputs": [
                    {
                      "upstream_task_key": None,
                      "name_in_upstream_task": None,
                      "file_name": None,
                      "value": None,
                      "name": "children_tasks",
                      "type": "task-list"
                    }
                  ]
                }, indent=2))

            with open(os.path.join(control_dir, "task-keys.tsv"), "w") as tc:
                for resolved_task in state_file_tracker.load_tasks_for_query(matcher, include_non_completed=True):

                    if resolved_task.is_completed():
                       continue

                    # ensure upstream dependencies are met
                    tp = TaskProcess(resolved_task.control_dir(), no_logger=True)
                    tp._unserialize_and_resolve_inputs_outputs(ensure_all_upstream_deps_complete=True)

                    if not resolved_task.is_ready():
                        not_ready_task_keys.append(resolved_task.key)

                    tc.write(resolved_task.key)
                    tc.write("\n")

            if len(not_ready_task_keys) > 0:
                print(f"Warning: {len(not_ready_task_keys)} are not in 'ready' state:")

            Path(os.path.join(control_dir, "state.ready")).touch(exist_ok=True)

    def _sub_parsers(self):


        self.subparsers = self.parser.add_subparsers(required=True, dest='command')
        self.add_run_args(self.subparsers.add_parser('run'))
        self.add_task_args(self.subparsers.add_parser('task'))
        self.add_sbatch_args(self.subparsers.add_parser('sbatch'))
        self.add_sbatch_args(self.subparsers.add_parser('sbatch-gen'))
        self.add_array_args(self.subparsers.add_parser('submit-array'))
        self.add_upload_array_args(self.subparsers.add_parser('upload-array'))
        self.add_create_array_parent_args(self.subparsers.add_parser('create-array-parent'))

    def add_status_args(self):
        pass

    def add_run_args(self, run_parser):

        run_parser.add_argument(
            '--generator',
            help='<module>:<function> task generator function, can also be set with environment var DRYPIPE_PIPELINE_GENERATOR',
            metavar="GENERATOR",
            default=self.env.get("DRYPIPE_PIPELINE_GENERATOR")
        )

        run_parser.add_argument(
            '--until', help='tasks matching PATTERN will not be started',
            action='append',
            metavar='PATTERN'
        )

        self._add_task_key_parser_arg(run_parser)

    def add_upload_array_args(self, upload_array_parser):

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

        run_parser.add_argument(
            '--restart-failed',
            action='store_true', default=False,
            help='re submit failed tasks in array, restart from last failed step, keep previous output'
        )

        run_parser.add_argument(
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
        parser.add_argument('control_dir', type=str)
        self._add_task_key_parser_arg(parser)
        self.__wait_arg(parser)
        parser.add_argument("--by-runner", dest="by_runner", action="store_true")
        parser.set_defaults(by_runner=False)
        self.add_ssh_remote_dest_arg(parser)

    def add_sbatch_args(self, parser):
        self._add_task_key_parser_arg(parser)
        self.__wait_arg(parser)

def handle_script_lib_main():
    try:
        cli = Cli(sys.argv[1:])
        cli.invoke()
    finally:
        logging.shutdown()


if __name__ == '__main__':
    call(sys.argv[2])
