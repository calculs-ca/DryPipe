import glob
import logging
import os
import re
import time
from pathlib import Path

from dry_pipe import DryPipe, TaskConf
# imported via aliases so pytest does not collect them as tests of this module
import dry_pipe_tests.base_pipeline_test as _bpt
from dry_pipe_tests.cli_tests import test_cli as _run_cli, create_cli as _create_cli
from dry_pipe_tests.test_utils import TestSandboxDir

python_path_for_tests = str(Path(__file__).resolve().parent.parent)

_generator = 'dry_pipe_tests.stop_after_step_tests:stop_after_step_pipeline'


def _slurm_task_conf():
    return TaskConf(
        executer_type="slurm",
        slurm_account="dummy-account",
        extra_env={
            "PYTHONPATH": python_path_for_tests,
            "DRYPIPE_SLEEP_SCHEDULE": "1"
        }
    ).with_sbatch_options(time="1:00:1")


def _three_step_task(dsl, key, is_slurm_array_child=False):
    return dsl.task(
        key=key,
        is_slurm_array_child=is_slurm_array_child,
        task_conf=_slurm_task_conf()
    ).outputs(
        out_file=dsl.file("out_file.txt")
    ).calls("""
        #!/usr/bin/env bash
        echo "s0" >> $out_file
    """).calls("""
        #!/usr/bin/env bash
        echo "s1" >> $out_file
    """).calls("""
        #!/usr/bin/env bash
        echo "s2" >> $out_file
    """)()


def stop_after_step_dag(dsl):
    # a standalone slurm task, launched with `cli sbatch`
    yield _three_step_task(dsl, "solo")

    # an array of slurm tasks, launched with `cli array-submit`
    children = [
        _three_step_task(dsl, f"t{i}", is_slurm_array_child=True)
        for i in range(1, 4)
    ]
    yield from children

    yield dsl.task(
        key="ap",
        task_conf=_slurm_task_conf()
    ).slurm_array_parent(
        children_tasks=children
    )()


def stop_after_step_pipeline():
    return DryPipe.create_pipeline(stop_after_step_dag)


class StopAfterStepBaseTest(_bpt.BasePipelineTest):

    # disable the generic full-pipeline test inherited from BasePipelineTest,
    # these tests drive the pipeline through the cli instead
    test_run_pipeline = None

    # shared dag for both the `cli sbatch` and the `cli array-submit` tests
    def dag_gen(self, dsl):
        yield from stop_after_step_dag(dsl)

    def validate(self, tasks_by_keys):
        pass

    def _control_dir(self, sandbox_dir, task_key):
        return os.path.join(sandbox_dir, ".drypipe", task_key)

    def _state_basename(self, control_dir):
        files = [os.path.basename(f) for f in glob.glob(os.path.join(control_dir, "state.*"))]
        self.assertEqual(len(files), 1, f"expected exactly one state file in {control_dir}, got {files}")
        return files[0]

    def _out_file_content(self, sandbox_dir, task_key):
        out_file = os.path.join(sandbox_dir, "output", task_key, "out_file.txt")
        with open(out_file) as f:
            return f.read()

    def _wait_until_state(self, control_dir, expected_basename, timeout=180):
        deadline = time.time() + timeout
        while time.time() < deadline:
            basename = self._state_basename(control_dir)
            if basename == expected_basename:
                return
            if basename.startswith("state.failed") or basename.startswith("state.timed-out") \
                    or basename.startswith("state.killed"):
                raise Exception(f"task in {control_dir} reached {basename}, expected {expected_basename}")
            time.sleep(1)
        raise Exception(
            f"timed out waiting for {expected_basename} in {control_dir}, "
            f"last state was {self._state_basename(control_dir)}"
        )


class StopAfterStepWithSbatchTest(StopAfterStepBaseTest):

    def test_sbatch_stops_after_step(self):
        d = TestSandboxDir(self)

        _run_cli(
            self, 'prepare',
            f'-pid={d.sandbox_dir}', f'--generator={_generator}'
        )

        # run steps 0 and 1 only, then stop
        _run_cli(
            self, 'sbatch',
            f'-pid={d.sandbox_dir}', f'--generator={_generator}',
            '-k=solo', '--stop-after-step=1', '--wait'
        )

        control_dir = self._control_dir(d.sandbox_dir, "solo")
        self._wait_until_state(control_dir, "state.ready.2")
        self.assertEqual(self._out_file_content(d.sandbox_dir, "solo"), "s0\ns1\n")

        # resume with no stop, task must complete
        _run_cli(
            self, 'sbatch',
            f'-pid={d.sandbox_dir}', f'--generator={_generator}',
            '-k=solo', '--wait'
        )

        self._wait_until_state(control_dir, "state.completed")
        self.assertEqual(self._out_file_content(d.sandbox_dir, "solo"), "s0\ns1\ns2\n")


class StopAfterStepWithArraySubmitTest(StopAfterStepBaseTest):

    def test_array_submit_stops_after_step(self):
        d = TestSandboxDir(self)

        _run_cli(
            self, 'prepare',
            f'-pid={d.sandbox_dir}', f'--generator={_generator}'
        )

        # run steps 0 and 1 of every child, then stop
        _run_cli(
            self, 'array-submit',
            f'-pid={d.sandbox_dir}', f'--generator={_generator}',
            '-k=ap', '--stop-after-step=1', '--wait'
        )

        for i in range(1, 4):
            control_dir = self._control_dir(d.sandbox_dir, f"t{i}")
            self._wait_until_state(control_dir, "state.ready.2")
            self.assertEqual(self._out_file_content(d.sandbox_dir, f"t{i}"), "s0\ns1\n")

        # resume with no stop, all children must complete.
        # children left at ready.2 by --stop-after-step must be resubmitted without
        # needing --include-all-incompleted-tasks
        _run_cli(
            self, 'array-submit',
            f'-pid={d.sandbox_dir}', f'--generator={_generator}',
            '-k=ap', '--wait'
        )

        for i in range(1, 4):
            control_dir = self._control_dir(d.sandbox_dir, f"t{i}")
            self._wait_until_state(control_dir, "state.completed")
            self.assertEqual(self._out_file_content(d.sandbox_dir, f"t{i}"), "s0\ns1\ns2\n")

    def _keys_selected_for_launch(self, sandbox_dir, filter_expr, stop_after_step=None):
        """The task keys the launch commands would select for the given --filter and
        --stop-after-step. This only exercises the read-only filter_key_state_step()
        selection method, it does NOT invoke the command, so nothing is submitted."""
        args = ['sbatch', f'-pid={sandbox_dir}', f'--generator={_generator}', f'--filter={filter_expr}']
        if stop_after_step is not None:
            args.append(f'--stop-after-step={stop_after_step}')
        cli = _create_cli(self, *args)
        # filter_key_state_step uses self.logger, which is normally set by invoke()
        cli.logger = logging.getLogger("cli")
        return {key for key, _, _, _ in cli.filter_key_state_step()}

    def _array_job_files(self, sandbox_dir):
        # only the array submit markers (array.<idx>.job.<job_id>), not the
        # array.<idx>.job.<job_id>.<n>.sacct.out files written while polling sacct
        return [
            f for f in glob.glob(os.path.join(sandbox_dir, ".drypipe", "ap", "array.*.job.*"))
            if re.match(r"array\.\d+\.job\.\d+$", os.path.basename(f))
        ]

    def test_array_submit_does_not_rerun_tasks_past_stop_step(self):
        d = TestSandboxDir(self)

        _run_cli(
            self, 'prepare',
            f'-pid={d.sandbox_dir}', f'--generator={_generator}'
        )

        # run steps 0 and 1 of every child, then stop at ready.2
        _run_cli(
            self, 'array-submit',
            f'-pid={d.sandbox_dir}', f'--generator={_generator}',
            '-k=ap', '--stop-after-step=1', '--wait'
        )

        for i in range(1, 4):
            self._wait_until_state(self._control_dir(d.sandbox_dir, f"t{i}"), "state.ready.2")

        # filter_key_state_step must honor --stop-after-step, purely on the step index:
        # the children are at ready.2 (index 2)
        self.assertEqual(self._keys_selected_for_launch(d.sandbox_dir, "t*"), {"t1", "t2", "t3"})
        self.assertEqual(self._keys_selected_for_launch(d.sandbox_dir, "t*", stop_after_step=2), {"t1", "t2", "t3"})
        self.assertEqual(self._keys_selected_for_launch(d.sandbox_dir, "t*", stop_after_step=1), set())
        self.assertEqual(self._keys_selected_for_launch(d.sandbox_dir, "t*", stop_after_step=0), set())

        jobs_before = len(self._array_job_files(d.sandbox_dir))

        # children are at ready.2, past step 0, so a submit stopping after step 0 must
        # not resubmit them, and must not run any further step
        _run_cli(
            self, 'array-submit',
            f'-pid={d.sandbox_dir}', f'--generator={_generator}',
            '-k=ap', '--stop-after-step=0', '--wait'
        )

        # no new array was submitted ...
        self.assertEqual(len(self._array_job_files(d.sandbox_dir)), jobs_before)

        # ... and every child is untouched, still at ready.2 with only steps 0 and 1 run
        for i in range(1, 4):
            self.assertEqual(self._state_basename(self._control_dir(d.sandbox_dir, f"t{i}")), "state.ready.2")
            self.assertEqual(self._out_file_content(d.sandbox_dir, f"t{i}"), "s0\ns1\n")
