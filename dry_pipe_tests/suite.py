import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from unittest import TextTestRunner, TestSuite, defaultTestLoader

from dry_pipe_tests import pipeline_tests_with_single_tasks
from dry_pipe_tests import pipeline_tests_with_multiple_tasks
from dry_pipe_tests import task_launch_tests
from dry_pipe_tests import test_core_lib
from dry_pipe_tests.cli_tests import CliArrayTests1, CliTestsPipelineWithSlurmArray, CliTestScenario2
from dry_pipe_tests.dsl_tests import TaskChangeTrackingTests
from dry_pipe_tests.pipeline_tests_with_slurm_arrays import PipelineWithSlurmArray2StepsWith2Sbatch
from dry_pipe_tests.pipeline_tests_with_slurm_mockup import all_low_level_tests_with_mockup_slurm
from dry_pipe_tests.test_state_machine import StateMachineTests, StateFileTrackerTest, MockupStateFileTrackerTest

from dry_pipe_tests.pipeline_tests_with_local_slurm import all_with_local_slurm
from dry_pipe_tests import pipeline_tests_with_slurm_arrays
from dry_pipe_tests.pipeline_tests_with_remote_slurm_arrays import \
    RemoteArrayTaskFullyAutomatedRun, RemoteArrayTaskFullyAutomatedRun2Steps2Sbatches, RemotePipelineWithAutoRestart1, \
    RemotePipelineWithAutoRestart2, PipelineWithPartialArrayMatchRemote
from dry_pipe_tests.pipeline_tests_with_remote_tasks import RemoteTestFileSet, RemoteTestFileSetWithDataDirVar, \
    RemoteTestFileSetWithGlobus


def ad_hoc():
    return [
        PipelineWithSlurmArray2StepsWith2Sbatch,
        RemotePipelineWithAutoRestart1
    ]

def local_array_tests():
    return [
        CliArrayTests1,
        CliTestsPipelineWithSlurmArray,
        CliTestScenario2,
        pipeline_tests_with_slurm_arrays.all_tests
    ]

def remote_array_tests():
    return [
        RemoteArrayTaskFullyAutomatedRun,
        RemoteArrayTaskFullyAutomatedRun2Steps2Sbatches,
        RemotePipelineWithAutoRestart1,
        RemotePipelineWithAutoRestart2,
        PipelineWithPartialArrayMatchRemote
    ]

def local_and_remote_array_tests():
    return local_array_tests() + remote_array_tests()

def remote_task_tests():
    return [
        pipeline_tests_with_single_tasks.TestFileSet,
        # ^ not remote, but useful, since it's the basis of the two following tests
        RemoteTestFileSetWithDataDirVar,
        RemoteTestFileSet
    ]

def globus_tests():
    return [
        RemoteTestFileSetWithGlobus
    ]

def all_remote_tests():
    return remote_task_tests() + remote_array_tests() + globus_tests()

def cli_tests():
    return [
        CliArrayTests1,
        CliTestsPipelineWithSlurmArray,
        CliTestScenario2,
    ]


def quick_sanity_tests():
    return [
        MockupStateFileTrackerTest,
        StateFileTrackerTest,
        StateMachineTests,
        task_launch_tests.all_launch_tests(),
        pipeline_tests_with_multiple_tasks.PipelineWithVariablePassing,
        pipeline_tests_with_single_tasks.PipelineWith4MixedStepsCrash,
        pipeline_tests_with_single_tasks.PipelineWithSinglePythonTask,
        pipeline_tests_with_single_tasks.PipelineWithVarAndFileOutput,
        all_low_level_tests_with_mockup_slurm(),
        test_core_lib.all_tests
    ]

def low_level_tests():
    return [
        MockupStateFileTrackerTest,
        StateFileTrackerTest,
        StateMachineTests,
        task_launch_tests.all_launch_tests(),
        pipeline_tests_with_single_tasks.all_tests(),
        pipeline_tests_with_multiple_tasks.all_basic_tests(),
        all_low_level_tests_with_mockup_slurm(),
        TaskChangeTrackingTests,
        CliArrayTests1,
        all_with_local_slurm(),
        test_core_lib.all_tests
    ]

def all_local_tests():
    return low_level_tests() + local_array_tests()

def exhaustive_test_suite():
    return low_level_tests() + local_array_tests() + all_remote_tests()


def _fullname(klass):
    module = klass.__module__
    if module == 'builtins':
        return klass.__qualname__  # avoid outputs like 'builtins.str'
    return module + '.' + klass.__qualname__


def flatten_test_classes(test_classes_or_list_of_test_classes):
    def g():
        for t in test_classes_or_list_of_test_classes:
            if isinstance(t, list):
                yield from t
            else:
                yield t

    # remove duplicate classes, keep first occurrence:
    d = {}
    for c in g():
        d.setdefault(_fullname(c), c)

    return list(d.values())


def run_classes_in_parallel(test_classes, max_workers=None):
    """
    Runs each test class in its own `python -m unittest` subprocess, fanned out across
    a thread pool. Test classes here are already isolated by sandbox dir (keyed by class
    name), so this is safe: the parallelism comes from separate OS processes (fresh
    interpreter, fresh logging handlers), not from threading within one process.
    """

    if max_workers is None:
        max_workers = os.cpu_count()

    classes = flatten_test_classes(test_classes)
    cwd = os.path.dirname(os.path.abspath(__file__))

    def run_one(cls):
        target = _fullname(cls)
        t0 = time.time()
        p = subprocess.run(
            [sys.executable, "-m", "unittest", target],
            capture_output=True, text=True, cwd=cwd
        )
        return target, p.returncode, time.time() - t0, p.stdout + p.stderr

    failures = []
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for target, code, dt, output in ex.map(run_one, classes):
            status = "ok" if code == 0 else "FAIL"
            print(f"{status:5} {dt:6.1f}s  {target}")
            if code != 0:
                failures.append((target, output))

    total = time.time() - t_start

    for target, output in failures:
        print(f"\n===== {target} =====\n{output}")

    print(
        f"\nRan {len(classes)} test classes in {total:.1f}s using {max_workers} workers "
        f"({'FAILED' if failures else 'OK'}, {len(failures)} failing)"
    )

    return len(failures) == 0


if __name__ == '__main__':

    #log_4_debug_daemon_mode()

    suite_to_test = "low_level_tests"

    args = [a for a in sys.argv[1:] if a != "--parallel"]
    parallel = "--parallel" in sys.argv[1:]

    if len(args) >= 1:
        suite_to_test = args[0]

    suite_funcs = {
        "low_level_tests": low_level_tests,
        "quick_sanity_tests": quick_sanity_tests,
        "task_launch_tests": task_launch_tests.all_launch_tests,
        "ad_hoc": ad_hoc,
        "local_array_tests": local_array_tests,
        "all_local_tests": all_local_tests,
        "remote_task_tests": remote_task_tests,
        "local_and_remote_array_tests": local_and_remote_array_tests,
        "exhaustive_test_suite": exhaustive_test_suite,
        "remote_array_tests": remote_array_tests,
        "all_remote_tests": all_remote_tests,
        "cli_tests": cli_tests,
        "globus_tests": globus_tests
    }

    def build_suite(test_classes):
        suite = TestSuite()
        for cls in flatten_test_classes(test_classes):
            suite.addTests(defaultTestLoader.loadTestsFromTestCase(cls))
        return suite

    chosen_suite_func = suite_funcs[suite_to_test]()

    if parallel:
        ok = run_classes_in_parallel(chosen_suite_func)
        sys.exit(0 if ok else 1)

    failfast = False

    if suite_to_test == "remote_tests":
        failfast = True

    result = TextTestRunner(verbosity=2, failfast=failfast, durations=25).run(
        build_suite(chosen_suite_func)
    )


