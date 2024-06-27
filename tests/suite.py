import sys

from unittest import TextTestRunner, TestSuite, defaultTestLoader

import pipeline_tests_with_single_tasks
import pipeline_tests_with_multiple_tasks
import task_launch_tests
from cli_tests import CliArrayTests1, CliTestsPipelineWithSlurmArray, CliTestScenario2, \
    CliTestsPipelineWithSlurmArrayRemote
from dsl_tests import TaskChangeTrackingTests
from pipeline_tests_with_slurm_mockup import all_low_level_tests_with_mockup_slurm
from test_state_machine import StateMachineTests, StateFileTrackerTest, MockupStateFileTrackerTest
from tests.pipeline_tests_with_local_slurm import all_with_local_slurm
from tests import pipeline_tests_with_slurm_arrays


def ad_hoc():
    return all_low_level_tests_with_mockup_slurm()


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
        all_with_local_slurm()
    ]


def array_tests():
    return [
        CliArrayTests1,
        CliTestsPipelineWithSlurmArray,
        CliTestsPipelineWithSlurmArrayRemote,
        CliTestScenario2,
        pipeline_tests_with_slurm_arrays.all_tests
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
        all_low_level_tests_with_mockup_slurm()
    ]


def exhaustive_test_suite():
    return quick_sanity_tests() + array_tests() + low_level_tests()


if __name__ == '__main__':

    #log_4_debug_daemon_mode()

    suite_to_test = "exhaustive_except_for_non_portable_tests"

    if len(sys.argv) >= 2:
        suite_to_test = sys.argv[1]

    suite_funcs = {
        "low_level_tests": low_level_tests,
        "quick_sanity_tests": quick_sanity_tests,
        "task_launch_tests": task_launch_tests.all_launch_tests,
        "ad_hoc": ad_hoc,
        "array_tests": array_tests,
        "exhaustive_test_suite": exhaustive_test_suite
    }

    def gen_test_classes(test_classes_or_list_of_test_classes):

        def fullname(klass):
            module = klass.__module__
            if module == 'builtins':
                return klass.__qualname__  # avoid outputs like 'builtins.str'
            return module + '.' + klass.__qualname__

        def g():
            for t in test_classes_or_list_of_test_classes:
                if isinstance(t, list):
                    for t0 in t:
                        yield fullname(t0), defaultTestLoader.loadTestsFromTestCase(t0)
                else:
                    yield fullname(t), defaultTestLoader.loadTestsFromTestCase(t)

        # remove duplicate classes:
        d = {
            qn: t for qn, t in g()
        }

        yield from d.values()

    def build_suite(test_classes):
        suite = TestSuite()
        for test_suite in gen_test_classes(test_classes):
            suite.addTests(test_suite)
        return suite

    chosen_suite_func = suite_funcs[suite_to_test]()

    failfast = False

    if suite_to_test == "remote_tests":
        failfast = True

    result = TextTestRunner(verbosity=2, failfast=failfast).run(
        build_suite(chosen_suite_func)
    )

    sys.exit(not result.wasSuccessful())
