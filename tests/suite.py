import sys

from unittest import TextTestRunner, TestSuite, defaultTestLoader

import pipeline_tests_with_single_tasks
import pipeline_tests_with_multiple_tasks
import task_launch_tests
from test_state_machine import StateMachineTests, StateFileTrackerTest, MockupStateFileTrackerTest


def ad_hoc():
    return [
        task_launch_tests.PipelineWithVariablePassingTaskLauncherTest,
        task_launch_tests.EnsureFailOfLaunchWhenUnsatisfiedUpstreamDependencyTest,
    ]

def low_level_tests():
    return [
        MockupStateFileTrackerTest,
        StateFileTrackerTest,
        StateMachineTests,
        task_launch_tests.all_launch_tests(),
        pipeline_tests_with_single_tasks.all_tests(),
        pipeline_tests_with_multiple_tasks.all_basic_tests()
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
        pipeline_tests_with_multiple_tasks.PipelineWithTwoPythonTasks,
        pipeline_tests_with_single_tasks.PipelineWithVarAndFileOutput
    ]



if __name__ == '__main__':

    #log_4_debug_daemon_mode()

    suite_to_test = "exhaustive_except_for_non_portable_tests"

    if len(sys.argv) >= 2:
        suite_to_test = sys.argv[1]

    suite_funcs = {
        "low_level_tests": low_level_tests,
        "quick_sanity_tests": quick_sanity_tests,
        "task_launch_tests": task_launch_tests.all_launch_tests,
        "ad_hoc": ad_hoc
    }

    def gen_test_classes(test_classes_or_list_of_test_classes):
        for t in test_classes_or_list_of_test_classes:
            if isinstance(t, list):
                for t0 in t:
                    yield defaultTestLoader.loadTestsFromTestCase(t0)
            else:
                yield defaultTestLoader.loadTestsFromTestCase(t)

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
