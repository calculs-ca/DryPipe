import sys

from unittest import TextTestRunner, TestSuite, defaultTestLoader

from base_tests import BaseTests, \
    NonTrivialPipelineLocalWithSingularityContainerTests, NonTrivialPipelineSlurmContainerlessTests, \
    NonTrivialPipelineSlurmWithSingularityContainerTests
import pipeline_tests_with_single_tasks
import pipeline_tests_with_multiple_tasks
from test_corner_case_failure_handling import CornerCasesFailureTests, CornerCasesRemoteZombiTests
from test_monitoring import MonitoringTests
from test_regressions import RegressionTests
from test_remote_tasks import RemoteTaskTests1, RemoteTaskTests2, RemoteTaskTestsWithSlurm
from test_state_machine import StateMachineTests, StateFileTrackerTest, MockupStateFileTrackerTest


def state_machine_tests():
    return [StateMachineTests, StateFileTrackerTest]


def low_level_tests():
    return [
        MockupStateFileTrackerTest,
        StateFileTrackerTest,
        StateMachineTests,
        pipeline_tests_with_single_tasks.PipelineWith3StepsNoCrash,
        pipeline_tests_with_single_tasks.PipelineWith3StepsCrash1,
        pipeline_tests_with_single_tasks.PipelineWith3StepsCrash2,
        pipeline_tests_with_single_tasks.PipelineWith3StepsCrash3,
        pipeline_tests_with_multiple_tasks.PipelineWithVariablePassing,
        pipeline_tests_with_single_tasks.PipelineWith4MixedStepsCrash,
        pipeline_tests_with_single_tasks.PipelineWithSinglePythonTask,
        pipeline_tests_with_single_tasks.PipelineWithSingleBashTask,
        pipeline_tests_with_single_tasks.PipelineWithVarAndFileOutput,
        pipeline_tests_with_single_tasks.PipelineWithVarSharingBetweenSteps,
        pipeline_tests_with_single_tasks.PipelineWith3StepsCrash3InContainer,
        pipeline_tests_with_single_tasks.PipelineWithSinglePythonTaskInContainer,
        pipeline_tests_with_single_tasks.PipelineWithSingleBashTaskInContainer,
        pipeline_tests_with_single_tasks.PipelineWithVarAndFileOutputInContainer,
        pipeline_tests_with_single_tasks.PipelineWithVarSharingBetweenStepsInContainer
    ]

def quick_sanity_tests():
    return [
        MockupStateFileTrackerTest,
        StateFileTrackerTest,
        StateMachineTests,
        pipeline_tests_with_multiple_tasks.PipelineWithVariablePassing,
        pipeline_tests_with_single_tasks.PipelineWith4MixedStepsCrash,
        pipeline_tests_with_single_tasks.PipelineWithSinglePythonTask,
        pipeline_tests_with_single_tasks.PipelineWithVarAndFileOutput,
        pipeline_tests_with_single_tasks.PipelineWithVarSharingBetweenSteps
    ]


def remote_tests():
    return [
        RemoteTaskTests1,
        RemoteTaskTests2,
        CornerCasesRemoteZombiTests,
        RemoteTaskTestsWithSlurm,
    ]


def exhaustive_1():
    return quick_sanity_tests() + [
        RegressionTests,
        MonitoringTests,
        MultipstepTaskTests,
        BaseTests,
        NonTrivialPipelineLocalWithSingularityContainerTests
    ]


def exhaustive_2():
    return exhaustive_1() + remote_tests()


def slurm_client():
    return [
        NonTrivialPipelineSlurmContainerlessTests,
        NonTrivialPipelineSlurmWithSingularityContainerTests,
        CornerCasesFailureTests
    ]


def exhaustive_3():
    return exhaustive_2() + slurm_client()


if __name__ == '__main__':

    #log_4_debug_daemon_mode()

    suite_to_test = "exhaustive_except_for_non_portable_tests"

    if len(sys.argv) >= 2:
        suite_to_test = sys.argv[1]

    suite_funcs = {
        "state_machine_tests": state_machine_tests,
        "low_level_tests": low_level_tests,
        "quick_sanity_tests": quick_sanity_tests,
        "remote_tests": remote_tests,
        "slurm_client": slurm_client,
        "exhaustive_1": exhaustive_1,
        "exhaustive_2": exhaustive_2,
        "exhaustive_3": exhaustive_3
    }

    def build_suite(test_classes):
        suite = TestSuite()
        for t in test_classes:
            test_suite = defaultTestLoader.loadTestsFromTestCase(t)
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
