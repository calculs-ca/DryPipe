import sys

from unittest import TextTestRunner, TestSuite, defaultTestLoader

from aggregate_task_tests import AggregateTaskTests
from base_tests import BaseTests, NonTrivialPipelineTests, NonTrivialPipelineLocalContainerlessTests, \
    NonTrivialPipelineLocalWithSingularityContainerTests, NonTrivialPipelineSlurmContainerlessTests, \
    NonTrivialPipelineSlurmWithSingularityContainerTests
from ground_level_tests import GroundLevelTests, TaskSignatureTests
from test_bash_funcs import BaseFuncTests
from test_corner_case_failure_handling import CornerCasesFailureTests
from test_daemon_mode import DaemonModeTests
from test_in_out_hashing import InOutHashingTests
from test_monitoring import MonitoringTests
from test_multistep_tasks import MultipstepTaskTests
from test_pipeline_composition import PipelineCompositionTests
from test_regressions import RegressionTests
from test_remote_tasks import RemoteTaskTests1, RemoteTaskTests2, RemoteTaskTestsWithSlurm
from test_utils import log_4_debug_daemon_mode


def low_level_tests():
    return [
        BaseFuncTests,
        NonTrivialPipelineTests,
        NonTrivialPipelineLocalContainerlessTests,
        TaskSignatureTests,
        GroundLevelTests,
        AggregateTaskTests,
        InOutHashingTests,
        PipelineCompositionTests
    ]

def quick_sanity_tests():
    return [
        DaemonModeTests,
        NonTrivialPipelineTests,
        NonTrivialPipelineLocalContainerlessTests,
        TaskSignatureTests,
        GroundLevelTests,
        BaseFuncTests,
        AggregateTaskTests,
        InOutHashingTests,
        PipelineCompositionTests
    ]


def remote_tests():
    return [
        RemoteTaskTests1,
        RemoteTaskTests2,
        RemoteTaskTestsWithSlurm
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
        "low_level_tests": low_level_tests,
        "quick_sanity_tests": quick_sanity_tests,
        "remote_tests": remote_tests,
        "slurm_client": slurm_client,
        "exhaustive_1": exhaustive_1,
        "exhaustive_2": exhaustive_3,
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
