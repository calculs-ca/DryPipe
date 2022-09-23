import inspect
import sys
from unittest import TextTestRunner, TestSuite, defaultTestLoader
from suite import exhaustive_3, low_level_tests

test_func = "test_pipeline_with_mixed_python_bash"
test_func = "test_single_bash_task_pipeline"
test_func = "test_single_bash_task_pipeline_with_container"
test_func = "test_non_trivial_local_containerless"

test_func = "test_transitions_of_tasks_with_failures_02_with_snippet"
test_func = "test_transitions_of_tasks_with_failures_02_no_snippet"

if __name__ == '__main__':

    def search():

        all_classes = exhaustive_3() + low_level_tests()

        for c in all_classes:
            #print(f"{c.__name__}")
            for name, z in inspect.getmembers(c):
                if name.startswith("test_") and name == test_func:
                    return f"{z.__module__}.{c.__name__}.{name}"


    suite = TestSuite()

    n = search()

    if n is None:
        raise Exception(f"{test_func} not found !")

    test_suite = defaultTestLoader.loadTestsFromName(n)
    suite.addTests(test_suite)
    result = TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
