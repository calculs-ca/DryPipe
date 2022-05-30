import inspect
import sys
from unittest import TextTestRunner, TestSuite, defaultTestLoader
from suite import exhaustive_3


test_func = "test_state_history_tracking"



if __name__ == '__main__':

    def search():

        all_classes = exhaustive_3()

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
