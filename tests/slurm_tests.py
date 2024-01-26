import unittest

from dry_pipe import TaskBuilder, TaskConf
from dry_pipe.core_lib import PortablePopen, assert_slurm_supported_version


class SlurmTests(unittest.TestCase):

    def setUp(self):
        assert_slurm_supported_version()


    def test_1(self):
        print("!")
