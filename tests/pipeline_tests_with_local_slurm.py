
from pathlib import Path
from dry_pipe import TaskConf
from tests.pipeline_tests_with_single_tasks import PipelineWithSinglePythonTask
from tests.pipeline_tests_with_slurm_mockup import PipelineWithSlurmArray

task_conf_with_local_slurm = TaskConf(
    executer_type="slurm",
    slurm_account="dummy-account",
    extra_env={
        "PYTHONPATH": str(Path(__file__).resolve().parent),
        "DRYPIPE_TASK_DEBUG": "True"
    }
)

class PipelineWithSinglePythonTaskWithSlurm(PipelineWithSinglePythonTask):

    def task_conf(self):
        return task_conf_with_local_slurm



class TestArrayTaskWithLocalSlurm(PipelineWithSlurmArray):

    def launches_tasks_in_process(self):
        return False

    def task_conf(self):
        return task_conf_with_local_slurm



def all_with_local_slurm():
    return [
        PipelineWithSinglePythonTaskWithSlurm,
        TestArrayTaskWithLocalSlurm
    ]
