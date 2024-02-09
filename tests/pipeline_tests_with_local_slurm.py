
from pathlib import Path
from dry_pipe import TaskConf
from tests.pipeline_tests_with_single_tasks import PipelineWithSinglePythonTask, PipelineWithSingleBashTask, \
    PipelineWithVarAndFileOutput, PipelineWithVarSharingBetweenSteps, PipelineWith3StepsNoCrash, \
    PipelineWith3StepsCrash3
from tests.pipeline_tests_with_slurm_mockup import PipelineWithSlurmArray

python_path_for_tests = str(Path(__file__).resolve().parent)

task_conf_with_local_slurm = TaskConf(
    executer_type="slurm",
    slurm_account="dummy-account",
    extra_env={
        "PYTHONPATH": python_path_for_tests,
        "DRYPIPE_TASK_DEBUG": "True"
    }
)

task_conf_with_test_container_in_local_slurm = TaskConf(
    executer_type="slurm",
    slurm_account="dummy-account",
    container="singularity-test-container.sif",
    extra_env={
        "PYTHONPATH": python_path_for_tests,
        "DRYPIPE_TASK_DEBUG": "True"
    }
)

task_conf_with_test_container_in_local_slurm_crash = TaskConf(
        executer_type="slurm",
        slurm_account="dummy-account",
        extra_env={
            "PYTHONPATH": python_path_for_tests,
            "CRASH_STEP_3": "TRUE"
        }
)

task_conf_with_test_container_in_local_slurm_in_container_crash = TaskConf(
        executer_type="slurm",
        slurm_account="dummy-account",
        container="singularity-test-container.sif",
        extra_env={
            "PYTHONPATH": python_path_for_tests,
            "CRASH_STEP_3": "TRUE"
        }
)


class PipelineWithSinglePythonTaskWithSlurm(PipelineWithSinglePythonTask):

    def task_conf(self):
        return task_conf_with_local_slurm


class PipelineWithSingleBashTaskWithSlurm(PipelineWithSingleBashTask):

    def task_conf(self):
        return task_conf_with_local_slurm


class PipelineWithVarAndFileOutputLocalSlurm(PipelineWithVarAndFileOutput):
    def task_conf(self):
        return task_conf_with_local_slurm

class PipelineWithVarAndFileOutputLocalSlurmInContainer(PipelineWithVarAndFileOutput):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm

class PipelineWithVarSharingBetweenStepsLocalSlurm(PipelineWithVarSharingBetweenSteps):
    def task_conf(self):
        return task_conf_with_local_slurm

class PipelineWithVarSharingBetweenStepsLocalSlurmInContainer(PipelineWithVarSharingBetweenSteps):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm


class PipelineWith3StepsNoCrashSlurm(PipelineWith3StepsNoCrash):
    def task_conf(self):
        return  task_conf_with_local_slurm


class PipelineWith3StepsNoCrashSlurmInContainer(PipelineWith3StepsNoCrash):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm


class PipelineWith3StepsCrash3LocalSlurm(PipelineWith3StepsCrash3):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm_crash

class PipelineWith3StepsCrash3LocalSlurmInContainer(PipelineWith3StepsCrash3):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm_in_container_crash



class TestArrayTaskWithLocalSlurm(PipelineWithSlurmArray):

    def launches_tasks_in_process(self):
        return False

    def task_conf(self):
        return task_conf_with_local_slurm



def all_with_local_slurm():
    return [
        PipelineWithSinglePythonTaskWithSlurm,
        TestArrayTaskWithLocalSlurm,
        PipelineWithSingleBashTaskWithSlurm,
        PipelineWithVarAndFileOutputLocalSlurm,
        PipelineWithVarAndFileOutputLocalSlurmInContainer,
        PipelineWithVarSharingBetweenStepsLocalSlurm,
        PipelineWithVarSharingBetweenStepsLocalSlurmInContainer,
        PipelineWith3StepsNoCrashSlurm,
        PipelineWith3StepsNoCrashSlurmInContainer,
        PipelineWith3StepsCrash3LocalSlurm,
        PipelineWith3StepsCrash3LocalSlurmInContainer
    ]
