
from pathlib import Path
from dry_pipe import TaskConf
from dry_pipe_tests.pipeline_tests_with_single_tasks import PipelineWithSinglePythonTask, PipelineWithSingleBashTask, \
    PipelineWithVarAndFileOutput, PipelineWithVarSharingBetweenSteps, PipelineWith3StepsNoCrash, \
    PipelineWith3StepsCrash3
from dry_pipe_tests.pipeline_tests_with_slurm_mockup import PipelineWithSlurmArray

python_path_for_tests = str(Path(__file__).resolve().parent.parent)

def task_conf_with_local_slurm(test):
    return TaskConf(
        executer_type="slurm",
        slurm_account="dummy-account",
        extra_env={
            "PYTHONPATH": python_path_for_tests,
            "DRYPIPE_TASK_DEBUG": test.is_log_level_debug().__str__(),
            "DRYPIPE_SLEEP_SCHEDULE": test.custom_sleep_schedule()
        }
    )

def task_conf_with_test_container_in_local_slurm(test):
    return TaskConf(
        executer_type="slurm",
        slurm_account="dummy-account",
        container="singularity-test-container.sif",
        python_bin="python3",
        extra_env={
            "PYTHONPATH": python_path_for_tests,
            "DRYPIPE_TASK_DEBUG": test.is_log_level_debug().__str__(),
            "DRYPIPE_SLEEP_SCHEDULE": test.custom_sleep_schedule()
        }
    )

def task_conf_with_test_container_in_local_slurm_crash(test):
    return TaskConf(
            executer_type="slurm",
            slurm_account="dummy-account",
            extra_env={
                "PYTHONPATH": python_path_for_tests,
                "CRASH_STEP_3": "TRUE",
                "DRYPIPE_TASK_DEBUG": test.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": test.custom_sleep_schedule()
            }
    )

def task_conf_with_test_container_in_local_slurm_in_container_crash(test):
    return TaskConf(
            executer_type="slurm",
            slurm_account="dummy-account",
            container="singularity-test-container.sif",
            extra_env={
                "PYTHONPATH": python_path_for_tests,
                "CRASH_STEP_3": "TRUE",
                "DRYPIPE_TASK_DEBUG": test.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": test.custom_sleep_schedule()
            }
    )


class PipelineWithSinglePythonTaskWithSlurm(PipelineWithSinglePythonTask):

    def task_conf(self):
        return task_conf_with_local_slurm(self)

    def is_log_level_debug(self):
        return True

    def launches_tasks_in_process(self):
        return False

class PipelineWithSingleBashTaskWithSlurm(PipelineWithSingleBashTask):

    def task_conf(self):
        return task_conf_with_local_slurm(self)


class PipelineWithVarAndFileOutputLocalSlurm(PipelineWithVarAndFileOutput):
    def task_conf(self):
        return task_conf_with_local_slurm(self)

class PipelineWithVarAndFileOutputLocalSlurmInContainer(PipelineWithVarAndFileOutput):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm(self)

class PipelineWithVarSharingBetweenStepsLocalSlurm(PipelineWithVarSharingBetweenSteps):
    def task_conf(self):
        return task_conf_with_local_slurm(self)

class PipelineWithVarSharingBetweenStepsLocalSlurmInContainer(PipelineWithVarSharingBetweenSteps):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm(self)
    def is_log_level_debug(self):
        return True


class PipelineWith3StepsNoCrashSlurm(PipelineWith3StepsNoCrash):
    def task_conf(self):
        return task_conf_with_local_slurm(self)


class PipelineWith3StepsNoCrashSlurmInContainer(PipelineWith3StepsNoCrash):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm(self)


class PipelineWith3StepsCrash3LocalSlurm(PipelineWith3StepsCrash3):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm_crash(self)

class PipelineWith3StepsCrash3LocalSlurmInContainer(PipelineWith3StepsCrash3):
    def task_conf(self):
        return task_conf_with_test_container_in_local_slurm_in_container_crash(self)
    def is_log_level_debug(self):
        return True



class TestArrayTaskWithLocalSlurm(PipelineWithSlurmArray):

    def launches_tasks_in_process(self):
        return True

    def task_conf(self):
        return task_conf_with_local_slurm(self)



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
