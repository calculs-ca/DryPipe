
import time
from pathlib import Path
from base_pipeline_test import BasePipelineTest
from dry_pipe import DryPipe, TaskConf
from dry_pipe_tests.pipeline_tests_with_single_tasks import PipelineWithSinglePythonTask, PipelineWithSingleBashTask, \
    PipelineWithVarAndFileOutput, PipelineWithVarSharingBetweenSteps, PipelineWith3StepsNoCrash, \
    PipelineWith3StepsCrash3
from dry_pipe_tests.pipeline_tests_with_slurm_mockup import PipelineWithSlurmArray

python_path_for_tests = str(Path(__file__).resolve().parent)

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


@DryPipe.python_call()
def sleep_forever():
    time.sleep(60*60*24*1000)


def task_conf_for_timeout():
    return TaskConf(
        executer_type="slurm",
        slurm_account="dummy-account",
        extra_env={
            "PYTHONPATH": python_path_for_tests,
            "DRYPIPE_TASK_DEBUG": "True",
            "DRYPIPE_SLEEP_SCHEDULE": "1"
        }
    ).with_sbatch_options(time="0:00:1")
        

# drypipe sbatch -pid dry_pipe_tests/sandboxes/to1 --generator=dry_pipe_tests.pipeline_tests_with_local_slurm:timeout_dag -k sleep

def timeout_dag(dsl):
    yield dsl.task(
        key="sleep",
        task_conf=task_conf_for_timeout()
    ).inputs(
        x=3,
        y=5
    ).outputs(
        result=int
    ).calls(
        sleep_forever
    )()

class PipelineWithForeverTaskSlurm(BasePipelineTest):

    def is_fail_test(self):
        return True
    
    def dag_gen(self, dsl):
        yield from timeout_dag(dsl)

    def is_log_level_debug(self):
        return True

    def launches_tasks_in_process(self):
        return False

    def validate(self, tasks_by_keys):

        self.assertEqual(tasks_by_keys['sleep'].state_name(), "state.time-out")
        print(tasks_by_keys['sleep'].state_name())
    

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
        return False

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
