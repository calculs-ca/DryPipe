
from dry_pipe.core_lib import UpstreamTasksNotCompleted
import pipeline_tests_with_single_tasks
import pipeline_tests_with_multiple_tasks


class BashTaskLauncherTest(pipeline_tests_with_single_tasks.PipelineWithSingleBashTask):

    def launches_tasks_in_process(self):
        return True


class PythonTaskLauncherTest(pipeline_tests_with_single_tasks.PipelineWithSinglePythonTask):

    def launches_tasks_in_process(self):
        return True



class PipelineWithVariablePassingTaskLauncherTest(pipeline_tests_with_multiple_tasks.PipelineWithVariablePassing):

    def launches_tasks_in_process(self):
        return True


class PipelineWith3StepsNoCrashTaskLauncherTest(pipeline_tests_with_single_tasks.PipelineWith3StepsNoCrash):
    def launches_tasks_in_process(self):
        return True


class EnsureFailOfLaunchWhenUnsatisfiedUpstreamDependencyTest(pipeline_tests_with_multiple_tasks.PipelineWithVariablePassing):


    def launches_tasks_in_process(self):
        return True

    def test_run_pipeline(self):

        #pi = self.run_pipeline(queue_only_pattern="*")

        pipeline_instance = self.create_pipeline_instance()

        pipeline_instance.run_sync(
            until_patterns=["*"],
            run_tasks_in_process=self.launches_tasks_in_process()
        )

        consume_and_produce_a_var = pipeline_instance.lookup_single_task(
            "consume_and_produce_a_var",
            include_incomplete_tasks=True
        )

        self.assertRaises(
            UpstreamTasksNotCompleted,
            lambda: self.launch_task_in_current_process(consume_and_produce_a_var.state_file)
        )

    def is_fail_test(self):
        return True

    def validate(self, tasks_by_keys):
        pass


# tests in containers

class BashTaskLauncherTestInContainer(pipeline_tests_with_single_tasks.PipelineWithSingleBashTaskInContainer):

    def launches_tasks_in_process(self):
        return True


class PythonTaskLauncherTestInContainer(pipeline_tests_with_single_tasks.PipelineWithSinglePythonTaskInContainer):

    def launches_tasks_in_process(self):
        return True


def all_launch_tests():
    return [
        BashTaskLauncherTest,
        PythonTaskLauncherTest,
        PipelineWithVariablePassingTaskLauncherTest,
        EnsureFailOfLaunchWhenUnsatisfiedUpstreamDependencyTest,
        BashTaskLauncherTestInContainer,
        PythonTaskLauncherTestInContainer
    ]
