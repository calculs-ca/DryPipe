import json
import os
import shutil
import unittest
from pathlib import Path

from dry_pipe.core_lib import SleepySpinner, PortablePopen, invoke_rsync
from dry_pipe import TaskConf
from dry_pipe.pipeline import Pipeline


class TestWithDirectorySandbox(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        all_sandbox_dirs = os.path.join(
            os.path.dirname(__file__),
            "sandboxes"
        )
        self.pipeline_code_dir = os.path.dirname(__file__)
        self.pipeline_instance_dir = os.path.join(all_sandbox_dirs, self.__class__.__name__)
        self.pipeline_instance = None

    def is_log_level_debug(self):
        return False

    def setUp(self):

        d = Path(self.pipeline_instance_dir)
        if d.exists():
            shutil.rmtree(d)



class BasePipelineTest(TestWithDirectorySandbox):

    def assert_file_content_equals(self, file, expected_content):
        with open(file) as f:
            self.assertEqual(f.read().strip(), expected_content)

    def create_monitor(self):
        return None

    def init_pipeline_instance(self, pipeline_instance):
        pass

    def custom_sleep_schedule(self):
        return "1"

    def custom_sleep_schedule_parsed(self):
        return [int(s) for s in self.custom_sleep_schedule().split(",")]

    def spin_until_no_running_jobs(self, sleep_schedule=[0]):
        with SleepySpinner(sleep_schedule) as ss:
            while True:
                with PortablePopen("squeue --noheader -o %j", shell=True) as p:
                    p.wait_and_raise_if_non_zero()
                    res = p.stdout_as_string().strip()
                    if res == "":
                        return
                    ss.sleep()

    def create_pipeline_instance(self, other_pipeline_instance_dir=None):
        pipeline = Pipeline(lambda dsl: self.dag_gen(dsl), pipeline_code_dir=self.pipeline_code_dir)
        if other_pipeline_instance_dir is not None:
            pipeline_instance = pipeline.create_pipeline_instance(
                other_pipeline_instance_dir, instance_log_is_debug=self.is_log_level_debug()
            )
        else:
            pipeline_instance = pipeline.create_pipeline_instance(
                self.pipeline_instance_dir, instance_log_is_debug=self.is_log_level_debug()
            )

        self.init_pipeline_instance(pipeline_instance)

        return pipeline_instance

    def run_pipeline(self, until_patterns=None):

        pipeline_instance = self.create_pipeline_instance()
        self.pipeline_instance = pipeline_instance
        pipeline_instance.monitor=self.create_monitor()

        pipeline_instance.run_sync(
            until_patterns=until_patterns,
            run_tasks_in_process=self.launches_tasks_in_process(),
            sleep_schedule=self.custom_sleep_schedule_parsed()
        )

        tasks_by_keys = {
            t.key: t
            for t in pipeline_instance.query("*", include_incomplete_tasks=self.is_fail_test())
        }

        self.validate(tasks_by_keys)

        return pipeline_instance


    def test_run_pipeline(self):
        self.run_pipeline()

    def task_conf(self):

        if self.remote_test_site() is not None:
            return self.task_conf_for_remote_tests()
        else:
            tc = TaskConf.default()
            tc.extra_env = {
                "PYTHONPATH": Path(__file__).parent.parent.__str__(),
                "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            }
            return tc

    def task_conf_for_remote_tests(self):
        rts = self.remote_test_site()
        tc = TaskConf(
            executer_type="slurm",
            ssh_remote_dest=rts.ssh_remote_dst(),
            sbatch_options=rts.sbatch_options,
            extra_env={
                "PYTHONPATH": self.python_path_for_remote_site(),
                "DRYPIPE_TASK_DEBUG": self.is_log_level_debug().__str__(),
                "DRYPIPE_SLEEP_SCHEDULE": self.custom_sleep_schedule()
            }
        )
        tc.python_bin = None
        return tc

    def launches_tasks_in_process(self):
        return False

    def is_fail_test(self):
        return False

    def dag_gen(self, dsl):
        raise NotImplementedError()

    def validate(self, tasks_by_keys):
        raise NotImplementedError()


    def save_crash_plan(self, crash_plan):
        with open(Path(self.pipeline_instance_dir, "crash-plan.json"), "w") as f:
            f.write(json.dumps(crash_plan))

        for f in Path(self.pipeline_instance_dir).glob("crash_count*"):
            f.unlink()

        rs = self.remote_test_site()
        if rs is not None:
            pid = self.pipeline_instance_dir
            invoke_rsync(f"rsync {pid}/crash-plan.json {rs.user_at_host()}:{self.remote_pid()}/")

    def python_path_for_remote_site(self):
        repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        return ":".join([
            f"{self.remote_pid()}/.drypipe",
            f"{self.remote_pid()}/external-file-deps{repo_dir}"
        ])

    def pre_run(self, pipeline_instance_dir):
        self.pipeline_instance_dir = pipeline_instance_dir
        if self.remote_test_site() is not None:
            self.reset_and_prepare_remote_dir()

    def create_prepare_and_run_pipeline(self, pid, until_patterns=["*"]):
        pipeline_instance = self.create_pipeline_instance(pid)
        pipeline_instance.run_sync(until_patterns, sleep_schedule=self.custom_sleep_schedule_parsed())
        return pipeline_instance

    def remote_test_site(self):
        return None

    def remote_pid(self):
        return Path(
            self.remote_test_site().remote_base_dir(),
            Path(self.pipeline_instance_dir).name
        ).__str__()

    def reset_and_prepare_remote_dir(self):
        pipeline_instance = self.create_pipeline_instance()
        remote_test_site = self.remote_test_site()
        remote_pid = self.remote_pid()
        remote_test_site.exec_remote(["rm", "-Rf", remote_pid])
        remote_test_site.exec_remote(["mkdir", "-p", remote_pid])

        return pipeline_instance
