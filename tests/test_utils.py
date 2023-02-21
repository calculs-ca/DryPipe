import inspect
import os
import pathlib
import shutil
import yaml
import logging.config
from dry_pipe import DryPipe, script_lib


def test_suite_base_dir():
    return os.path.dirname(__file__)

def file_in_test_suite(f):
    return os.path.join(test_suite_base_dir(), f)

def setup_log_conf(log_file):
    log_conf_file = os.path.join(
        os.path.dirname(__file__),
        "logging-configs",
        log_file
    )

    with open(log_conf_file) as f:
        print(f"log config: {log_conf_file}")
        config = yaml.load(f, Loader=yaml.FullLoader)
        logging.config.dictConfig(config)


def log_4_debug_daemon_mode():
    setup_log_conf("debug-daemon-mode.yml")


class TestSandboxDir:

    def __init__(self, parent):

        s = inspect.stack()

        all_sandbox_dirs = os.path.join(
            os.path.dirname(__file__),
            "sandboxes"
        )
        sandbox_dir = os.path.join(
            all_sandbox_dirs,
            f"{parent.__class__.__name__}.{s[1].function}"
        )

        self.sandbox_dir = sandbox_dir

        if os.path.exists(self.sandbox_dir):
            shutil.rmtree(self.sandbox_dir)

        pathlib.Path(self.sandbox_dir).mkdir(parents=True, exist_ok=False)

    def create_subdir(self, d):
        res = os.path.join(self.sandbox_dir, d)
        pathlib.Path(res).mkdir(parents=False, exist_ok=False)
        return res

    def pipeline_instance(self, template_pipeline):
        return template_pipeline.create_pipeline_instance(
            pipeline_instance_dir=self.sandbox_dir
        )

    def pipeline_instance_from_generator(
            self, task_generator, completed=False, env_vars=None, fail_silently=False, task_conf=None, remote_task_confs=None):
        from base_tests import test_containers_dir
        pi = DryPipe.create_pipeline(task_generator, remote_task_confs=remote_task_confs).create_pipeline_instance(
            pipeline_instance_dir=self.sandbox_dir,
            env_vars=env_vars,
            containers_dir=test_containers_dir(),
            task_conf=task_conf
        )

        copy_pre_existing_file_deps_from_code_dir(pi)

        if completed:
            pi.run_sync(fail_silently=fail_silently)

        return pi

    def init_pid_for_tests(self):
        def mkdir(d):
            d = os.path.join(self.sandbox_dir, d)
            pathlib.Path(d).mkdir(exist_ok=True)
            return d

        drypipe_dir = mkdir('.drypipe')
        mkdir('output')

        shutil.copy(script_lib.__file__, drypipe_dir)



def copy_pre_existing_file_deps_from_code_dir(pipeline_instance):

    instance_dir_for_test = pipeline_instance.pipeline_instance_dir

    src_pipeline = pipeline_instance.pipeline.create_pipeline_instance()

    file_deps = src_pipeline.pre_existing_file_deps()

    src_dir = src_pipeline.pipeline_instance_dir

    for f in file_deps:
        f = os.path.join(src_dir, f)
        if not os.path.exists(f):
            raise Exception(f"file not found {f}")
        shutil.copy(f, instance_dir_for_test)


def _copy_pre_existing_file_deps(src_pipeline, dst_pipeline_instance_dir):

    instance_dir_for_test = dst_pipeline_instance_dir

    src_pipeline = src_pipeline.pipeline.create_pipeline_instance()

    file_deps = src_pipeline.pre_existing_file_deps()

    src_dir = src_pipeline.pipeline_instance_dir

    for f in file_deps:
        f = os.path.join(src_dir, f)
        if not os.path.exists(f):
            raise Exception(f"file not found {f}")
        shutil.copy(f, instance_dir_for_test)


class A:
    pass


def zaz():
    TestSandboxDir(A())


if __name__ == '__main__':
    zaz()


def ensure_remote_dirs_dont_exist(pipeline_instance, clear_file_cache=False):

    remote_dir_for_test = pipeline_instance.instance_dir_base_name()

    for task_conf in pipeline_instance.remote_sites_task_confs():
        complete_remote_dir_for_test = f"{task_conf.remote_base_dir}/{remote_dir_for_test}"
        with task_conf.create_executer() as rex:
            rex.invoke_remote(f"rm -Rf {complete_remote_dir_for_test}")

            if clear_file_cache:
                rex.invoke_remote(f"rm -Rf {task_conf.remote_base_dir}/.file-cache")
