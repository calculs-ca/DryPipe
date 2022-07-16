import inspect
import os
import pathlib
import shutil
import yaml
import logging.config
from dry_pipe import DryPipe

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

    def pipeline_instance(self, template_pipeline):
        return template_pipeline.create_pipeline_instance(
            pipeline_instance_dir=self.sandbox_dir
        )

    def pipeline_instance_from_generator(self, task_generator, completed=False, env_vars=None, fail_silently=False):
        from base_tests import test_containers_dir
        pi = DryPipe.create_pipeline(task_generator).create_pipeline_instance(
            pipeline_instance_dir=self.sandbox_dir,
            env_vars=env_vars,
            containers_dir=test_containers_dir()
        )

        copy_pre_existing_file_deps_from_code_dir(pi)

        if completed:
            pi.run_sync(fail_silently=fail_silently)

        return pi


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
