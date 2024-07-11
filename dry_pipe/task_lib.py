import glob
import json
import os
import shutil
from tempfile import TemporaryDirectory

from dry_pipe import DryPipe
from dry_pipe.core_lib import invoke_rsync, exec_remote
from dry_pipe.state_file_tracker import StateFileTracker


@DryPipe.python_call()
def run_array(__task_process):
    from dry_pipe.slurm_array_task import SlurmArrayParentTask

    SlurmArrayParentTask(__task_process).run_array(False, False, None)


@DryPipe.python_call()
def upload_array(
    __task_key,
    __task_control_dir,
    __user_at_host,
    __remote_base_dir,
    __ssh_key_file,
    __task_logger,
    __children_task_keys,
    __pipeline_work_dir,
    __pipeline_instance_dir,
    __remote_pipeline_work_dir,
    __task_conf
):
    from dry_pipe.task_process import TaskProcess

    def dump_unique_files_in_file(files, dep_file):
        uniq_files = set()
        with open(dep_file, "w") as tf:
            for dep_file in files:
                if dep_file not in uniq_files:
                    tf.write(dep_file)
                    tf.write("\n")
                    uniq_files.add(dep_file)

    internal_dep_file_txt = os.path.join(__task_control_dir, "deps.txt")
    external_dep_file_txt = os.path.join(__task_control_dir, "external-deps.txt")
    external_file_deps = []

    def gen_inner_pipeline_file_deps():

        def gen_internal_file_deps():
            def _gen_0():
                for child_task_key in __children_task_keys:

                    p = TaskProcess(
                        os.path.join(__pipeline_work_dir, child_task_key),
                        ensure_all_upstream_deps_complete=True
                    )

                    for _, file in p.inputs.rsync_file_list_produced_upstream():
                        yield file

                    for file in p.inputs.rsync_output_var_file_list_produced_upstream():
                        yield file

                    for _, file in p.inputs.rsync_external_file_list():
                        external_file_deps.append(file)

                    yield f".drypipe/{child_task_key}/state.ready"
                    yield f".drypipe/{child_task_key}/task-conf.json"

                    for step in p.task_conf.step_invocations:
                        if step["call"] == "bash":
                            _, script = step["script"].rsplit("/", 1)
                            yield f".drypipe/{child_task_key}/{script}"

                yield ".drypipe/cli"

                for py_file in glob.glob(os.path.join(os.path.dirname(__file__), "*.py")):
                    yield f".drypipe/dry_pipe/{os.path.basename(py_file)}"

                yield f".drypipe/{__task_key}/task-conf.json"
                yield f".drypipe/{__task_key}/task-keys.tsv"
                #yield f".drypipe/{__task_key}/state.ready"

            pipeline_instance_name = os.path.basename(__pipeline_instance_dir)
            for f in _gen_0():
                yield f"{pipeline_instance_name}/{f}"

        __task_logger.info("will generate file list for upload")

        dump_unique_files_in_file(gen_internal_file_deps(), internal_dep_file_txt)

        __task_logger.info("done")

    def gen_external_file_deps():
        if len(external_file_deps) == 0:
            __task_logger.info("no external file deps")
        else:
            __task_logger.info("will generate external file deps")

        dump_unique_files_in_file(external_file_deps, external_dep_file_txt)

    # gen all deps files before rsync
    gen_inner_pipeline_file_deps()

    gen_external_file_deps()

    ssh_remote_dest = f"{__user_at_host}:{__remote_base_dir}"

    pid = os.path.abspath(os.path.dirname(__pipeline_instance_dir))

    pid_base_name = os.path.basename(__pipeline_instance_dir)

    remote_pid = os.path.join(__remote_base_dir, pid_base_name)

    if __task_conf.run_as_group is None:
        rsync_chown_arg = ""
    else:
        user = __user_at_host.split("@")[0].strip()
        rsync_chown_arg = f"--chown={user}:{__task_conf.run_as_group}"

    def do_rsync(src, dst, deps_file):
        rsync_cmd = f"rsync {rsync_chown_arg} --mkpath -a --dirs --files-from={deps_file} {src}/ {dst}/"
        __task_logger.info("%s", rsync_cmd)
        invoke_rsync(rsync_cmd)

    overrides_basename = "task-conf-overrides.json"
    def gen_task_conf_remote_overrides():

        with TemporaryDirectory(dir=__task_control_dir) as tmp_dir:
            overrides_file = os.path.join(tmp_dir, overrides_basename)
            with open(overrides_file, "w") as tmp_overrides:
                tmp_overrides.write(json.dumps(
                    {
                        "is_on_remote_site": True,
                        "external_files_root": f"{remote_pid}/external-file-deps"
                    },
                    indent=2
                ))

            try:
                # make a copy, just for transparency (self documenting)
                shutil.copy(
                    overrides_file,
                    os.path.join(__task_control_dir, f"task-conf-overrides-{__user_at_host}.json")
                )
                dst = f"{__user_at_host}:{remote_pid}/.drypipe/{__task_key}/"
                invoke_rsync(f"rsync {rsync_chown_arg} --mkpath {overrides_file} {dst}")
            finally:
                if os.path.exists(overrides_file):
                    os.remove(overrides_file)

    gen_task_conf_remote_overrides()

    do_rsync(pid, ssh_remote_dest, internal_dep_file_txt)

    do_rsync("", f"{ssh_remote_dest}/{pid_base_name}/external-file-deps", external_dep_file_txt)


@DryPipe.python_call()
def download_array(
    __task_key,
    __task_control_dir,
    __user_at_host,
    __remote_base_dir,
    __ssh_key_file,
    __task_logger,
    __children_task_keys,
    __pipeline_work_dir,
    __pipeline_instance_dir,
    __remote_pipeline_work_dir
):
    from dry_pipe.task_process import TaskProcess

    def gen_result_files():
        for child_task_key in __children_task_keys:
            p = TaskProcess(
                os.path.join(__pipeline_work_dir, child_task_key),
                ensure_all_upstream_deps_complete=False
            )
            for file in p.outputs.rsync_file_list():
                yield file

    result_file_txt = os.path.join(__task_control_dir, "result-files.txt")
    uniq_files = set()
    with open(result_file_txt, "w") as tf:
        for result_file in gen_result_files():
            if result_file not in uniq_files:
                tf.write(result_file)
                tf.write("\n")
                uniq_files.add(result_file)

    pid = __pipeline_instance_dir

    pipeline_base_name = os.path.basename(pid)

    ssh_remote_dest = \
        f"{__user_at_host}:{__remote_base_dir}/{pipeline_base_name}/"

    rsync_cmd = f"rsync -a --dirs --files-from={result_file_txt} {ssh_remote_dest} {pid}/"

    __task_logger.debug("%s", rsync_cmd)
    invoke_rsync(rsync_cmd)

    remote_cli = os.path.join(__remote_pipeline_work_dir, "cli")

    remote_exec_result = exec_remote(__user_at_host, [
        remote_cli,
        "list-array-states",
        f"--task-key={__task_key}"
    ])

    __task_logger.debug("remote states:\n %s", remote_exec_result)

    for child_task_key_task_state in remote_exec_result.split("\n"):

        child_task_key_task_state = child_task_key_task_state.strip()

        if child_task_key_task_state == "":
            continue

        child_task_key, child_task_state = child_task_key_task_state.split("/")

        child_task_control_dir = os.path.join(__pipeline_work_dir, child_task_key)

        child_state_file_path = StateFileTracker.find_state_file_if_exists(child_task_control_dir)

        if child_state_file_path is not None:
            actual_state = os.path.join(child_task_control_dir, child_task_state)
            os.rename(child_state_file_path.path, actual_state)


@DryPipe.python_call()
def execute_remote_task(
    __task_key,
    __user_at_host,
    __remote_base_dir,
    __ssh_key_file,
    __pipeline_instance_name,
    __remote_pipeline_work_dir,
    __task_conf,
    __task_logger
):

    remote_cli = os.path.join(__remote_pipeline_work_dir, "cli")
    remote_task_control_dir = os.path.join(__remote_pipeline_work_dir, __task_key)

    cmd = [
        remote_cli, "task", remote_task_control_dir, "--from-remote"
    ]

    if __task_conf.run_as_group is not None:
        __task_logger.info("remote execution at %s, as group %s ", __user_at_host,  __task_conf.run_as_group)
        cmd = " ".join(cmd)
        cmd = [
            "newgrp", __task_conf.run_as_group, "<<<", f"'{cmd}'"
        ]
    else:
        __task_logger.info("remote execution at %s")


    exec_remote(__user_at_host, cmd)
