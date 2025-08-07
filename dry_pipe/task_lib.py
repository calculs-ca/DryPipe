import os
from dry_pipe import DryPipe
from dry_pipe.core_lib import invoke_rsync, exec_remote
from dry_pipe.state_file_tracker import StateFileTracker


@DryPipe.python_call()
def run_array(__task_process):
    from dry_pipe.slurm_array_task import SlurmArrayParentTask

    SlurmArrayParentTask(__task_process).run_array(False, False, None)


@DryPipe.python_call()
def upload_task_inputs(
    __task_key,
    __task_control_dir,
    __remote_pipeline_specs,
    __task_logger,
    __task_process,
    __pipeline_work_dir,
    __pipeline_instance_dir,
    __task_conf
):

    external_file_deps = []

    __task_logger.info("will generate file list for upload")

    internal_dep_file_txt = __remote_pipeline_specs.dump_unique_files_in_file(
        __task_process.gen_internal_file_deps(external_file_deps),
        "deps.txt"
    )


    if len(external_file_deps) == 0:
        __task_logger.info("no external file deps")
    else:
        __task_logger.info("will generate external file deps")
        external_dep_file_txt = __remote_pipeline_specs.dump_unique_files_in_file(external_file_deps, "external-deps.txt")


    def do_rsync(src, dst, deps_file):
        rsync_cmd = f"rsync {__remote_pipeline_specs.rsync_chown_arg} --mkpath -a --dirs --files-from={deps_file} {src}/ {dst}/"
        __task_logger.info("%s", rsync_cmd)
        invoke_rsync(rsync_cmd)


    def rsync_upload(overrides_file, dst):
        invoke_rsync(f"rsync {__remote_pipeline_specs.rsync_chown_arg} --mkpath {overrides_file} {dst}")

    __remote_pipeline_specs.gen_and_upload_task_conf_remote_overrides(rsync_upload)

    do_rsync(
        __remote_pipeline_specs.absolute_pid,
        f"{__remote_pipeline_specs.ssh_remote_dest}/{__remote_pipeline_specs.pid_base_name}",
        internal_dep_file_txt
    )

    if len(external_file_deps) > 0:
        do_rsync("", f"{__remote_pipeline_specs.ssh_remote_dest}/{__remote_pipeline_specs.pid_base_name}/external-file-deps", external_dep_file_txt)


@DryPipe.python_call()
def download_task_outputs(
    __task_key,
    __task_control_dir,
    __task_logger,
    __task_process,
    __pipeline_work_dir,
    __pipeline_instance_dir,
    __remote_pipeline_specs
):
    from dry_pipe.task_process import TaskProcess

    #fetch states, and generate rsync list
    remote_cli = os.path.join(__remote_pipeline_specs.remote_instance_work_dir, "cli")

    remote_exec_result = exec_remote(__remote_pipeline_specs.user_at_host, [
        "python3",
        remote_cli,
        "list-states",
        f"--task-key={__task_key}"
    ])

    __task_logger.debug("remote states:\n %s", remote_exec_result)

    if __task_process.is_slurm_array_parent():
        for child_task_key_task_state in remote_exec_result.split("\n"):
            child_task_key_task_state = child_task_key_task_state.strip()
            if child_task_key_task_state == "":
                continue
            if child_task_key_task_state.startswith("implicit") and "=" in child_task_key_task_state:
                continue
            child_task_key, child_task_state = child_task_key_task_state.split("/")
            child_task_control_dir = os.path.join(__pipeline_work_dir, child_task_key)
            child_state_file_path = StateFileTracker.find_state_file_if_exists(child_task_control_dir)
            if child_state_file_path is not None:
                actual_state = os.path.join(child_task_control_dir, child_task_state)
                os.rename(child_state_file_path.path, actual_state)

    def gen_result_files():

        def g(task_key):
            p = TaskProcess(
                os.path.join(__pipeline_work_dir, task_key),
                ensure_all_upstream_deps_complete=False
            )
            for file in p.outputs.rsync_file_list():
                yield file

        if __task_process.is_slurm_array_parent():
            for child_task_key in __task_process.children_task_keys():
                yield from g(child_task_key)
        else:
            yield from g(__task_key)

        yield f".drypipe/{__task_key}/file-sets-rsync-list.txt"

    result_file_txt = __remote_pipeline_specs.dump_unique_files_in_file(gen_result_files(), "result-files.txt")

    pid = __pipeline_instance_dir

    pipeline_base_name = os.path.basename(pid)

    ssh_remote_dest = \
        f"{__remote_pipeline_specs.user_at_host}:{__remote_pipeline_specs.remote_base_dir}/{pipeline_base_name}/"

    rsync_cmd = f"rsync -a --dirs --partial --ignore-missing-args --files-from={result_file_txt} {ssh_remote_dest} {pid}/"
    __task_logger.debug("rsync file list: %s", rsync_cmd)
    invoke_rsync(rsync_cmd)

    file_set_list = __task_process.file_sets_rsync_list_file()

    if os.path.exists(file_set_list) and os.stat(file_set_list).st_size > 0:
        rsync_cmd = f"rsync -a --dirs --partial --files-from={file_set_list} {ssh_remote_dest} {pid}/"
        __task_logger.debug("rsync file set list: %s", rsync_cmd)
        invoke_rsync(rsync_cmd)


@DryPipe.python_call()
def execute_remote_task(
    __task_key,
    __remote_pipeline_specs,
    __pipeline_instance_name,
    __task_conf,
    __task_logger
):

    remote_cli = os.path.join(__remote_pipeline_specs.remote_instance_work_dir, "cli")
    remote_task_control_dir = os.path.join(__remote_pipeline_specs.remote_instance_work_dir, __task_key)

    cmd = [
        "python3", remote_cli, "task", remote_task_control_dir, "--from-remote"
    ]

    if __task_conf.run_as_group is not None:
        __task_logger.info("remote execution at %s, as group %s ", __remote_pipeline_specs.user_at_host,  __task_conf.run_as_group)
        cmd = " ".join(cmd)
        cmd = [
            "newgrp", __task_conf.run_as_group, "<<<", f"'{cmd}'"
        ]
    else:
        __task_logger.info("remote execution at %s")


    exec_remote(__remote_pipeline_specs.user_at_host, cmd)
