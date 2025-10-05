import os
from pathlib import Path

from dry_pipe import DryPipe
from dry_pipe.core_lib import invoke_rsync, exec_remote, SleepySpinner
from dry_pipe.state_file import StateFile


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
    __pipeline_instance_dir,
    __task_conf
):

    external_file_deps = []

    __task_logger.info("will generate file list for upload")

    def g():
        for f in __task_process.gen_internal_file_deps(external_file_deps):
            ff = Path(__pipeline_instance_dir, f)
            if ff.is_dir() and not f.endswith("/"):
                yield f"{f}/"
            else:
                yield f

    internal_dep_file_txt = __remote_pipeline_specs.dump_unique_files_in_file(
        g(),
        "deps.txt"
    )


    if len(external_file_deps) == 0:
        __task_logger.info("no external file deps")
    else:
        __task_logger.info("will generate external file deps")
        external_dep_file_txt = __remote_pipeline_specs.dump_unique_files_in_file(external_file_deps, "external-deps.txt")


    def rs(cmd):
        __task_logger.info(f"running command: {cmd}")
        invoke_rsync(cmd)

    def do_rsync(src, dst, deps_file):
        rs(f"rsync {__remote_pipeline_specs.rsync_chown_arg} --mkpath -a --dirs --files-from={deps_file} {src}/ {dst}/")


    def rsync_upload(overrides_file, dst):
        rs(f"rsync {__remote_pipeline_specs.rsync_chown_arg} --mkpath {overrides_file} {dst}")

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
    from dry_pipe.slurm_array_task import SlurmArrayParentTask

    #fetch states, and generate rsync list
    remote_cli = os.path.join(__remote_pipeline_specs.remote_instance_work_dir, "cli")

    remote_exec_result = exec_remote(__remote_pipeline_specs.user_at_host, [
        "python3",
        remote_cli,
        "list-states",
        "--gen-rsync-list",
        f"--task-key={__task_key}"
    ])

    __task_logger.debug("remote states:\n %s", remote_exec_result)

    if __task_process.is_slurm_array_parent():
        __remote_pipeline_specs.reconcile_local_array_states_with_remote_state(remote_exec_result)

    result_file_txt = __remote_pipeline_specs.dump_unique_files_in_file(
        __remote_pipeline_specs.gen_result_files(),
        "result-files.txt"
    )

    if __task_process.is_slurm_array_parent():
        sa = SlurmArrayParentTask(__task_process)
        with open(result_file_txt, "a+") as f:
            for k in sa.children_task_keys():
                l1 = Path(f"{__pipeline_instance_dir}/.drypipe/{k}/drypipe.log")
                l2 = Path(f"{__pipeline_instance_dir}/.drypipe/{k}/out.log")
                if l1.exists():
                    l1.unlink()
                if l2.exists():
                    l2.unlink()

                f.write(f".drypipe/{k}/drypipe.log\n")
                f.write(f".drypipe/{k}/out.log\n")


    pid = __pipeline_instance_dir

    pipeline_base_name = os.path.basename(pid)

    ssh_remote_dest = \
        f"{__remote_pipeline_specs.user_at_host}:{__remote_pipeline_specs.remote_base_dir}/{pipeline_base_name}/"

    def rs(cmd):
        __task_logger.info(f"running command: {cmd}")
        invoke_rsync(cmd)

    rs(f"rsync -a --dirs --partial --ignore-missing-args --files-from={result_file_txt} {ssh_remote_dest} {pid}/")

    file_set_list = __task_process.file_sets_rsync_list_file()

    if os.path.exists(file_set_list) and os.stat(file_set_list).st_size > 0:
        rs(f"rsync -a --dirs --partial --files-from={file_set_list} {ssh_remote_dest} {pid}/")


@DryPipe.python_call()
def execute_remote_task(
        __task_key,
        __remote_pipeline_specs
):
    __remote_pipeline_specs.remote_exec("remote-exec")



@DryPipe.python_call()
def poll_remote_task(
    __task_key,
    __remote_pipeline_specs
):
    task_logger = __remote_pipeline_specs.task_process.task_logger

    def fetch_remote_state():
        res = __remote_pipeline_specs.remote_exec("poll-task")
        for remote_state_file_absolute_path in res.split("\n"):

            if not "/state." in remote_state_file_absolute_path:
                continue

            return StateFile.create_from_path(__task_key, remote_state_file_absolute_path)

    max_sleep = 180

    with SleepySpinner([1, 5, 6, 10, 30, 30, 30, 120, max_sleep]) as ss:

        while True:

            remote_state_file = fetch_remote_state()

            if __remote_pipeline_specs.task_process.is_slurm_array_parent():
                #if ss.next_sleep() in [1, 6, 120, max_sleep] or True:
                __remote_pipeline_specs.fetch_remote_array_states_and_reconcile()

            if __remote_pipeline_specs.task_process.auto_reconcile_logs():
                __remote_pipeline_specs.fetch_remote_logs()


            if remote_state_file.did_not_succeed():
                raise Exception(f"remote task {remote_state_file.path} did not succeed")

            if remote_state_file.is_completed():
                task_logger.info("remote task completed")
                return

            ns = ss.next_sleep()
            task_logger.debug(
                "remote state is %s, will sleep for %s", remote_state_file.state_as_string(), ns
            )

            ss.sleep()