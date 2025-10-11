import json
import logging
import os
from pathlib import Path

from dry_pipe import DryPipe, RemotePipelineSpecs
from dry_pipe.core_lib import invoke_rsync, exec_remote, SleepySpinner
from dry_pipe.state_file import StateFile
from dry_pipe.globus import GlobusToken, GlobusFileTransfer
from dry_pipe.task_process import TaskProcess


@DryPipe.python_call()
def submit_local_array(__task_process):
    from dry_pipe.slurm_array_task import SlurmArrayParentTask
    t = SlurmArrayParentTask(__task_process)
    array_tasks_submitted = t.prepare_and_launch_next_array(None, restart_failed=True)
    return {"array_tasks_submitted": array_tasks_submitted}


@DryPipe.python_call()
def watch_local_array(__task_process):
    from dry_pipe.slurm_array_task import SlurmArrayParentTask, AutoRestartManager

    t = SlurmArrayParentTask(__task_process)
    task_logger = __task_process.task_logger

    with SleepySpinner(__task_process.sleep_schedule([30, 120, 240]), task_logger) as ss:

        round_counter = 0

        if t.task_process.task_conf.auto_restart_condition_regexp_per_log_file is not None:
            arm = AutoRestartManager(t.task_process.task_conf.auto_restart_condition_regexp_per_log_file)
        else:
            arm = None

        while True:

            t.compare_and_reconcile_squeue_with_state_files(alternate_logger=task_logger)
            if arm is not None:
                #if round % 5 == 1 and round > 1:
                relaunch_count = t.prepare_and_launch_next_array(
                    auto_restart_manager=arm, alternate_logger=__task_process.task_logger
                )
            else:
                relaunch_count = 0

            number_of_active_sbatch_submissions = \
                t.number_of_active_sbatch_submissions()
            if number_of_active_sbatch_submissions == 0:
                if relaunch_count == 0:
                    task_logger.info("array task has no more running array jobs")

                    report = t.inspect_child_tasks()
                    failed_task_keys = report["failed_task_keys"]
                    failed_task_count = len(failed_task_keys)
                    if failed_task_count > 0:
                        msg = json.dumps(report)
                        raise Exception(f"{failed_task_count} tasks failed, {msg}")
                    break
            else:
                task_logger.info(
                    "array still has %s active submissions, will sleep %s seconds",
                    number_of_active_sbatch_submissions, ss.next_sleep()
                )
                ss.sleep()

            round_counter += 1



@DryPipe.python_call()
def submit_remote_array(__remote_pipeline_specs):
    res = __remote_pipeline_specs.remote_exec_json_results("submit-array-from-remote")
    msg = json.dumps(res)
    __remote_pipeline_specs.task_process.task_logger.info("submit array from remote %s", msg)


@DryPipe.python_call()
def watch_remote_array(__remote_pipeline_specs):

    __task_process = __remote_pipeline_specs.task_process
    task_logger = __task_process.task_logger

    with SleepySpinner(__task_process.sleep_schedule([30, 120, 240]), task_logger) as ss:

        round_counter = 0

        while True:

            __remote_pipeline_specs.fetch_remote_array_states_and_reconcile()
            __remote_pipeline_specs.fetch_remote_logs()

            try:
                report = __remote_pipeline_specs.remote_exec_json_results("watch-array-from-remote")
            except Exception:
                __remote_pipeline_specs.fetch_remote_logs()
                raise

            total_children_tasks = report["total_children_tasks"]
            completed_tasks = report["completed_tasks"]

            if completed_tasks == total_children_tasks:
                __task_process.task_logger.info("array task completed successfully")
                return

            failed_task_count = len(report["failed_task_keys"])

            if failed_task_count > 0:
                first_20_tasks = ','.join(report["failed_task_keys"][:20])

                try:
                    __remote_pipeline_specs.fetch_remote_array_states_and_reconcile()
                    __remote_pipeline_specs.fetch_remote_logs()
                    task_logger.info("remote states and logs have been reconciled")
                except Exception as ex:
                    task_logger.info("failed to reconcile remote states and logs")

                raise Exception(f"{failed_task_count} child remote tasks failed {first_20_tasks}")
            else:
                task_logger.debug("%s", json.dumps(report))

            round_counter += 1

            ss.sleep()



@DryPipe.python_call()
def upload_task_inputs_rsync(
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
def download_other_task_outputs(__task_process, __other_task_key):

    other_task_process = TaskProcess(
        Path(__task_process.control_dir).parent.joinpath(__other_task_key).__str__()
    )

    if other_task_process.task_conf.globus_transfer is not None:
        download_task_outputs_globus.func(other_task_process)
    elif other_task_process.task_conf.ssh_remote_dest is not None:
        download_task_outputs_rsync.func(other_task_process)
    else:
        __task_process.task_logger.info("task %s is not remote, no need to download", __other_task_key)


@DryPipe.python_call()
def download_task_outputs_rsync(__task_process):
    from dry_pipe.slurm_array_task import SlurmArrayParentTask

    __remote_pipeline_specs = RemotePipelineSpecs(__task_process)

    #fetch states, and generate rsync list
    remote_cli = os.path.join(__remote_pipeline_specs.remote_instance_work_dir, "cli")

    remote_exec_result = exec_remote(__remote_pipeline_specs.user_at_host, [
        "python3",
        remote_cli,
        "list-states",
        "--gen-rsync-list",
        f"--task-key={__task_process.task_key}"
    ])

    __task_process.task_logger.debug("remote states:\n %s", remote_exec_result)

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
                l1 = Path(f"{__task_process.pipeline_instance_dir}/.drypipe/{k}/drypipe.log")
                l2 = Path(f"{__task_process.pipeline_instance_dir}/.drypipe/{k}/out.log")
                if l1.exists():
                    l1.unlink()
                if l2.exists():
                    l2.unlink()

                f.write(f".drypipe/{k}/drypipe.log\n")
                f.write(f".drypipe/{k}/out.log\n")


    pid = __task_process.pipeline_instance_dir

    pipeline_base_name = os.path.basename(pid)

    ssh_remote_dest = \
        f"{__remote_pipeline_specs.user_at_host}:{__remote_pipeline_specs.remote_base_dir}/{pipeline_base_name}/"

    def rs(cmd):
        __task_process.task_logger.info(f"running command: {cmd}")
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
    task_process = __remote_pipeline_specs.task_process
    task_logger = __remote_pipeline_specs.task_process.task_logger

    def fetch_remote_state():
        res = __remote_pipeline_specs.remote_exec("poll-task")
        for remote_state_file_absolute_path in res.split("\n"):

            if not "/state." in remote_state_file_absolute_path:
                continue

            return StateFile.create_from_path(__task_key, remote_state_file_absolute_path)

    with SleepySpinner(task_process.sleep_schedule([30, 120, 240]), task_logger) as ss:

        while True:

            remote_state_file = fetch_remote_state()

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


@DryPipe.python_call()
def upload_task_inputs_globus(
    __task_key,
    __task_control_dir,
    __remote_pipeline_specs,
    __task_logger,
    __task_process,
    __pipeline_instance_dir,
    __task_conf
):

    def rewrite_if(it):
        if __task_conf.globus_local_path_rewrite is None:
            yield from it
        else:
            prefix, replacement_prefix = __task_conf.globus_local_path_rewrite.split(":")
            for f1, f2 in it:
                if f1.startswith(prefix):
                    f_suffix = f1[len(prefix):]
                    yield f"{replacement_prefix}{f_suffix}", f2
                else:
                    yield f1, f2

    __task_logger.info("will generate file list for upload")

    src_endpoint, dst_endpoint, tok_file, client_id = __task_conf.globus_transfer.split(":")

    tok = GlobusToken(access_token_file=tok_file, client_id=client_id)

    transfer = GlobusFileTransfer(tok, src_endpoint, dst_endpoint)

    def g():
        external_file_deps = []
        for f in __task_process.gen_internal_file_deps(external_file_deps):
            yield str(Path(__pipeline_instance_dir, f)), str(Path(__remote_pipeline_specs.remote_pid, f))

        for f in external_file_deps:
            yield f, str(Path(__remote_pipeline_specs.remote_pid, "external-file-deps", f))

        o_src = __remote_pipeline_specs.gen_remote_site_env_file()

        yield o_src, str(Path(__remote_pipeline_specs.remote_instance_work_dir, "site.env"))

    transfer_response = transfer.submit_file_transfer(
        rewrite_if(g()),
        log_file=
        Path(__task_control_dir, "globus-uploads.json")
        if __task_logger.isEnabledFor(logging.DEBUG)
        else None
    )

    transfer_response.spin_until_complete(__task_logger)




@DryPipe.python_call()
def download_task_outputs_globus(
    __task_key,
    __task_control_dir,
    __task_logger,
    __task_process,
    __pipeline_work_dir,
    __pipeline_instance_dir,
    __remote_pipeline_specs,
    __task_conf
):

    def rewrite_if(it):
        if __task_conf.globus_local_path_rewrite is None:
            yield from it
        else:
            prefix, replacement_prefix = __task_conf.globus_local_path_rewrite.split(":")
            for f1, f2 in it:
                if f2.startswith(prefix):
                    f_suffix = f2[len(prefix):]
                    yield f1, f"{replacement_prefix}{f_suffix}"
                else:
                    yield f1, f2


    #fetch states, and generate rsync list
    remote_cli = os.path.join(__remote_pipeline_specs.remote_instance_work_dir, "cli")

    remote_exec_result = exec_remote(__remote_pipeline_specs.user_at_host, [
        "python3",
        remote_cli,
        "list-states",
        f"--task-key={__task_key}"
    ], logger_func=__task_logger.info)

    __task_logger.debug("remote states:\n %s", remote_exec_result)

    if __task_process.is_slurm_array_parent():
        __remote_pipeline_specs.reconcile_local_array_states_with_remote_state(remote_exec_result)


    __task_logger.info("will generate file list for upload")

    src_endpoint, dst_endpoint, tok_file, client_id = __task_conf.globus_transfer.split(":")

    tok = GlobusToken(access_token_file=tok_file, client_id=client_id)

    transfer = GlobusFileTransfer(tok, dst_endpoint, src_endpoint)

    def g():
        for f in __remote_pipeline_specs.gen_result_files():
            yield str(Path(__remote_pipeline_specs.remote_pid, f)), str(Path(__pipeline_instance_dir, f))

    transfer_response = transfer.submit_file_transfer(
        rewrite_if(g()),
        log_file=
            Path(__task_control_dir, "globus-downloads-1.json")
            if __task_logger.isEnabledFor(logging.DEBUG)
            else None
    )

    transfer_response.spin_until_complete(__task_logger)

    file_set_list = __task_process.file_sets_rsync_list_file()

    if os.path.exists(file_set_list) and os.stat(file_set_list).st_size > 0:

        def g2():
            with open(file_set_list) as _file_set_list:
                for f in _file_set_list.readlines():
                    f = f.strip()
                    yield str(Path(__remote_pipeline_specs.remote_pid, f)), str(Path(__pipeline_instance_dir, f))

        transfer_response = transfer.submit_file_transfer(
            rewrite_if(g2()),
            log_file=
                Path(__task_control_dir, "globus-downloads-2.json")
                if __task_logger.isEnabledFor(logging.DEBUG)
                else None
        )

        transfer_response.spin_until_complete(__task_logger)