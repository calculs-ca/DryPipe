import logging
import time
from datetime import datetime
from threading import Thread


from dry_pipe import Task
from dry_pipe.ssh_executer import RemoteSSH
from dry_pipe.task_state import TaskState
from dry_pipe.utils import send_email_error_report_if_configured, count_cpus

def janitor_sub_logger(sub_logger):
    return logging.getLogger(f"{__name__}.{sub_logger}")

module_logger = logging.getLogger(__name__)
download_j_logger = janitor_sub_logger("download_d")
upload_d_logger = janitor_sub_logger("upload_d")
main_d_logger = janitor_sub_logger("main_d")

class DaemonThreadHelper:

    MAX_DEAMON_FAILS_BEFORE_SHUTDOWN = 10

    SLEEP_SECONDS_AFTER_DAEMON_FAIL = 5

    def __init__(self, logger, min_sleep, max_sleep, pipelines, never_sleep=False):

        logger.debug("daemon thread %s about to start janitoring", logger.name)
        self.max_sleep = max_sleep
        self.min_sleep = min_sleep
        self.never_sleep = never_sleep
        self.fail_count = 0
        self.loop_counter = 0
        self.round_counter = 0

        self.sleep_time = min_sleep
        self.has_worked_in_round = False
        self.pipelines = pipelines
        self.logger = logger
        self.ssh_executer_per_remote_site_key = {}
        self.last_exception_at = None
        self.cpu_count = count_cpus()

    def iterate_on_pipeline_instances(self):

        self.logger.debug("will iterate on pipeline instances")

        c = 0
        try:
            for p in self.pipelines:
                c += 1
                yield p
        finally:
            self.logger.debug("worked on %s pipeline instances", c)

    def begin_round(self):
        self.has_worked_in_round = False
        self.round_counter += 1

    # For throttling
    def should_work_now(self, must_work_every_n):
        res = self.round_counter % must_work_every_n == 0
        return res

    def register_work(self):
        self.has_worked_in_round = True

    def end_round(self, skip_sleep=False):

        if not self.has_worked_in_round:
            self.loop_counter += 1
            self.logger.debug("no work in this round")

        if self.has_worked_in_round:
            self.sleep_time = self.min_sleep
        elif self.sleep_time < self.max_sleep:
            self.sleep_time += 1

        if not self.never_sleep:
            if not skip_sleep:
                time.sleep(self.sleep_time)

    def handle_exception_in_daemon_loop(self, exception):

        self.fail_count += 1

        self.logger.error(f"daemon failure ({self.fail_count})")

        if self.last_exception_at is not None:
            seconds_since_last_exception = (datetime.now() - self.last_exception_at).total_seconds()
            hour_in_seconds = 60*60
            if seconds_since_last_exception > hour_in_seconds:
                self.logger.info(f"last daemon exceeds 1 hour, will reset fail_count")
                self.fail_count = 1

        if self.fail_count >= DaemonThreadHelper.MAX_DEAMON_FAILS_BEFORE_SHUTDOWN:

            self.logger.critical(f"daemon failed {self.fail_count} 10x in last hour, will exit.")

            daemon_name = self.logger.name

            send_email_error_report_if_configured(f"drypipe daemon {daemon_name} has crashed", exception=exception)

            return True

        self._reset_ssh_connections_if_exception_recoverable(exception)

        self.last_exception_at = datetime.now()
        time.sleep(DaemonThreadHelper.SLEEP_SECONDS_AFTER_DAEMON_FAIL)

        return False

    def _reset_ssh_connections_if_exception_recoverable(self, ex):

        if RemoteSSH.exception_is_recoverable_with_connection_reset(ex):
            self.logger.info(f"will reset ssh connections")
            try:
                for ssh_executer in self.ssh_executer_per_remote_site_key.values():
                    ssh_executer.close()
            except:
                pass

            self.ssh_executer_per_remote_site_key = {}
        else:
            self.logger.info(f"exception not known as retryable %s", ex.__class__.__name__)

    def get_executer(self, task_conf):

        if not task_conf.is_remote():
            return task_conf.create_executer()

        ssh_executer = self.ssh_executer_per_remote_site_key.get(task_conf.remote_site_key)

        if ssh_executer is None:
            ssh_executer = task_conf.create_executer()
            self.ssh_executer_per_remote_site_key[task_conf.remote_site_key] = ssh_executer
            ssh_executer.connect()

        if ssh_executer.ssh_client is None:
            raise Exception(f"{task_conf.remote_site_key} has no ssh_client")

        return ssh_executer


class Janitor:

    def __init__(
        self, pipeline_instance=None, pipeline_instances_iterator=None, min_sleep=0, max_sleep=5
    ):

        if pipeline_instance is not None and pipeline_instances_iterator is not None:
            raise Exception(f"can't supply both pipeline_instance and pipeline_instances_iterator")

        if pipeline_instance is None and pipeline_instances_iterator is None:
            raise Exception(f"must supply either pipeline_instance or pipeline_instances_iterator")

        if pipeline_instances_iterator is None:
            self.pipelines = [pipeline_instance]
        else:
            self.pipelines = pipeline_instances_iterator

        self.min_sleep = min_sleep
        self.max_sleep = max_sleep
        self._shutdown = False

    def request_shutdown(self):
        self._shutdown = True

    def is_shutdown(self):
        return self._shutdown

    def iterate_main_work(self, sync_mode=False, fail_silently=False):

        stay_alive_when_no_more_work = not sync_mode

        daemon_thread_helper = DaemonThreadHelper(
            main_d_logger, self.min_sleep, self.max_sleep, self.pipelines, sync_mode
        )

        while True:

            if self.is_shutdown():
                daemon_thread_helper.logger.info("shutdown requested, will exit main loop")
                break

            daemon_thread_helper.begin_round()

            try:

                work_done = 0

                for pipeline_instance in daemon_thread_helper.iterate_on_pipeline_instances():

                    pipeline_instance.init_work_dir()

                    work_done_on_pipeline_instance = _janitor_ng(
                        pipeline_instance,
                        wait_for_completion=sync_mode,
                        fail_silently=fail_silently,
                        daemon_thread_helper=daemon_thread_helper
                    )

                    if sync_mode:
                        _upload_janitor(daemon_thread_helper, pipeline_instance)
                        _download_janitor(daemon_thread_helper, pipeline_instance, sync_mode)

                    if work_done_on_pipeline_instance > 0:
                        work_done += work_done_on_pipeline_instance
                        daemon_thread_helper.register_work()

                    yield True

                if sync_mode and not stay_alive_when_no_more_work and work_done == 0:
                    yield False

                daemon_thread_helper.end_round()

            except Exception as ex:
                daemon_thread_helper.logger.exception(ex)
                if sync_mode:
                    raise ex
                if daemon_thread_helper.handle_exception_in_daemon_loop(ex):
                    self.request_shutdown()

    def start(self):

        def work():
            work_iterator = self.iterate_main_work(sync_mode=False)

            has_work = next(work_iterator)
            while has_work:
                has_work = next(work_iterator)

        tread = Thread(target=work)
        tread.start()
        return tread

    def start_remote_janitors(self):

        def upload_j():

            daemon_thread_helper = DaemonThreadHelper(
                upload_d_logger, self.min_sleep, self.max_sleep, self.pipelines
            )

            while not self.is_shutdown():

                daemon_thread_helper.begin_round()

                try:

                    for pipeline in daemon_thread_helper.iterate_on_pipeline_instances():

                        if _upload_janitor(daemon_thread_helper, pipeline) > 0:
                            daemon_thread_helper.register_work()

                    daemon_thread_helper.end_round()

                except Exception as ex:
                    daemon_thread_helper.logger.exception(ex)
                    if daemon_thread_helper.handle_exception_in_daemon_loop(ex):
                        self.request_shutdown()

        def download_j():

            daemon_thread_helper = DaemonThreadHelper(
                download_j_logger, self.min_sleep, self.max_sleep, self.pipelines
            )

            while not self.is_shutdown():

                daemon_thread_helper.begin_round()

                try:

                    for pipeline in daemon_thread_helper.iterate_on_pipeline_instances():

                        download_j_logger.debug("will check remote tasks of %s", pipeline.instance_dir_base_name())

                        has_worked = _download_janitor(daemon_thread_helper, pipeline) > 0

                        if has_worked:
                            daemon_thread_helper.register_work()

                    daemon_thread_helper.end_round()

                except Exception as ex:
                    daemon_thread_helper.logger.exception(ex)
                    if daemon_thread_helper.handle_exception_in_daemon_loop(ex):
                        self.request_shutdown()


        # setup stalled transfer for restart
        for p in self.pipelines:
            TaskState.reset_stalled_transfers(p)

        utread = Thread(target=upload_j)
        dtread = Thread(target=download_j)
        utread.start()
        dtread.start()

        return dtread, utread


def _janitor_ng(pipeline_instance, wait_for_completion=False, fail_silently=False, daemon_thread_helper=None):

    pipeline_instance.regen_tasks_if_stale()

    for task_conf in pipeline_instance.remote_sites_task_confs():
        remote_executor = daemon_thread_helper.get_executer(task_conf)
        remote_executor.prepare_remote_instance_directory(pipeline_instance, task_conf)

    work_done = 0
    total_tasks = 0
    tasks_completed = 0

    for task in pipeline_instance.tasks:
        total_tasks += 1

        task_state = task.get_state()

        if task_state is None:
            work_done += 1
            task.create_state_file_and_control_dir()
            task.prepare()
            # assert task.get_state().is_waiting_for_deps()

    currently_running = TaskState.count_running_local(pipeline_instance)
    throttled_count = 0

    for task_state in TaskState.fetch_all(pipeline_instance.pipeline_instance_dir):

        task = Task.load_from_task_state(task_state)
        action = task_state.action_if_exists()

        if action is not None:
            work_done += 1
            action.do_it(task, task.executer)

        if task_state.is_completed():
            tasks_completed += 1
            continue
        if task_state.is_failed() or task_state.is_crashed():
            continue
        elif task_state.is_completed_unsigned():
            work_done += 1
            task_state.transition_to_completed(task)
        else:

            if task_state.is_waiting_for_deps():
                if task_state.is_all_deps_ready():
                    work_done += 1
                    task_state.transition_to_prepared()
            elif task_state.is_queued():
                work_done += 1

                if task.task_conf.is_process():
                    if currently_running >= daemon_thread_helper.cpu_count:
                        daemon_thread_helper.logger.debug(
                            "exceeded cpu load %s tasks running, will resume launching when below threshold",
                            currently_running
                        )
                        throttled_count += 1
                        continue

                    currently_running += 1

                executer = daemon_thread_helper.get_executer(task.task_conf)
                task_state.transition_to_launched(executer, task, wait_for_completion, fail_silently=fail_silently)

    if total_tasks == tasks_completed:
        pipeline_state = pipeline_instance.get_state()
        if not pipeline_state.is_completed():
            pipeline_state.transition_to_completed()

    return work_done


def _upload_janitor(daemon_thread_helper, pipeline):

    work_done = 0

    for task_state in TaskState.queued_for_upload_task_states(pipeline):
        task_state = task_state.transition_to_upload_started()
        task = pipeline.tasks[task_state.task_key]
        executer = daemon_thread_helper.get_executer(task.task_conf)
        executer.upload_task_inputs(task_state)
        task_state = task_state.transition_to_upload_completed()
        task_state.transition_to_queued()

        work_done += 1

    return work_done


def _download_janitor(daemon_thread_helper, pipeline_instance, is_sync_mode=False):

    work_done = 0

    frequency_mod = 2 if is_sync_mode else 5

    if daemon_thread_helper.round_counter % frequency_mod == 0:
        remote_task_confs_to_check = [
            task_conf for task_conf in pipeline_instance.remote_sites_task_confs(for_zombie_detection=True)
        ]
        if len(remote_task_confs_to_check) == 0:
            download_j_logger.debug("no zombies possible at this time")

        for task_conf in remote_task_confs_to_check:
            download_j_logger.debug("will check for zombies on %s", task_conf.full_remote_path(pipeline_instance))
            remote_executor = daemon_thread_helper.get_executer(task_conf)
            zombies_detected_msg = remote_executor.detect_zombies(pipeline_instance)
            if zombies_detected_msg is not None:
                send_email_error_report_if_configured(f"zombie tasks detected", details=zombies_detected_msg)
                module_logger.error("zombies detected: %s", zombies_detected_msg)

    for task_conf in pipeline_instance.remote_sites_task_confs():
        remote_executor = daemon_thread_helper.get_executer(task_conf)
        download_j_logger.debug("will check task states on %s", task_conf.full_remote_path(pipeline_instance))

        running_task_count = 0

        for task_state in remote_executor.fetch_remote_task_states(pipeline_instance):

            running_task_count += 1

            if task_state.is_completed_unsigned():
                task_state.transition_to_queued_remote_download(pipeline_instance.pipeline_instance_dir, task_state.task_key)
                remote_executor.delete_remote_state_file(task_state.abs_file_name())
            else:
                task_state.assign_remote_state_to_local_state_file(pipeline_instance.pipeline_instance_dir, task_state)

            work_done += 1

        if running_task_count > 0:
            remote_executor.fetch_new_log_lines(pipeline_instance.pipeline_instance_dir)

    for task_state in TaskState.queued_for_dowload_task_states(pipeline_instance):
        task_state = task_state.transition_to_download_started()
        task = pipeline_instance.tasks[task_state.task_key]
        remote_executor = daemon_thread_helper.get_executer(task.task_conf)
        remote_executor.download_task_results(task_state)
        task_state = task_state.transition_to_download_completed()
        task_state.transition_to_completed(task)
        work_done += 1

    return work_done

