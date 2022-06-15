import logging
import os
import time
from threading import Thread

import psutil

from dry_pipe import Local
from dry_pipe.actions import TaskAction
from dry_pipe.task_state import TaskState, NON_TERMINAL_STATES
from dry_pipe.utils import send_email_error_report_if_configured

module_logger = logging.getLogger(__name__)

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

    def iterate_on_pipelines(self):

        self.logger.debug("will iterate on pipelines")

        c = 0
        try:
            for p in self.pipelines:
                c += 1
                yield p
        finally:
            self.logger.debug("worked on %s pipelines", c)

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

        self.logger.exception(f"daemon failure ({self.fail_count})")

        if self.fail_count >= DaemonThreadHelper.MAX_DEAMON_FAILS_BEFORE_SHUTDOWN:

            self.logger.critical(f"daemon failed {self.fail_count} times, will exit.")

            daemon_name = self.logger.name

            send_email_error_report_if_configured(f"drypipe daemon {daemon_name} has crashed", exception=exception)

            os._exit(0)

        time.sleep(DaemonThreadHelper.SLEEP_SECONDS_AFTER_DAEMON_FAIL)


def janitor_sub_logger(sub_logger):
    return logging.getLogger(f"{__name__}.{sub_logger}")


class Janitor:

    def __init__(
        self, pipeline=None, pipeline_instance=None, min_sleep=0, max_sleep=5, pipeline_instances_dir=None
    ):

        if pipeline_instance is not None and pipeline is not None:
            raise Exception(f"can't supply both pipeline and pipeline_instance")

        if pipeline_instances_dir is None:
            if pipeline_instance is None:
                raise Exception(f"pipeline_instance can't be None if pipeline_instances_dir is not supplied")
            self.pipelines = [pipeline_instance]
        else:
            if pipeline is None:
                raise Exception(f"pipeline can't be None if pipeline_instances_dir is supplied")
            self.pipelines = pipeline.pipeline_instance_iterator_for_dir(pipeline_instances_dir)

        self.min_sleep = min_sleep
        self.max_sleep = max_sleep
        self._shutdown = False

    def is_shutdown(self):
        return self._shutdown

    def iterate_main_work(self, sync_mode=False, fail_silently=False, stay_alive_when_no_more_work=False):

        daemon_thread_helper = DaemonThreadHelper(
            janitor_sub_logger("main_d"), self.min_sleep, self.max_sleep, self.pipelines, sync_mode
        )
        strike = 0

        while True:

            daemon_thread_helper.begin_round()

            try:

                no_more_work = False

                active_pipelines = 0

                for pipeline in daemon_thread_helper.iterate_on_pipelines():

                    active_pipelines += 1

                    pipeline.init_work_dir()

                    work_done, no_more_work = _janitor(
                        pipeline, wait_for_completion=sync_mode, logger=daemon_thread_helper.logger,
                        fail_silently=fail_silently
                    )

                    if sync_mode:
                        _upload_janitor(pipeline, daemon_thread_helper.logger)
                        _download_janitor(pipeline)

                    yield True

                    if work_done > 0:
                        daemon_thread_helper.register_work()
                    else:

                        if no_more_work and not stay_alive_when_no_more_work:
                            daemon_thread_helper.logger.debug("no more work")
                            yield False
                        else:
                            strike = 0

                if no_more_work:
                    strike += 1

                if strike >= 4 and sync_mode:
                    yield False

                daemon_thread_helper.end_round()

                if active_pipelines == 0 and sync_mode:
                    yield False

#                if active_pipelines == 1 and no_more_work:
#                    break

            except Exception as ex:
                if sync_mode:
                    raise ex
                daemon_thread_helper.handle_exception_in_daemon_loop(ex)

    def start(self, stay_alive_when_no_more_work=False):

        def work():
            work_iterator = self.iterate_main_work(sync_mode=False, stay_alive_when_no_more_work=stay_alive_when_no_more_work)

            has_work = next(work_iterator)
            while has_work:
                has_work = next(work_iterator)

        tread = Thread(target=work)
        tread.start()
        return tread

    def start_remote_janitors(self):

        def upload_j():

            daemon_thread_helper = DaemonThreadHelper(
                janitor_sub_logger("upload_d"), self.min_sleep, self.max_sleep, self.pipelines
            )

            while not self.is_shutdown():

                daemon_thread_helper.begin_round()

                try:

                    for pipeline in daemon_thread_helper.iterate_on_pipelines():

                        if _upload_janitor(pipeline, daemon_thread_helper.logger) > 0:
                            daemon_thread_helper.register_work()

                    daemon_thread_helper.end_round()

                except Exception as ex:
                    daemon_thread_helper.handle_exception_in_daemon_loop(ex)

        def download_j():

            download_j_logger = janitor_sub_logger("download_d")

            daemon_thread_helper = DaemonThreadHelper(
                download_j_logger, self.min_sleep, self.max_sleep, self.pipelines
            )

            while not self.is_shutdown():

                daemon_thread_helper.begin_round()

                try:

                    for pipeline in daemon_thread_helper.iterate_on_pipelines():

                        download_j_logger.debug("will check remote tasks of %s", pipeline.instance_dir_base_name())

                        has_worked = _download_janitor(pipeline, download_j_logger) > 0

                        if has_worked:
                            daemon_thread_helper.register_work()

                    daemon_thread_helper.end_round()

                except Exception as ex:
                    daemon_thread_helper.handle_exception_in_daemon_loop(ex)

        # setup stalled transfer for restart
        for p in self.pipelines:
            TaskState.reset_stalled_transfers(p)

        utread = Thread(target=upload_j)
        dtread = Thread(target=download_j)
        utread.start()
        dtread.start()

        return dtread, utread

    def do_shutdown(self):
        self._shutdown = True


def _janitor(pipeline_instance, wait_for_completion=False, fail_silently=False, logger=None):

    if logger is None:
        logger = module_logger

    logger.debug("launching janitor on %s", pipeline_instance.pipeline_instance_dir)

    pipeline_instance.regen_tasks_if_stale()

    work_done = 0
    tasks_total = 0
    tasks_completed = 0
    tasks_in_non_terminal_states = 0

    for task in pipeline_instance.tasks:

        tasks_total += 1

        task_state = task.get_state()

        if task_state is None:
            logger.debug("will create state file for %s", task)
            task.create_state_file_and_control_dir()
            work_done += 1
        else:

            if task_state.state_name in NON_TERMINAL_STATES:
                tasks_in_non_terminal_states += 1

            if task_state.is_waiting_for_deps():

                if not task.has_unsatisfied_deps():
                    if task_state.is_prepared():
                        continue
                    else:
                        task_state.transition_to_prepared(task)
                        tasks_in_non_terminal_states += 1
                        work_done += 1

            elif task_state.is_completed():
                if task_state.action_if_exists() is None:
                    tasks_completed += 1

    if tasks_total == tasks_completed:
        pipeline_state = pipeline_instance.get_state()
        if not pipeline_state.is_completed():
            pipeline_state.transition_to_completed()

    if tasks_total == tasks_completed or tasks_in_non_terminal_states == 0:
        return work_done, True

    for task_state in TaskState.prepared_task_states(pipeline_instance):
        logger.debug("will queue %s", task_state.control_dir())
        task_state.transition_to_queued()
        work_done += 1

    currently_running = TaskState.count_running_local(pipeline_instance)
    cpu_count = len(psutil.Process().cpu_affinity())

    launched_count = 0
    throttled_count = 0
    queued_count = 0

    for task_state in TaskState.queued_task_states(pipeline_instance):

        queued_count += 1

        task = pipeline_instance.tasks[task_state.task_key]

        executer = task.executer

        if isinstance(executer, Local):
            if currently_running >= cpu_count:
                logger.info(
                    "exceeded cpu load %s tasks running, will resume launching when below threshold", currently_running
                )
                throttled_count += 1
                continue

            currently_running += 1

        logger.debug("will launch %s", task_state.control_dir())
        task_state.transition_to_launched(task, wait_for_completion, fail_silently=fail_silently)
        launched_count += 1
        work_done += 1

    module_logger.debug(
        "completed launch round (queued_count, throttled_count, launched_count): (%s %s %s), ",
        queued_count, throttled_count, launched_count
    )

    for task_state in TaskState.completed_unsigned_task_states(pipeline_instance):

        task = pipeline_instance.tasks[task_state.task_key]

        logger.debug("will transition %s to complete", task_state.control_dir())
        task_state.transition_to_completed(task)

    for task_action in TaskAction.fetch_all_actions(pipeline_instance.pipeline_instance_dir):
        task_action.do_it(pipeline_instance)

    return work_done, False


def _upload_janitor(pipeline, logger):

    for remote_executor, task_conf in pipeline.remote_executors_with_task_confs():
        remote_executor.upload_overrides(pipeline, task_conf)
        #remote_executor.rsync_remote_code_dir_if_applies(pipeline, task_conf)

    work_done = 0

    for task_state in TaskState.queued_for_upload_task_states(pipeline):
        task_state = task_state.transition_to_upload_started()
        task = pipeline.tasks[task_state.task_key]
        task.executer.upload_task_inputs(task_state, task)
        task_state = task_state.transition_to_upload_completed()
        task_state.transition_to_queued()

        work_done += 1

    return work_done


def _download_janitor(pipeline, download_j_logger=None):

    if download_j_logger is None:
        download_j_logger = logging.getLogger()

    work_done = 0

    for remote_executor, task_conf in pipeline.remote_executors_with_task_confs():

        download_j_logger.debug("handle remote exec %s ", remote_executor)

        running_task_count = 0

        for task_state in remote_executor.fetch_remote_task_states(pipeline):

            running_task_count += 1

            if task_state.is_completed_unsigned():
                task_state.transition_to_queued_remote_download(pipeline.pipeline_instance_dir, task_state.task_key)
                remote_executor.delete_remote_state_file(task_state.abs_file_name())
            else:
                task_state.assign_remote_state_to_local_state_file(pipeline.pipeline_instance_dir, task_state)

            work_done += 1

        if running_task_count > 0:
            remote_executor.fetch_new_log_lines(pipeline.pipeline_instance_dir)

    for task_state in TaskState.queued_for_dowload_task_states(pipeline):
        task_state = task_state.transition_to_download_started()
        pipeline.tasks[task_state.task_key].executer.download_task_results(task_state)
        task_state = task_state.transition_to_download_completed()

        task = pipeline.tasks[task_state.task_key]

        task_state.transition_to_completed(task)

        work_done += 1

    return work_done

