import logging
import os.path
import time
from itertools import groupby
from timeit import default_timer as timer

from dry_pipe.task_process import TaskProcess
from dry_pipe.state_machine import AllRunnableTasksCompletedOrInError

logger = logging.getLogger(__name__)


class PipelineRunner:

    def __init__(
        self, state_machines, run_tasks_in_process=False, run_tasks_sync=False, sleep_schedule=None
    ):
        self.state_machine = state_machines
        self._shutdown_requested = False
        self.run_tasks_in_process = run_tasks_in_process
        if sleep_schedule is None:
            self.sleep_schedule = [1, 1, 1, 1, 2, 2, 2, 3, 3, 5]
        else:
            self.sleep_schedule = sleep_schedule
        self.run_tasks_sync = run_tasks_sync

        logger.debug(
            f"PipelineRunner(run_tasks_in_process=%s, run_tasks_sync=%s)",
            self.run_tasks_in_process,
            self.run_tasks_sync
        )


    def iterate_work_rounds(self):
        try:
            sleep_idx = 0
            max_sleep_idx = len(self.sleep_schedule) - 1
            while True:
                c = 0
                for state_file in self.state_machine.iterate_tasks_to_launch():
                    control_dir = state_file.control_dir()
                    as_subprocess = not self.run_tasks_in_process
                    wait_for_completion = self.run_tasks_sync
                    tp = TaskProcess(control_dir, as_subprocess=as_subprocess, wait_for_completion=wait_for_completion)
                    yield lambda: tp.run(by_pipeline_runner=True), None
                    c += 1
                    sleep_idx = 0

                if c == 0:
                    if sleep_idx < max_sleep_idx:
                        sleep_idx += 1
                    yield None, self.sleep_schedule[sleep_idx]
        except AllRunnableTasksCompletedOrInError:
            logger.info(f"no more tasks to launch")
            yield None, None


class WorkiteratorBlender:

    def __init__(self):
        self.work_unit_iterators = []
        work_units_for_round = []
        terminated_idx = []
        while True:
            sleep_for_round = 0
            terminated_idx.clear()
            work_units_for_round.clear()
            for i in range(0, len(self.work_unit_iterators) - 1):
                wi = self.work_unit_iterators[i]
                t, o = next(wi)
                if t == "work":
                    work_units_for_round.append(o)
                elif t == "sleep":
                    if sleep_for_round == 0:
                        sleep_for_round = o
                    elif o < sleep_for_round:
                        sleep_for_round = o
                elif t == "end":
                    terminated_idx.append(i)

            start = timer()
            for w in work_units_for_round:
                w()
            time_elapsed = timer() - start
            sleep_required = sleep_for_round - time_elapsed
            if sleep_required > 0:
                time.sleep(sleep_required)

            if i in terminated_idx:
                del self.work_unit_iterators[i]


class Monitor:
    def __init__(self, task_grouper=None):

        if task_grouper is None:
            self.task_grouper = lambda s: self.default_grouper(s)
        else:
            self.task_grouper = task_grouper


    def default_grouper(self, task_key):
        if "." in task_key:
            task_group_key = task_key.rsplit(".", 1)[0]
            task_group_key = f"{task_group_key}.*"
            return task_group_key
        else:
            return task_key

    def dump(self, all_state_files):
        for task_group, state_counts in self.produce_report(all_state_files):
            dump_counts = ",".join([
                f"{s}: {c}" for s, c in state_counts
            ])
            #print(f"{task_group}: ({dump_counts})")

    def produce_report(self, all_state_files):
        def g():
            for state_file in all_state_files:
                key, state, _ = state_file.key_state_step()

                task_group_key = self.task_grouper(key)

                if state.startswith("_"):
                    state = "launched"

                yield task_group_key, state

        def gf(t):
            task_group_key, state = t
            return task_group_key

        for task_group, states in groupby(sorted(g(), key=gf), key=gf):
            yield task_group, [
                (state, len(list(all_states)))
                for state, all_states in groupby(sorted([s for _, s in states]))
            ]