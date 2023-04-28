import time
from timeit import default_timer as timer

from dry_pipe.core_lib import TaskProcess
from dry_pipe.state_machine import AllRunnableTasksCompletedOrInError


class PipelineRunner:

    def __init__(self, state_machines, run_tasks_in_process=False, run_tasks_async=True, sleep_schedule=None):
        self.state_machine = state_machines
        self._shutdown_requested = False
        self.running_status = "idle"
        self.run_tasks_in_process = run_tasks_in_process
        if sleep_schedule is None:
            self.sleep_schedule = [1, 1, 1, 1, 2, 2, 2, 3, 3, 5]
        else:
            self.sleep_schedule = sleep_schedule
        self.run_tasks_async = run_tasks_async


    def iterate_work_rounds(self):
        try:
            sleep_idx = 0
            max_sleep_idx = len(self.sleep_schedule) - 1
            while True:
                c = 0
                for state_file in self.state_machine.iterate_tasks_to_launch():
                    yield lambda: TaskProcess.run(
                        state_file.control_dir(),
                        as_subprocess=not self.run_tasks_in_process,
                        wait_for_completion=not self.run_tasks_async
                    ), None
                    c += 1
                if c == 0:
                    if sleep_idx < max_sleep_idx:
                        sleep_idx += 1
                    yield None, self.sleep_schedule[sleep_idx]
        except AllRunnableTasksCompletedOrInError:
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