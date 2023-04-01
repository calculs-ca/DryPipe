from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from queue import SimpleQueue
import time
from threading import Thread


class PipelineRunner:

    def __init__(self, *state_machines):
        self.state_machines = state_machines
        self._shutdown_requested = False
        self.running_status = "idle"

    def run_sync(self):
        for state_machine in self.state_machines:
            for state_file in state_machine.iterate_tasks_to_launch():
                if state_file is None:
                    time.sleep(1)
                else:
                    self.launch(state_file)

    def start(self):
        launch_queue = SimpleQueue()

        def launch_daemon():

            def process_task(state_file):
                state_file

            with ThreadPoolExecutor(max_workers=5) as executor:
                while not self._shutdown_requested:
                    batch = self.launch_queue.get()
                    executor.map(process_task, batch)

        def main_daemon():
            last_inactive_round_count = 0
            sleep_prescriptions = [0, 1, 2, 2, 4, 4, 5, 8, 9, 10]
            while not self._shutdown_requested:
                work_done = 0
                while batch := list(islice(self.iterate_tasks_to_launch(), 50)):
                    work_done += len(batch)
                    self.launch_queue.put(batch)
                if work_done > 0:
                    last_inactive_round_count = 0
                    continue
                elif last_inactive_round_count < len(sleep_prescriptions):
                    last_inactive_round_count += 1

                if last_inactive_round_count > 0:
                    time.sleep(sleep_prescriptions[last_inactive_round_count])

        main_thread = Thread(target=main_daemon)
        main_thread.start()
        launch_thread = Thread(target=launch_daemon)
        launch_thread.start()

        self.threads.append(main_thread)
        self.threads.append(launch_thread)
