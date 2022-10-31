import logging
import os
import time
import traceback
from threading import Thread

import readchar
from rich.columns import Columns
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.align import Align

from dry_pipe.monitoring import PipelineMetricsTable, fetch_task_group_metrics
from dry_pipe.pipeline import PipelineInstance, Pipeline
from dry_pipe.task_state import TaskState

logger = logging.getLogger(__name__)


class CliScreen:

    def __init__(self, pipeline_instance_dir, regex_grouper, is_monitor_mode, quit_listener=None, group_by=None):
        self.is_monitor_mode = is_monitor_mode
        self.pipeline_instance_dir = pipeline_instance_dir
        self.pipeline_hints = PipelineInstance.load_hints(pipeline_instance_dir)
        self.quit = False
        self.screen = 'summary'
        self.failed_task_states = []
        self.failed_task_states_iter_count = 0
        self.selected_failed_task = None
        self.prompt = None
        self.error_msg = []
        self.loop_counter = 0
        self.rich_live_auto_refresh = True
        self.quit_listener = quit_listener

        pipeline = Pipeline.load_from_module_func(self.pipeline_hints["pipeline"])
        self.task_groupers = pipeline.task_groupers

        if regex_grouper is not None:
            self.task_groupers["regex_grouper"] = regex_grouper
            self.selected_grouper_name = "regex_grouper"
        else:
            self.selected_grouper_name = "by_type"

        if os.environ.get("DRYPIPE_CLI_AUTO_REFRESH") == "False":
            self.rich_live_auto_refresh = False

        self.listen_keyboard_enabled = True

        if os.environ.get("DRYPIPE_CLI_LISTEN_KEYBOARD_ENABLED") == "False":
            self.listen_keyboard_enabled = False

        if group_by is not None:

            g = pipeline.task_groupers.get(group_by)
            if g is None:
                raise Exception(
                    f"pipeline was not instantiated with task_groupers dict containing {group_by}"
                )
            self.selected_grouper_name = group_by

        class ShallowPipelineInstance:
            def __init__(self):
                self.pipeline_instance_dir = pipeline_instance_dir

        self.pipeline_instance = ShallowPipelineInstance()

    def fetch_failed_task_states(self):
        if self.failed_task_states_iter_count % 3 == 0:
            ts = [
                task_state for task_state in TaskState.failed_task_states(self.pipeline_instance)
            ]
            self.failed_task_states = sorted(ts, key=lambda s: s.task_key)
            if self.selected_failed_task is None and len(self.failed_task_states) > 0:
                self._set_selected_failed_task(0)

        self.failed_task_states_iter_count += 1

    def _set_selected_failed_task(self, idx):
        self.selected_failed_task = self.failed_task_states[idx]

    def _selected_failed_task_index(self):
        if self.selected_failed_task is None:
            return None

        for i in range(0, len(self.failed_task_states)):
            if self.failed_task_states[i].task_key == self.selected_failed_task.task_key:
                return i

    def restart_failed_task(self):
        self.selected_failed_task.transition_to_queued()

    def task_up(self):
        c = len(self.failed_task_states)
        if c == 0:
            return
        idx = self._selected_failed_task_index()
        if idx is None or idx == 0:
            self._set_selected_failed_task(0)
        else:
            idx -= 1
            if idx < c:
                self._set_selected_failed_task(idx)

    def task_down(self):
        c = len(self.failed_task_states)
        if c == 0:
            return
        idx = self._selected_failed_task_index()
        if idx is None:
            self._set_selected_failed_task(0)
        else:
            idx += 1
            if idx < c:
                self._set_selected_failed_task(idx)
            else:
                self._set_selected_failed_task(c - 1)

    def request_quit(self):
        self.quit = True

    def press(self, key):
        logger.debug("key pressed: %s", key)
        try:
            if key == "q" or key == readchar.key.CTRL_C:
                self.request_quit()
            elif key == readchar.key.UP:
                logger.debug("UP")
            elif key == readchar.key.DOWN:
                logger.debug("DOWN")
            elif key == "f":
                self.cancel_prompt()
                self.screen = "errors"
            elif key == "s":
                self.cancel_prompt()
                self.screen = "summary"
            elif key == "r":
                if self.selected_failed_task is not None:
                    self.set_prompt_restart_task()
            elif key == "n":
                self.cancel_prompt()
            elif key == "y":
                if self.is_prompt_restart_task():
                    self.restart_failed_task()
            elif key == "up":
                self.cancel_prompt()
                self.task_up()
            elif key == "down":
                self.cancel_prompt()
                self.task_down()
        except Exception:
            logger.exception(f"failed on key command {key}")
            self.error_msg.append(traceback.format_exc())

    def is_prompt_restart_task(self):
        return self.prompt == "restart-task"

    def set_prompt_restart_task(self):
        self.prompt = "restart-task"

    def cancel_prompt(self):
        self.prompt = None

    def start(self):
        def refresher():
            refresh_per_second = 4
            if self.rich_live_auto_refresh:
                refresh_per_second = 1
            with Live(auto_refresh=self.rich_live_auto_refresh, refresh_per_second=refresh_per_second) as live:
                try:
                    while not self.quit:
                        self.update_screen(live)
                        if not self.quit:
                            time.sleep(3)
                except Exception:
                    logger.exception(f"failed in refresh loop")
                    self.error_msg.append(traceback.format_exc())

            logger.info(f"out of Live loop")

            self.quit_listener()

        logger.info("will start screen refresher thread")
        refresh_thread = Thread(target=refresher)
        refresh_thread.start()

        if self.listen_keyboard_enabled:

            readchar.config.INTERRUPT_KEYS = []

            def _listen_keyboard():
                logger.info("_listen_keyboard thread started")
                while True:
                    c = readchar.readkey()
                    self.press(c)

                    if self.quit:
                        break
                    if c == "q":
                        break

                logger.debug("out of _listen_keyboard loop")

            listen_keyboard_thread = Thread(target=_listen_keyboard)
            listen_keyboard_thread.start()
        else:
            logger.info("will NOT listen_keyboard")

    def _errors_screen(self):

        failed_cnt = len(self.failed_task_states)

        layout = self._main_screen()

        if failed_cnt == 0:
            layout["header3"].update("Failed Tasks")
            layout["body"].update("...there are no failed tasks")
            self.cancel_prompt()
            return layout

        def get_header_text():

            if not self.is_prompt_restart_task():
                t = Text()
                t.append(f"Task({self.selected_failed_task.task_key}) -> ", style="bold magenta")
                err_file = os.path.join(self.selected_failed_task.control_dir(), "err.log")
                t.append(f"tail -50 {err_file}\n", style="bold magenta")
                t.append(f"key r to restart", style="bold gree1")
                return t
            else:
                t = Text()
                t.append(f"Restart task {self.selected_failed_task.task_key} ? y/N/a (a=all failed tasks)", style="bold green1")
                return t

        def get_tail_text():
            t = Text()
            t.append(self.selected_failed_task.tail_err_if_failed(50), style="red")
            return t

        layout["header3"].update(get_header_text())
        layout["body"].update(get_tail_text())

        return layout

    def _main_screen(self):
        layout = Layout()
        layout.split_column(
            Layout(name="header1", size=1),
            Layout(name="header2", size=1),
            Layout(name="header3", size=1),
            Layout(name="body", ratio=10),
            Layout(name="footer", size=1)
        )

        pipeline_mod_func = self.pipeline_hints.get("pipeline")
        if self.is_monitor_mode:
            console_mode = f"  [bright_yellow]console in monitor mode (no launching)[/]  ({self.loop_counter})"
        else:
            console_mode = f"  [bright_yellow]console in launcher mode[/]  ({self.loop_counter})"

        if pipeline_mod_func is not None:
            title = f"[bold]DryPipe([green1]{pipeline_mod_func}[/], "
            title += f"[cyan1]{self.pipeline_instance.pipeline_instance_dir}[/])  {console_mode}"
        else:
            title = f"DryPipe - {console_mode}"

        help_panel = Columns([
            "command keys: "
            "[b]s[/]: summary",
            "[b]f[/]: failed tasks",
            "[b]q[/]: quit"
        ], equal=True, expand=False)

        layout["header1"].update(Align(title, align="center"))
        layout["header2"].update(Align(help_panel, align="left"))

        return layout

    def _status_table(self):

        table = Table(
            show_header=True,
            header_style="none",
            show_lines=False,
            show_edge=False,
            expand=True
        )

        task_grouper = self.task_groupers[self.selected_grouper_name]

        header, body, footer = PipelineMetricsTable.summarized_table_from_task_group_metrics(
            fetch_task_group_metrics(
                self.pipeline_instance.pipeline_instance_dir,
                task_grouper
            )
        )

        for h in header:
            table.add_column(h)

        def color_row(row):
            c = 0
            row_cells = []
            for v in row:
                if v is None or v == 0:
                    row_cells.append("")
                else:
                    if c == 4:
                        row_cells.append(f"[green1]{v}[/]")
                    elif c == 5:
                        row_cells.append(f"[blue]{v}[/]")
                    elif c in [6, 7, 8]:
                        row_cells.append(f"[red]{v}[/]")
                    elif c in [0, 1]:
                        row_cells.append(f"[yellow]{v}[/]")
                    else:
                        row_cells.append(f"{v}")
                c += 1
            return row_cells

        for row in body:
            table.add_row(*color_row(row))

        table.add_row(*color_row(footer))

        layout = self._main_screen()

        layout["header3"].update("Task Execution Status Summary")
        layout["body"].update(table)

        return layout

    def update_screen(self, live):
        self.loop_counter += 1
        logger.debug("will update screen %s, %s", self.loop_counter, self.screen)
        if self.screen == 'summary':
            l = self._status_table()
        elif self.screen == "errors":
            self.fetch_failed_task_states()
            l = self._errors_screen()
        elif self.screen == "restart-task":
            l = self._errors_screen()
        else:
            raise Exception(f"unknown screen {self.screen}")

        live.update(Panel(l, border_style="blue"), refresh=not self.rich_live_auto_refresh)
        logger.debug("screen %s updated", self.screen)

