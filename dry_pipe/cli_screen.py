import datetime
import logging
import os
import sys
import termios
import time
import traceback
from queue import LifoQueue
from threading import Thread

import readchar
from rich.columns import Columns
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.align import Align
from rich import box


from dry_pipe.monitoring import PipelineMetricsTable, fetch_task_group_metrics
from dry_pipe.pipeline import PipelineInstance, Pipeline
from dry_pipe.script_lib import ps_resources
from dry_pipe.task_state import TaskState

logger = logging.getLogger(__name__)


class CliScreen:

    def __init__(self, pipeline_instance_dir, regex_grouper, is_monitor_mode, quit_listener=None, group_by=None):
        self.is_monitor_mode = is_monitor_mode
        self.pipeline_instance_dir = pipeline_instance_dir
        self.pipeline_hints = PipelineInstance.load_hints(pipeline_instance_dir)
        self.quit = False
        self.screen = Summary(self)
        self.selected_failed_task = None
        self.prompt = None
        self.error_msg = []
        self.loop_counter = 0
        self.rich_live_auto_refresh = True
        self.quit_listener = quit_listener

        stdin_fd = sys.stdin.fileno()
        old_tty_settings = termios.tcgetattr(stdin_fd)

        self.cleanup_tty = lambda:\
            termios.tcsetattr(stdin_fd, termios.TCSADRAIN, old_tty_settings)

        self.queue = LifoQueue(maxsize=2)

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

    def request_quit(self):
        self.quit = True

    def press(self, key):
        logger.debug("key pressed: %s", key)
        try:
            if key == "q" or key == readchar.key.CTRL_C:
                self.request_quit()
                self.queue.put("m")
            elif key == readchar.key.UP:
                logger.debug("UP")
                self.queue.put("up")
            elif key == readchar.key.DOWN:
                self.queue.put("down")
                logger.debug("DOWN")
            elif key == readchar.key.ENTER:
                self.queue.put("enter")
            elif key == readchar.key.LEFT:
                self.queue.put("left")
            elif key == "f":
                self.queue.put("failed-screen")
            elif key == "s":
                self.queue.put("summary-screen")
            elif key == "r":
                self.queue.put("restart")
            elif key == "y":
                self.queue.put("y")
            elif key == "n":
                self.queue.put("n")
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
                        msg = self.queue.get()
                        if msg == "failed-screen":
                            self.screen = FailedTaskList(self)
                            self.screen.reload()
                        elif msg == "summary-screen":
                            self.screen = Summary(self)
                            self.screen.reload()

                        self.screen.refresh(live, self._main_screen(), msg)

                except Exception:
                    self.request_quit()
                    logger.exception(f"failed in refresh loop")
                    self.error_msg.append(traceback.format_exc())

            logger.info(f"out of Live loop")

            self.quit_listener(self)

        logger.info("will start screen refresher thread")
        Thread(target=refresher).start()

        def data_loader_thread():
            while not self.quit:
                try:
                    self.screen.reload()
                    self.queue.put("reload")
                    time.sleep(3)
                except Exception as ex:
                    self.request_quit()
                    logger.exception(ex)
                    break

        Thread(target=data_loader_thread).start()

        if self.listen_keyboard_enabled:

            readchar.config.INTERRUPT_KEYS = []

            def _listen_keyboard():
                logger.info("_listen_keyboard thread started")
                while True:
                    try:
                        c = readchar.readkey()
                        self.press(c)

                        if self.quit:
                            break
                        if c == "q":
                            break
                    except Exception as ex:
                        self.cleanup_tty()
                        self.request_quit()
                        logger.exception(ex)
                        break

                logger.debug("out of _listen_keyboard loop")

            listen_keyboard_thread = Thread(target=_listen_keyboard)
            listen_keyboard_thread.start()
        else:
            logger.info("will NOT listen_keyboard")


    def _main_screen(self):
        layout = Layout()
        layout.split_column(
            Layout(name="header1", size=1),
            Layout(name="header2", size=1),
            Layout(name="header3", size=1),
            Layout(name="header4", size=1),
            Layout(name="body", ratio=10),
            Layout(name="footer", minimum_size=1)
        )

        layout["footer"].update("+++")

        pipeline_mod_func = self.pipeline_hints.get("pipeline")
        if self.is_monitor_mode:
            console_mode = f"  [bright_yellow]console in monitor mode (no launching)[/]  ({self.loop_counter})"
        else:
            console_mode = f"  [bright_yellow]console in launcher mode[/]  ({self.loop_counter})"

        if pipeline_mod_func is not None:
            title = f"[bold]DryPipe([green1]{pipeline_mod_func}[/], "
            title += f"[cyan1]{self.pipeline_instance_dir}[/])  {console_mode}"
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


def styled_prompt(question, choices, default):
    return f"[cyan1]{question}[/] [purple]{','.join(choices)}[/] ({default}):"


class TaskView:

    def __init__(self, prev_screen, task_key):
        self.cli_screen = prev_screen.cli_screen
        self.prev_screen = prev_screen
        self.task_key = task_key
        self.task_state = TaskState.from_task_control_dir(
            self.cli_screen.pipeline_instance_dir, self.task_key
        )
        self.task_resources_view = TaskResourcesView(self.task_state)
        self.prompt_restart = False
        self.restart_in_progress = False

    def refresh(self, live, layout, msg=None):
        if msg == "left":
            self.cli_screen.screen = self.prev_screen
            self.cli_screen.screen.reload()
            self.cli_screen.screen.refresh(live, layout)
        else:
            if self.prompt_restart:
                if msg == "y":
                    self.prompt_restart = False
                    self.restart_in_progress = True
                    self.task_state.transition_to_queued()
                elif msg == "n":
                    self.prompt_restart = False

            if msg == "restart":
                self.prompt_restart = True


            out, err, log = self.data

            def g():
                for step_number, started, completed_or_none in self.task_state.history_steps_time_intervals():
                    if completed_or_none is None:
                        yield f"step-{step_number}(started at {started.strftime('%Hh:%M:%S%z')}, ...)"
                    else:
                        dt = (completed_or_none - started)
                        dt = datetime.timedelta(seconds=round(dt.total_seconds(), 0) or 1)
                        yield f"step-{step_number}({dt})"

            history_line = " -> ".join(g())

            def task_attr(v):
                return f"[cyan1]{v}[/]"

            task_title = f"Task(key={task_attr(self.task_key)}, state={task_attr(self.task_state.state_name)}"

            if self.task_resources_view.pid is not None:
                task_title += f", pid={task_attr(self.task_resources_view.pid)})"
            elif self.task_resources_view.slurm_job_id is not None:
                task_title += f", slurm_job_id={task_attr(self.task_resources_view.slurm_job_id)})"
            else:
                task_title += ")"

            layout["header3"].update(Align(task_title, align="center"))

            if self.prompt_restart:
                layout["header4"].update(Align(styled_prompt("restart task ?", ["y", "n"], "y"), align="center"))
            elif self.restart_in_progress:
                layout["header4"].update(Align("...restarting"))
                self.restart_in_progress = False
            else:
                layout["header4"].update(Text(f"History[{history_line}]"))

            task_ctrl_layout = Layout()
            task_ctrl_layout.split_column(
                Text("r: restart"),
                Text("k: kill (SIGTERM)"),
            )

            layout3 = Layout()
            layout3.split_column(
                self.task_resources_view.render(),
                Panel(Text(log, style="blue"), title="drypipe janitor log(tail -f drypipe.log)")
            )
            main_layout = Layout()
            main_layout.split_row(
                Panel(Text(out, style="green"), title="tail -f out.log"),
                Panel(Text(err, style="red"), title="tail -f err.log"),
                layout3
            )
            layout["body"].update(main_layout)
            live.update(Panel(layout, border_style="blue"), refresh=True)

    def reload(self):
        self.task_state = TaskState.from_task_control_dir(
            self.cli_screen.pipeline_instance_dir, self.task_key
        )
        out, err, log = self.task_state.tail_out_err_drypipe()
        self.data = [out, err, log]
        self.task_resources_view.reload()


class TaskResourcesView:

    def __init__(self, task_state):
        self.task_state = task_state
        self.pid = None
        self.slurm_job_id = None
        self.task_ps_resources = None

    def reload(self):
        self.pid = self.task_state.pid()
        if self.pid is not None:
            r = list(ps_resources(self.pid))
            if len(r) == 0:
                self.task_ps_resources = None
            else:
                self.task_ps_resources = r

        else:
            self.slurm_job_id = self.task_state.slurm_job_id()
            if self.slurm_job_id is not None:
                self.task_ps_resources = [["na"], ["na"]]
            else:
                self.task_ps_resources = None

    def render(self):
        if self.task_ps_resources is None:
            return Panel(Text("Task Not Running"))
        else:
            resource_headers, resource_values = self.task_ps_resources
            resource_table = Table(
                show_header=True,
                header_style="none",
                expand=True,
                box=box.ROUNDED
            )
            for h in resource_headers:
                resource_table.add_column(h, style="bold")
            resource_table.add_row(*[str(v) for v in resource_values], style="green1")
            return resource_table


class FailedTaskList:

    def __init__(self, cli_screen):
        self.selected_row = None
        self.cli_screen = cli_screen
        self.selected_task_key = None
        self.failed_task_states = []

    def refresh(self, live, layout, msg=None):

        if msg in ["up", "down"]:
            if self.selected_row is None:
                self.selected_row = 0
            else:
                if msg == "up":
                    if self.selected_row > 0:
                        self.selected_row -= 1
                else:
                    row_count = len(self.failed_task_states)
                    if self.selected_row < (row_count - 1):
                        self.selected_row += 1
        elif msg == "enter":
            if self.selected_row is None:
                self.selected_row = 0
            self.cli_screen.screen = TaskView(self, self.failed_task_states[self.selected_row].task_key)
            self.cli_screen.screen.reload()
            self.cli_screen.screen.refresh(live, layout)
            return
        elif msg == "restart":
            #TODO
            pass

        elif msg == "left":
            self.cli_screen.press("s")

        layout["header3"].update("Failed Tasks")

        fail_count = len(self.failed_task_states)
        if fail_count == 0:
            layout["header4"].update(Text(f"there are no failed tasks"))
        else:
            layout["header4"].update("")

        table = Table(
            show_header=False,
            expand=True,
            show_lines=True
        )
        table.add_column(ratio=1)
        table.add_column(ratio=9)
        c = 0
        for task_state in self.failed_task_states:
            err = task_state.tail_err_if_failed(4)
            if self.selected_row == c:
                selected_prefix = " -> "
                selected_style = "bold"
            else:
                selected_prefix = "    "
                selected_style = ""

            table.add_row(
                Text(f"{selected_prefix} {task_state.task_key}"),
                Text(err, style=f"red"),
                style=selected_style
            )
            c += 1

        layout["body"].update(table)
        live.update(Panel(layout, border_style="blue"), refresh=True)

    def reload(self):
        self.failed_task_states = sorted(
            [
                task_state
                for task_state in TaskState.failed_task_states(self.cli_screen.pipeline_instance_dir)
            ],
            key=lambda s: s.task_key
        )


class Summary:

    def __init__(self, cli_screen):
        self.selected_row = None
        self.cli_screen = cli_screen
        self.data = None
        self.group_selected = None
        self.selected_task_key = None

    def refresh(self, live, layout, msg=None):
        if self.data is None:
            time.sleep(1)
        else:

            header, body, footer = self.data

            if msg in ["up", "down"]:
                if self.selected_row is None:
                    self.selected_row = 0
                else:
                    if msg == "up":
                        if self.selected_row > 0:
                            self.selected_row -= 1
                    else:
                        row_count = len(body)
                        if self.selected_row < (row_count - 1):
                            self.selected_row += 1
            elif msg == "enter":
                if self.group_selected is None:
                    if self.selected_row is not None:
                        self.group_selected = body[self.selected_row][0]
                        self.selected_row = None
                        logger.debug(f"group_selected %s", self.group_selected)
                        header, body, footer = self.reload()
                else:
                    if self.selected_row is None:
                        self.selected_row = 0
                    self.cli_screen.screen = TaskView(self, body[self.selected_row][0])
                    self.cli_screen.screen.reload()
                    self.cli_screen.screen.refresh(live, layout)
                    return

            elif msg == "left" and self.group_selected is not None:
                self.group_selected = None
                logger.debug(f"group_selected %s", self.group_selected)
                header, body, footer = self.reload()

            table = Table(
                show_header=True,
                header_style="none",
                show_lines=False,
                show_edge=False,
                expand=True
            )

            for h in header:
                table.add_column(h)

            def color_row(row, is_selected=False):
                c = 0
                row_cells = []
                for v in row:
                    selected_style = "bold" if is_selected else ""
                    if v is None or v == 0:
                        row_cells.append("")
                    else:
                        if c == 4:
                            row_cells.append(f"[green1 {selected_style}]{v}[/]")
                        elif c == 5:
                            row_cells.append(f"[blue {selected_style}]{v}[/]")
                        elif c in [6, 7, 8]:
                            row_cells.append(f"[red {selected_style}]{v}[/]")
                        elif c == 0:

                            if self.group_selected is None:
                                parent_group = ""
                            else:
                                parent_group = f"{self.group_selected}/"
                            selected_cursor = "-> " if is_selected else "   "
                            row_cells.append(f"[yellow {selected_style}]{selected_cursor}{parent_group}{v}[/]")
                        elif c == 1:
                            row_cells.append(f"[yellow {selected_style}]{v}[/]")
                        else:
                            row_cells.append(f"[yellow {selected_style}]{v}[/]")
                    c += 1
                return row_cells

            r = 0
            for row in body:
                is_selected = self.selected_row is not None and self.selected_row == r
                table.add_row(*color_row(row, is_selected))
                r += 1

            table.add_row(*color_row(footer))

            layout["header3"].update("Task Execution Status Summary")
            layout["header4"].update("")
            layout["body"].update(table)
            live.update(Panel(layout, border_style="blue"), refresh=True)

    def reload(self):
        task_grouper = self.cli_screen.task_groupers[self.cli_screen.selected_grouper_name]

        if self.group_selected is None:
            header, body, footer = PipelineMetricsTable.summarized_table_from_task_group_metrics(
                fetch_task_group_metrics(
                    self.cli_screen.pipeline_instance.pipeline_instance_dir,
                    task_grouper
                )
            )
        else:
            def task_filter(task_key):
                task_group = task_grouper(task_key)
                return task_group == self.group_selected

            header, body, footer = PipelineMetricsTable.summarized_table_from_task_group_metrics(
                fetch_task_group_metrics(
                    self.cli_screen.pipeline_instance.pipeline_instance_dir,
                    task_key_grouper=lambda i: i,
                    task_filter=task_filter
                )
            )

        self.data = [header, body, footer]
        return header, body, footer

