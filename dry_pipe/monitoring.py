import itertools
import os
import statistics

from dry_pipe import Task
from dry_pipe.task_state import TaskState, VALID_TRANSITIONS, STATE_EQUIVALENCE_FOR_DISPLAY, \
    STATE_EQUIVALENCE_FOR_DISPLAY_WEB


class Counter:

    def __init__(self):
        self.value = 0

    def inc(self):
        self.value += 1


class TaskGroupMetric:

    def __init__(self, task_group_key, display_order_key):
        self.count_per_state_name = {}
        self.task_group_key = task_group_key
        self.display_order_key = display_order_key

    def inc(self, state_name):
        counter = self.count_per_state_name.get(state_name)

        if counter is None:
            counter = Counter()
            self.count_per_state_name[state_name] = counter

        counter.inc()

    def counts(self, state_names_groups):

        def f(counter):
            if counter is None:
                return 0
            else:
                return counter.value

        def count_states_names(state_names):
            c = 0
            for state_name in state_names:
                c += f(self.count_per_state_name.get(state_name))

            return c

        return [
            count_states_names(state_names)
            for state_names in state_names_groups
        ]


class PipelineMetricsTable:

    @staticmethod
    def detailed_table_from_task_group_metrics(task_group_metrics):

        all_states = [
            s
            for s in VALID_TRANSITIONS.keys()
            if s != ""
        ]

        all_states_groups = [
            [s]
            for s in all_states
        ]

        return PipelineMetricsTable._table_from_task_group_metrics(
            all_states,
            all_states_groups,
            task_group_metrics
        )

    @staticmethod
    def summarized_table_from_task_group_metrics(task_group_metrics):
        return PipelineMetricsTable._table_from_task_group_metrics(
            TaskState.state_display_names(),
            TaskState.display_groups_of_state_names(),
            task_group_metrics
        )

    @staticmethod
    def _table_from_task_group_metrics(state_display_names, display_groups_of_state_names, task_group_metrics):

        header_row = ["task group"] + state_display_names + ["total"]

        count_totals = [
            0
            for n in display_groups_of_state_names
        ]

        table_body = []

        for task_group_metric in task_group_metrics:

            row = [task_group_metric.task_group_key]

            counts = task_group_metric.counts(display_groups_of_state_names)

            for i in range(0, len(count_totals)):
                count_totals[i] += counts[i]
                row.append(counts[i])

            row.append(sum(counts))

            table_body.append(row)

        footer = [None] + count_totals + [sum(count_totals)]

        return header_row, table_body, footer

    @staticmethod
    def totals_row(pipeline_instance_dir):

        res = {
            s: 0
            for s in TaskState.state_display_names_web
        }

        for task_state in TaskState.fetch_all(pipeline_instance_dir=pipeline_instance_dir):

            display_state = STATE_EQUIVALENCE_FOR_DISPLAY_WEB[task_state.state_name]

            res[display_state] += 1

        return res


def fetch_task_group_metrics(pipeline_instance_dir, task_key_grouper, task_state_visitor=None, task_filter=None):

    task_group_metrics = {}

    for task_state in TaskState.fetch_all(pipeline_instance_dir):

        if task_filter is not None:
            if not task_filter(task_state.task_key):
                continue

        if task_state_visitor is not None:
            task_state_visitor(task_state)

        task_group_key = task_key_grouper(task_state.task_key)

        m = task_group_metrics.get(task_group_key)

        if m is None:
            m = TaskGroupMetric(task_group_key, os.path.getctime(task_state.control_dir()))
            task_group_metrics[task_group_key] = m

        m.inc(task_state.state_name)

    return sorted(
        task_group_metrics.values(),
        key=lambda group: group.display_order_key
    )


def fetch_task_groups_stats(pipeline_instance_dir, no_header=False, units='seconds'):

    def gen_step_durations():

        for task_state in TaskState.fetch_all(pipeline_instance_dir):

            task_group_key = Task.key_grouper(task_state.task_key)

            def row_name(history_row):
                return history_row[0]

            def row_time(history_row):
                return TaskState.parse_history_timestamp(history_row[1])

            def row_step_number(history_row):
                return int(history_row[2])

            def step_intervals():
                for history_row in task_state.load_history_rows():
                    if row_name(history_row) in ["step-started", "step-completed"]:
                        yield history_row

            for step_number, rows, in itertools.groupby(
                    sorted(step_intervals(), key=row_step_number),
                    key=row_step_number
            ):
                rows = list(rows)
                if len(rows) < 2:
                    continue
                last_step_start_row = rows[-2]
                last_step_completion_row = rows[-1]

                if row_name(last_step_start_row) != "step-started":
                    continue

                if row_name(last_step_completion_row) != "step-completed":
                    continue

                total_time = (row_time(last_step_completion_row) - row_time(last_step_start_row)).total_seconds()
                yield f"{task_group_key}:{step_number}", total_time

    if not no_header:
        yield "task:step\tmin_t\tmax_t\ttotal_t\tavg_t\ts_dev"

    if units == "minutes":
        unit_converter = lambda i: round(i / 60, 2)
    elif units == "seconds":
        unit_converter = lambda i: round(i)
    elif units == 'hours':
        unit_converter = lambda i: round(i / (60 * 60), 2)
    else:
        raise Exception(f"unknown units: {units}, valid units are 'seconds', 'minutes', 'hours' ")

    for step_name, step_duration_tuples in itertools.groupby(
            sorted(gen_step_durations(), key=lambda t: t[0]),
            key=lambda t: t[0]
    ):

        step_durations = sorted([
            unit_converter(time) for s, time in step_duration_tuples
        ])

        min_t = step_durations[0]
        max_t = step_durations[-1]
        total_t = sum(step_durations)
        avg_t = total_t / len(step_durations)
        s_dev = 0 if len(step_durations) == 1 else statistics.stdev(step_durations)

        yield (
            step_name,
            unit_converter(min_t),
            unit_converter(max_t),
            unit_converter(total_t),
            unit_converter(avg_t),
            s_dev
        )
