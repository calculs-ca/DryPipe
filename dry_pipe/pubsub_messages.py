import os
import time

from dry_pipe import Task
from dry_pipe.actions import TaskAction
from dry_pipe.monitoring import PipelineMetricsTable, fetch_task_group_metrics
from dry_pipe.pipeline_state import PipelineState
from dry_pipe.task_state import TaskState


def all_pipeline_states_as_json(instances_dir):
    return [
        {
            "dir": pipeline_state.dir_basename(),
            "state": pipeline_state.state_name,
            "totals": pipeline_state.totals_row()
        }
        for pipeline_state in PipelineState.iterate_from_instances_dir(instances_dir)
        if not pipeline_state.is_completed()
    ]


def task_details_message(instances_dir, pid_task_key):

    pid, task_key = pid_task_key.split("|")

    task_state = TaskState.from_task_control_dir(
        os.path.join(instances_dir, pid), task_key
    )

    return task_state.as_json()


def pipeline_counts_message(instances_dir, pid):

    def prepare_counts_message(pipeline_instance_dir):

        pipeline_state = PipelineState.from_pipeline_instance_dir(pipeline_instance_dir)
        instance_dir = pipeline_state.dir_basename()

        actions_by_task_key = {
            task_action.task_key: task_action
            for task_action in TaskAction.fetch_all_actions(pipeline_state.instance_dir())
        }

        def create_visitor():

            partial_task_infos = []

            def visit_task(task_state):

                o = {
                    "key": task_state.task_key,
                    "state_name": task_state.state_name,
                    "step": task_state.step_number()
                }

                err_tail = [] #task_state.tail_err_if_failed(3)

                if err_tail is not None:
                    o["err_tail"] = err_tail

                action = actions_by_task_key.get(task_state.task_key)

                if action is not None:
                    o["action"] = action.action_name

                partial_task_infos.append(o)

            return partial_task_infos, visit_task

        partial_task_infos, task_state_visitor = create_visitor()

        snapshot_time = int(time.time_ns())

        return {
            # SHOULD SEND DAG HERE ?
            # "dag": pipeline.summarized_dependency_graph(),
            "tsv": all_task_states_as_tsv(pipeline_state.instance_dir(), task_state_visitor),
            "pipelineDir": pipeline_state.dir_basename(),
            "partial_task_infos": partial_task_infos,
            "snapshot_time": snapshot_time
        }

    def all_task_states_as_tsv(pipeline_instance_dir, task_state_visitor=None):
        header, table_body, footer = PipelineMetricsTable.detailed_table_from_task_group_metrics(
            fetch_task_group_metrics(
                pipeline_instance_dir,
                Task.key_grouper,
                task_state_visitor=task_state_visitor
            )
        )

        def format_cell(c):
            return str(c or 0)

        res = "\n".join([
            "\t".join([
                format_cell(cell) for cell in row
            ])
            for row in table_body
        ])

        header = '\t'.join(header[1:-1])

        return f"{header}\n{res}"

    pipeline_state = PipelineState.from_pipeline_instance_dir(os.path.join(instances_dir, pid))

    return prepare_counts_message(pipeline_state.instance_dir())
