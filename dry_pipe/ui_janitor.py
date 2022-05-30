import logging
import os

from dry_pipe.actions import TaskAction
from dry_pipe.pubsub import SubscriptionRegistry
from dry_pipe.pubsub_messages import all_pipeline_states_as_json, task_details_message, pipeline_counts_message

logger = logging.getLogger(__name__)


class UIJanitor:

    def __init__(self, instances_dir):
        self.instances_dir = instances_dir

    def _pack_message(self, event, sids, observed_key, data):

        logger.debug("packing message %s for %s", event, sids)

        return {
            "event": event,
            "sids": sids,
            "observed_key": observed_key,
            "data": data
        }

    def messages_for_round(self):

        sids = SubscriptionRegistry.instance.sids()

        if len(sids) > 0:
            yield self._pack_message(
                "running-pipelines", sids, "", all_pipeline_states_as_json(self.instances_dir)
            )

        for task_key, sids in SubscriptionRegistry.instance.task_keys_to_sids().items():
            yield self._pack_message(
                "latestTaskDetails", sids, task_key, task_details_message(self.instances_dir, task_key)
            )

        for pid, sids in SubscriptionRegistry.instance.pids_to_sids().items():
            yield self._pack_message(
                "latestPipelineDetailedState", sids, pid, pipeline_counts_message(self.instances_dir, pid)
            )

    def message_after_subscription_update(self, action_from_browser):

        if action_from_browser["name"] == "observePipeline":

            pid = action_from_browser["pid"]

            yield self._pack_message(
                "latestPipelineDetailedState",
                [action_from_browser["sid"]],
                pid,
                pipeline_counts_message(self.instances_dir, pid)
            )

        elif action_from_browser["name"] == "observeTask":

            task_key = action_from_browser["task_key"]

            yield self._pack_message(
                "latestTaskDetails",
                [action_from_browser["sid"]],
                task_key,
                task_details_message(self.instances_dir, task_key)
            )

    def task_action_submitted(self, sid, message):

        task_key = message['task_key']
        pipeline_dir = message['pipeline_dir']
        action_name = message['action_name']
        is_cancel = message.get("is_cancel")

        pipeline_instance_dir = os.path.join(self.instances_dir, pipeline_dir)

        if is_cancel is not None and is_cancel:
            action = TaskAction.load_from_task_control_dir(
                os.path.join(pipeline_instance_dir, ".drypipe", task_key)
            )

            if action is not None:
                action.delete()

        else:
            TaskAction.submit(pipeline_instance_dir, task_key, action_name, message.get("step"))

        return self._pack_message(
            "taskActionSubmitted", [sid], task_key, message
        )
