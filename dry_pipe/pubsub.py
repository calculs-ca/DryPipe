import logging
from itertools import groupby

DRYPIPE_WEBSOCKET_NAMESPACE = "/drypipe"

logger = logging.getLogger(__name__)


def _example_subscription():
    return {
        "sid": "7bo6ke91DIxk-8V4AAAz",
        "pid": "myPipeline",
        "task_key": "myTaskKey"
    }


def subscription_reducer(state, action):

    action_name = action["name"]

    sid = action["sid"]

    if action_name == 'init':
        return {
            "sid": sid
        }
    elif action_name == 'observePipeline':
        return {
            "sid": sid,
            "pid": action["pid"]
        }
    elif action_name == 'unObservePipeline':
        return {
            "sid": sid
        }
    elif action_name == 'observeTask':
        return {
            ** state,
            "sid": sid,
            "task_key": action["task_key"]
        }
    elif action_name == 'unObserveTask':
        if "pid" in state:
            return {
                "sid": sid,
                "pid": state["pid"]
            }
        else:
            return {
                "sid": sid
            }
    elif action_name == 'close':
        return {
            "sid": sid,
            "is_closed": True
        }
    else:
        raise Exception(f"unknown action {action_name}")


def subscription_registry_reducer(state, action):

    if state is None:

        assert action is None

        return None, {
            "sids_to_subscriptions": {},
            "pids_to_sids": {},
            "task_keys_to_sids": {}
        }

    action_name = action["name"]

    updated_subscription = None

    prev_sids_to_subscriptions = state["sids_to_subscriptions"]

    def prev_subscription():
        return prev_sids_to_subscriptions[sid()]

    def sid():
        return action["sid"]

    if action_name == 'init':

        updated_subscription = subscription_reducer(None, action)

        sids_to_subscriptions = {
            ** prev_sids_to_subscriptions,
            sid(): updated_subscription
        }

    elif action_name == 'close':

        prev_sid = sid()
        sids_to_subscriptions = {
            sid: subscription
            for sid, subscription in prev_sids_to_subscriptions.items()
            if sid != prev_sid
        }

    elif action_name == "reset_from_sids_to_subscriptions":

        sids_to_subscriptions = action["sids_to_subscriptions"]

    elif action_name == "close_sessions":

        sids_to_close = action["sids"]

        sids_to_subscriptions = {
            sid_k: subscription
            for sid_k, subscription in prev_sids_to_subscriptions.items()
            if sid_k not in sids_to_close
        }

    else:

        updated_subscription = subscription_reducer(
            prev_subscription(), action
        )

        sids_to_subscriptions = {
            ** prev_sids_to_subscriptions,
            sid(): updated_subscription
        }

    def dict_of_sids_by_other_keys(other_key):

        def gen_tuples_other_key_to_sid():

            for sid, subscription in sids_to_subscriptions.items():

                task_key = subscription.get(other_key)

                if task_key is not None:
                    yield task_key, subscription["sid"]

        def group_func(o):
            return o[0]

        return {
            other_k: [t[1] for t in sid_tuples]
            for other_k, sid_tuples in groupby(sorted(gen_tuples_other_key_to_sid(), key=group_func), key=group_func)
        }

    return updated_subscription, {
        "sids_to_subscriptions": sids_to_subscriptions,
        "pids_to_sids": dict_of_sids_by_other_keys("pid"),
        "task_keys_to_sids": dict_of_sids_by_other_keys("task_key")
    }


class SubscriptionRegistry:

    instance = None

    @staticmethod
    def init():
        SubscriptionRegistry.instance = SubscriptionRegistry()

    def __init__(self):
        z, self.subscription_registry = subscription_registry_reducer(None, None)

    def update_subscription(self, action_from_browser):

        subscription, self.subscription_registry = subscription_registry_reducer(
            self.subscription_registry, action_from_browser
        )

        return subscription

    def close_sessions(self, sids):

        z, self.subscription_registry = subscription_registry_reducer(
            self.subscription_registry, {"name": "close_sessions", "sids": sids}
        )

    def sids_to_subscriptions(self):
        return self.subscription_registry["sids_to_subscriptions"]

    def reset_subscriptions(self, sids_to_subscriptions):
        z, self.subscription_registry = subscription_registry_reducer(self.subscription_registry, {
            "name": "reset_from_sids_to_subscriptions",
             "sids_to_subscriptions": sids_to_subscriptions
        })

    def pids_to_sids(self):
        return self.subscription_registry["pids_to_sids"]

    def task_keys_to_sids(self):
        return self.subscription_registry["task_keys_to_sids"]

    def sids(self):
        return list(self.sids_to_subscriptions().keys())
