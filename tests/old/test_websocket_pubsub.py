
import unittest

from dry_pipe.pubsub import subscription_reducer, subscription_registry_reducer


class WebSocketPubSubTests(unittest.TestCase):

    def test_subscription_reducer(self):

        state = {}

        state = subscription_reducer(state, {
            "name": "init",
            "sid": "s1"
        })

        self.assertDictEqual({
            "sid": "s1"
        }, state)

        state = subscription_reducer(state, {
            "name": "observePipeline",
            "pid": "pA",
            "sid": "s1"
        })

        state = subscription_reducer(state, {
            "name": "observeTask",
            "task_key": "tA",
            "sid": "s1"
        })

        self.assertDictEqual({
            "sid": "s1",
            "pid": "pA",
            "task_key": "tA"
        }, state)

        state = subscription_reducer(state, {
            "name": "unObserveTask"
        })

        self.assertDictEqual({
            "sid": "s1",
            "pid": "pA"
        }, state)

    def test_subscription_registry_reducer_01(self):

        base_reg = {
            "sids_to_subscriptions": {},
            "pids_to_sids": {},
            "task_keys_to_sids": {}
        }

        sub0, reg0 = subscription_registry_reducer(None, None)

        self.assertDictEqual(base_reg, reg0)

        sub1, reg1 = subscription_registry_reducer(reg0, {
            "name": "init",
            "sid": "s1"
        })

        self.assertDictEqual({
            "sids_to_subscriptions": {
                "s1": {
                    "sid": "s1"
                }
            },
            "pids_to_sids": {},
            "task_keys_to_sids": {}
        }, reg1)

        sub2, reg2 = subscription_registry_reducer(reg1, {
            "name": "observePipeline",
            "pid": "pA",
            "sid": "s1"
        })

        self.assertDictEqual({
            "sids_to_subscriptions": {
                "s1": {
                    "sid": "s1",
                    "pid": "pA"
                }
            },
            "pids_to_sids": {
                "pA": ["s1"]
            },
            "task_keys_to_sids": {}
        }, reg2)

        sub3, reg3 = subscription_registry_reducer(reg2, {
            "name": "init",
            "sid": "s2"
        })

        self.assertDictEqual({
            "sids_to_subscriptions": {
                "s1": {
                    "sid": "s1",
                    "pid": "pA"
                },
                "s2": {
                    "sid": "s2"
                }
            },
            "pids_to_sids": {
                "pA": ["s1"]
            },
            "task_keys_to_sids": {}
        }, reg3)

        sub4, reg4 = subscription_registry_reducer(reg3, {
            "name": "observePipeline",
            "pid": "pA",
            "sid": "s2"
        })

        self.assertDictEqual({
            "sids_to_subscriptions": {
                "s1": {
                    "sid": "s1",
                    "pid": "pA"
                },
                "s2": {
                    "sid": "s2",
                    "pid": "pA"
                }
            },
            "pids_to_sids": {
                "pA": ["s1", "s2"]
            },
            "task_keys_to_sids": {}
        }, reg4)

        sub5, reg5 = subscription_registry_reducer(reg4, {
            "name": "observeTask",
            "task_key": "tA",
            "sid": "s2"
        })

        self.assertDictEqual({
            "sids_to_subscriptions": {
                "s1": {
                    "sid": "s1",
                    "pid": "pA"
                },
                "s2": {
                    "sid": "s2",
                    "pid": "pA",
                    "task_key": "tA"
                }
            },
            "pids_to_sids": {
                "pA": ["s1", "s2"]
            },
            "task_keys_to_sids": {
                "tA": ["s2"]
            }
        }, reg5)

        sub6, reg6 = subscription_registry_reducer(reg5, {
            "name": "observeTask",
            "task_key": "tB",
            "sid": "s2"
        })

        self.assertDictEqual({
            "sids_to_subscriptions": {
                "s1": {
                    "sid": "s1",
                    "pid": "pA"
                },
                "s2": {
                    "sid": "s2",
                    "pid": "pA",
                    "task_key": "tB"
                }
            },
            "pids_to_sids": {
                "pA": ["s1", "s2"]
            },
            "task_keys_to_sids": {
                "tB": ["s2"]
            }
        }, reg6)

        sub7, reg7 = subscription_registry_reducer(reg6, {
            "name": "unObservePipeline",
            "sid": "s2"
        })

        self.assertDictEqual({
            "sids_to_subscriptions": {
                "s1": {
                    "sid": "s1",
                    "pid": "pA"
                },
                "s2": {
                    "sid": "s2"
                }
            },
            "pids_to_sids": {
                "pA": ["s1"]
            },
            "task_keys_to_sids": {}
        }, reg7)

        sub8, reg8 = subscription_registry_reducer(reg7, {
            "name": "close",
            "sid": "s1"
        })

        self.assertDictEqual({
            "sids_to_subscriptions": {
                "s2": {
                    "sid": "s2"
                }
            },
            "pids_to_sids": {},
            "task_keys_to_sids": {}
        }, reg8)

        return base_reg, reg1, reg2, reg3, reg4, reg5, reg6, reg7, reg8

    def test_subscription_registry_reducer_02(self):

        base_reg, reg1, reg2, reg3, reg4, reg5, reg6, reg7, reg8 = self.test_subscription_registry_reducer_01()

        subs, reg = subscription_registry_reducer(reg5, {
            "name": "close_sessions",
            "sids": ["s1", "s2"]
        })

        self.assertDictEqual(base_reg, reg)

        subs, reg = subscription_registry_reducer(reg5, {
            "name": "close_sessions",
            "sids": ["s1"]
        })

        self.assertDictEqual({
            "sids_to_subscriptions": {
                "s2": {
                    "sid": "s2",
                    "pid": "pA",
                    "task_key": "tA"
                }
            },
            "pids_to_sids": {
                "pA": ["s2"]
            },
            "task_keys_to_sids": {
                "tA": ["s2"]
            }
        }, reg)

        for reg in [base_reg, reg1, reg2, reg3, reg4, reg5, reg6, reg7, reg8]:

            sids_to_subscriptions = reg["sids_to_subscriptions"]

            subs, reg_after = subscription_registry_reducer(reg, {
                "name": "reset_from_sids_to_subscriptions",
                "sids_to_subscriptions": sids_to_subscriptions
            })

            self.assertDictEqual(reg, reg_after)
