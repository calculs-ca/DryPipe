import asyncio
import json
import logging
import os
from threading import Thread

import socketio
import uvicorn
from socketio import AsyncNamespace

from dry_pipe.janitors import DaemonThreadHelper
from dry_pipe.pubsub import SubscriptionRegistry, DRYPIPE_WEBSOCKET_NAMESPACE
from dry_pipe.ui_janitor import UIJanitor

logger = logging.getLogger(__name__)


class WebsocketServer(AsyncNamespace):

    @staticmethod
    def start(bind_address, port, instances_dir_if_has_local_janitor=None):

        try:
            SubscriptionRegistry.init()

            server_websocket_adapter = WebsocketServer(instances_dir_if_has_local_janitor)

            def _is_port_in_use(bind_address, port):
                import socket
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    return s.connect_ex((bind_address, port)) == 0

            if _is_port_in_use(bind_address, port):
                print(f" port {port} is used by another application, use --port=X to listen on another port")
                return

            socket_io_server = socketio.AsyncServer(
                async_mode='asgi', logger=True, engineio_logger=True
            )

            socket_io_server.register_namespace(server_websocket_adapter)

            def _obsolete():
                if server_websocket_adapter.ui_janitor is not None:

                    def janitor_thread_func():

                        async def go():
                            await server_websocket_adapter.janitor()

                        logger.info("starting ui_janitor thread")
                        loop = asyncio.get_event_loop()
                        loop.run_until_complete(go())

                    janitor_thread = Thread(target=janitor_thread_func)
                    janitor_thread.start()
                    logger.info("ui_janitor thread started")

            janitor_thread = Thread(target=lambda: server_websocket_adapter.janitor())
            janitor_thread.start()
            logger.info("ui_janitor thread started")

            app = socketio.ASGIApp(
                socket_io_server,
                socketio_path="/socket.io",
                #other_asgi_app=App(),
                #    static_files={
                #        "/main.js": map_static_file("dist/main.js"),
                #        "/main.js.map": map_static_file("dist/main.js.map")
                #    }
            )

            logger.info(f"web monitor running on: http://{bind_address}:{port} (Press CTRL+C to quit)")

            uvicorn.run(
                #"dry_pipe.server:app",
                app,
                host=bind_address, port=port,
                log_level="error"
                #reload=True,
                #reload_dirs=["/home/maxou/dev/dry_pipe/dry_pipe"],
                #reload_delay=2.0
            )
        except Exception as ex:
            logger.exception(ex)

    def __init__(self, instances_dir_if_has_local_janitor=None):
        super(AsyncNamespace, self).__init__(DRYPIPE_WEBSOCKET_NAMESPACE)
        self.drypipe_ui_session_id = None

        if instances_dir_if_has_local_janitor is not None:
            logger.info("will run local ui_janitor on dir %s", instances_dir_if_has_local_janitor)
            self.ui_janitor = UIJanitor(instances_dir_if_has_local_janitor)
        else:
            self.ui_janitor = None
            raise Exception(f"not supported")

    def janitor(self):

        def call_async(f):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def asyncify():
                await f()

            loop.run_until_complete(asyncify())
            loop.close()

        try:

            daemon_thread_helper = DaemonThreadHelper(logger, min_sleep=4, max_sleep=8, pipelines=None)

            while True:

                for m in self.ui_janitor.messages_for_round():

                    async def go():
                        await self.trigger_event("broadcast", None, m)

                    call_async(go)

                    daemon_thread_helper.register_work()

                daemon_thread_helper.end_round()

        except Exception as ex:
            logger.exception(ex)
            raise ex

    async def send_latest_subscription_registry(self):
        self.server.emit("latest_subscription_registry")


    def _autheticate(self, http_env):

        headers = http_env["asgi.scope"]["headers"]

        def auth_cookie():
            for h in headers:
                name, val = h
                name = name.decode("utf-8").lower()
                if name == "set-cookie":
                    cookies = val.decode("utf-8").split("=")[1]

    async def trigger_event(self, event, *args):
        try:
            await self._trigger_event(event, *args)
        except Exception as ex:
            logger.exception("trigger_event failed: %s %s", event, json.dumps(args))
            raise ex

    async def _trigger_event(self, event, *args):

        sid = args[0]

        #if event == "connect":
        #    # self._autheticate(args[1])
        #    return

        logger.debug("received event %s, %s", event, sid)

        if event == "announce_drypipe_ui":

            if self.ui_janitor is not None:
                logger.warning(
                    "this drypipe ui hub already has a local server, will not accept connections from ui servers"
                )
                return

            self.drypipe_ui_session_id = sid

            logger.info(f"ui server connected %s, will send reset_subscriptions message", self.drypipe_ui_session_id)

            await self.server.emit(
                "reset_subscriptions",
                {"sids_to_subscriptions": SubscriptionRegistry.instance.sids_to_subscriptions()},
                to=self.drypipe_ui_session_id,
                namespace=DRYPIPE_WEBSOCKET_NAMESPACE
            )

            #self.server.start_background_task(target=self.work)

        elif event == "update_subscription":

            action_from_browser = args[1]
            updated_subscription = SubscriptionRegistry.instance.update_subscription(action_from_browser)

            if self.ui_janitor is None:

                await self.server.emit(
                    "update_subscription",
                    {
                        "sids_to_subscriptions": SubscriptionRegistry.instance.sids_to_subscriptions(),
                        "action_from_browser": action_from_browser
                    },
                    to=self.drypipe_ui_session_id,
                    namespace=DRYPIPE_WEBSOCKET_NAMESPACE
                )
            else:

                for m in self.ui_janitor.message_after_subscription_update(action_from_browser):
                    await self.server.emit(m["event"], m["data"], sid=sid, namespace=DRYPIPE_WEBSOCKET_NAMESPACE)

            # sending the ack *after* calling drypipe_ui, ensures the user knows if the server is down

            await self.server.emit(
                "subscription_update_ack",
                {
                    "updated_subscription": updated_subscription,
                    "action_name": action_from_browser["name"]
                },
                to=sid,
                namespace=DRYPIPE_WEBSOCKET_NAMESPACE
            )

        elif event == "disconnect":
            if sid == self.drypipe_ui_session_id:
                logger.info(f"drypipe_ui_session_id %s disconnected", sid)
                self.drypipe_ui_session_id = None
            else:

                SubscriptionRegistry.instance.update_subscription({"name": "close", "sid": sid})

                await self.server.emit(
                    "reset_subscriptions",
                    {"sids_to_subscriptions": SubscriptionRegistry.instance.sids_to_subscriptions()},
                    to=self.drypipe_ui_session_id,
                    namespace=DRYPIPE_WEBSOCKET_NAMESPACE
                )

        elif event == "submitTaskAction":

            sid, message = args
            message["sid"] = sid

            logger.info("task submitted %s", message)

            if self.ui_janitor is not None:
                m = self.ui_janitor.task_action_submitted(sid, message)
                await self.trigger_event("broadcast", None, m)
            else:
                await self.server.emit(
                    "submitTaskAction",
                    message,
                    to=self.drypipe_ui_session_id,
                    namespace=DRYPIPE_WEBSOCKET_NAMESPACE
                )

        elif event == "broadcast":

            _sid_of_drypipe_ui_server_or_none, message = args

            relayed_event = message["event"]
            sids = message["sids"]

            logger.debug("will broadcast message %s to sids %s", relayed_event, sids)

            broadcast_to_all_failed_sids = []

            for sid in sids:
                try:
                    await self.server.emit(
                        relayed_event,
                        message["data"],
                        to=sid,
                        namespace=DRYPIPE_WEBSOCKET_NAMESPACE
                    )
                except Exception as exc:
                    logger.warning("failed while relaying event %s to %s\n%s", event, sid, exc)
                    broadcast_to_all_failed_sids.append(sid)

            for failed_sid in broadcast_to_all_failed_sids:
                logger.info("will disconnect %s", failed_sid)
                await self.server.disconnect(failed_sid)

            if len(broadcast_to_all_failed_sids) > 0:
                SubscriptionRegistry.instance.close_sessions(broadcast_to_all_failed_sids)

            #TODO: use this:
            #exceptions = await asyncio.gather(broadcast_to_all())


class App:
    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            return


if __name__ == '__main__':

    from dry_pipe.internals import setup_debug_log_config

    extra = {
        'socketio': {
            'handlers': ['console'],
            'level': "DEBUG",
            'propagate': False
        },
    }

    setup_debug_log_config(drypype_level="DEBUG", root_level="INFO", main_level="DEBUG", extra_loggers=extra)

    WebsocketServer.start("0.0.0.0", 5000)
