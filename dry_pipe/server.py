import asyncio
import logging
import os
import pathlib
import time
import weakref
from datetime import datetime
from threading import Thread

import socketio
import json
import uvicorn

from dry_pipe import Task
from dry_pipe.actions import TaskAction
from dry_pipe.janitors import DaemonThreadHelper
from dry_pipe.monitoring import PipelineMetricsTable, fetch_task_group_metrics
from dry_pipe.pipeline_state import PipelineState
from dry_pipe.task_state import TaskState

logger = logging.getLogger(__name__)

DRYPIPE_WEBSOCKET_NAMESPACE = "/drypipe"


def task_room_name(pipeline_dir, task_key):
    return f"task,{pipeline_dir},{task_key}"


class PipelineUIServer:

    instance = None

    @staticmethod
    def init_multi_pipeline_instance(instances_dir):
        PipelineUIServer.instance = PipelineUIServer(instances_dir=instances_dir)
        return PipelineUIServer.instance

    @staticmethod
    def init_single_pipeline_instance(single_pipeline_state):
        PipelineUIServer.instance = PipelineUIServer(single_pipeline_state=single_pipeline_state)
        return PipelineUIServer.instance

    def is_single_pipeline_mode(self):
        return self.single_pipeline_state is not None

    def __init__(self, instances_dir=None, single_pipeline_state=None):
        self.instances_dir = instances_dir
        self.single_pipeline_state = single_pipeline_state

        if single_pipeline_state is not None:
            self.pipeline_states = [single_pipeline_state]
        else:
            self.pipeline_states = [
                pipeline_state
                for pipeline_state in PipelineState.iterate_from_instances_dir(instances_dir)
            ]

        self.shutdown = False

        def call_async(f):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def asyncify():
                await f()

            loop.run_until_complete(asyncify())
            loop.close()

        def work():

            daemon_thread_helper = DaemonThreadHelper(
                logging.getLogger(f"{__name__}.ui_server"),
                min_sleep=2, max_sleep=6, pipelines=None
            )

            while not self.shutdown:

                daemon_thread_helper.begin_round()

                try:
                    rooms = sio.manager.rooms

                    if DRYPIPE_WEBSOCKET_NAMESPACE in rooms:
                        for pipeline_state in self.iterate_pipeline_states():

                            #pipeline_holder = self.get_and_maybe_load_pipeline_holder(pipeline_state)

                            def is_room_active(room_name):
                                return DRYPIPE_WEBSOCKET_NAMESPACE in rooms and \
                                       room_name in rooms[DRYPIPE_WEBSOCKET_NAMESPACE]

                            if is_room_active(f"counts-{pipeline_state.dir_basename()}"):

                                call_async(
                                    self.prepare_emit_counts_message(pipeline_state.instance_dir())
                                )

                                daemon_thread_helper.register_work()
                                #pipeline_holder.inc_work_counter()

                    if DRYPIPE_WEBSOCKET_NAMESPACE in rooms:
                        for room_name in list(rooms[DRYPIPE_WEBSOCKET_NAMESPACE]):
                            if room_name is not None and room_name.startswith("task,"):

                                t, instance_dir, task_key = room_name.split(",")

                                call_async(
                                    self.prepare_emit_task_details_message(
                                        os.path.join(self.instances_dir, instance_dir),
                                        task_key,
                                        room_name
                                    )
                                )

                                daemon_thread_helper.register_work()

                    daemon_thread_helper.end_round()
                except Exception as ex:
                    daemon_thread_helper.handle_exception_in_daemon_loop(ex)

        self.thread = Thread(target=work)

    def iterate_pipeline_states(self):
        if self.is_single_pipeline_mode:
            for pipeline_state in self.pipeline_states:
                yield pipeline_state
        else:
            for s in PipelineState.iterate_from_instances_dir(self.instances_dir):
                yield s

    def full_pipeline_instance_dir(self, pipeline_dir):
        if self.is_single_pipeline_mode():
            return self.single_pipeline_state.instance_dir()
        else:
            return os.path.join(self.instances_dir, pipeline_dir)

    def prepare_emit_counts_message(self, pipeline_instance_dir):

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

                err_tail = task_state.tail_err_if_failed(3)

                if err_tail is not None:
                    o["err_tail"] = err_tail

                action = actions_by_task_key.get(task_state.task_key)

                if action is not None:
                    o["action"] = action.action_name

                partial_task_infos.append(o)

            return partial_task_infos, visit_task

        partial_task_infos, task_state_visitor = create_visitor()

        snapshot_time = int(time.time_ns())

        data = {
            # SHOULD SEND DAG HERE ?
            # "dag": pipeline.summarized_dependency_graph(),
            "tsv": all_task_states_as_tsv(pipeline_state.instance_dir(), task_state_visitor),
            "pipelineDir": pipeline_state.dir_basename(),
            "partial_task_infos": partial_task_infos,
            "snapshot_time": snapshot_time
        }

        return lambda: sio.emit(
            'latestPipelineDetailedState',
            data,
            room=f"counts-{instance_dir}",
            namespace=DRYPIPE_WEBSOCKET_NAMESPACE
        )

    def prepare_emit_task_details_message(self, pipeline_instance_dir, task_key, room_name):

        task_state = TaskState.from_task_control_dir(pipeline_instance_dir, task_key)

        data = task_state.as_json()

        return lambda: sio.emit(
            'latestTaskDetails',
            data,
            room=room_name,
            namespace=DRYPIPE_WEBSOCKET_NAMESPACE
        )

    def start(self):
        self.thread.start()

    def stop(self):
        self.shutdown = False
        self.thread.join()

    def all_pipeline_states(self):
        return [
            {
                "dir": pipeline_state.dir_basename(),
                "state": pipeline_state.state_name,
                "totals": pipeline_state.totals_row()
            }
            for pipeline_state in self.pipeline_states
        ]


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


def json_response(func):

    def enc(s):
        return s.encode("utf-8")

    body = enc(json.dumps(func()))

    headers = {
        'type': 'http.response.start',
        'status': 200,
        'headers': [
            [b'Content-Type', b'application/json'],
            [b'Content-Length', enc(str(len(body)))]
        ],
    }

    def body_func():
        return {
            'type': 'http.response.body',
            'body': body
        }

    return headers, body_func


def text_response(func):

    headers = {
        'type': 'http.response.start',
        'status': 200,
        'headers': [
            [b'content-type', b'text/plain'],
        ],
    }

    def body_func():
        return {
            'type': 'http.response.body',
            'body': func().encode("utf-8")
        }

    return headers, body_func


def map_static_file(static_file):

    the_static_file = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        "ui",
        static_file
    )

    if not os.path.exists(the_static_file):
        the_static_file = os.path.join(
            os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
            "ui",
            "dist",
            static_file
        )

    return the_static_file


async def serve_static_file(static_file, ct, send, receive):

    the_static_file = map_static_file(static_file)

    event = await receive()

    with open(the_static_file, 'rb') as f:
        payload = f.read()

    await send({'type': 'http.response.start',
                'status': 200,
                'headers': [(b'Content-Type', ct.encode('utf-8'))]})

    await send({'type': 'http.response.body',
                'body': payload})


class App:

    async def __call__(self, scope, receive, send):

        if scope["type"] == "lifespan":
            return

        assert scope['type'] == 'http'

        path = scope["path"]

        if path == "/":
            await serve_static_file("index.html", 'text/html', send, receive)
            return

        response = None

        if scope["method"] == "GET":

            if path == "/drypipe/pipelines":
                response = json_response(lambda: PipelineUIServer.instance.all_pipeline_states_as_json())

            elif path.startswith("/drypipe/pipeline/"):

                if "/action/" in path:

                    response = json_response(lambda: PipelineUIServer.instance.all_pipeline_states_as_json())

        if response is None:
            response = text_response(lambda: "Bad Route")

        headers, body = response

        await send(headers)

        b = body()

        await send(b)


sio = socketio.AsyncServer(async_mode='asgi')


@sio.on('join', namespace=DRYPIPE_WEBSOCKET_NAMESPACE)
async def join(sid, message):
    room = message['room']
    #print(f"entering {room}")
    sio.enter_room(sid, room, namespace=DRYPIPE_WEBSOCKET_NAMESPACE)

    if PipelineUIServer.instance is not None:

        pipeline_ui_server = PipelineUIServer.instance

        if room.startswith("counts-"):
            instance_dir = room[7:]
            #pipeline_holder = pipeline_ui_server.get_and_maybe_load_pipeline_holder(instance_dir=instance_dir)

            pid = os.path.join(pipeline_ui_server.instances_dir, instance_dir)
            emit_call = pipeline_ui_server.prepare_emit_counts_message(pid)
            await emit_call()

        if room.startswith("task,"):
            t, instance_dir, task_key = room.split(",")

            logger.debug("new observer for task %s in pipeline %s", task_key, instance_dir)

            pid = os.path.join(pipeline_ui_server.instances_dir, instance_dir)

            #pipeline_holder = pipeline_ui_server.get_and_maybe_load_pipeline_holder(instance_dir=instance_dir)
            emit_call = pipeline_ui_server.prepare_emit_task_details_message(pid, task_key, room)
            await emit_call()


@sio.on('leave', namespace=DRYPIPE_WEBSOCKET_NAMESPACE)
async def leave(sid, message):
    room = message['room']

    logger.debug("stop observing %s", room)

    sio.leave_room(sid, room, namespace=DRYPIPE_WEBSOCKET_NAMESPACE)


@sio.on('submitTaskAction', namespace=DRYPIPE_WEBSOCKET_NAMESPACE)
async def task_action_submitted(sid, message):

    task_key = message['task_key']
    pipeline_dir = message['pipeline_dir']
    action_name = message['action_name']
    is_cancel = message.get("is_cancel")

    if PipelineUIServer.instance is not None:

        pipeline_ui_server = PipelineUIServer.instance

        pipeline_instance_dir = pipeline_ui_server.full_pipeline_instance_dir(pipeline_dir)

        try:

            if is_cancel is not None and is_cancel:
                action = TaskAction.load_from_task_control_dir(
                    os.path.join(pipeline_instance_dir, ".drypipe", task_key)
                )

                if action is not None:
                    action.delete()

                await sio.emit(
                    'taskActionSubmitted',
                    message,
                    room=task_room_name(pipeline_dir, task_key),
                    namespace=DRYPIPE_WEBSOCKET_NAMESPACE
                )

                emit_call = pipeline_ui_server.prepare_emit_task_details_message(
                    pipeline_instance_dir, task_key, task_room_name(pipeline_dir, task_key)
                )

                await emit_call()

            else:
                TaskAction.submit(pipeline_instance_dir, task_key, action_name, message.get("step"))
                await sio.emit(
                    'taskActionSubmitted',
                    message,
                    room=task_room_name(pipeline_dir, task_key),
                    namespace=DRYPIPE_WEBSOCKET_NAMESPACE
                )
        except Exception as ex:
            await sio.emit(
                "serverSideException",
                {"message": f"{ex}"},
                room=sid,
                namespace=DRYPIPE_WEBSOCKET_NAMESPACE
            )

    #print(f"leaving {room}")

@sio.on('close room')
async def close(sid, message):
    await sio.close_room(message['room'])


@sio.on('disconnect request')
async def disconnect_request(sid):
    await sio.disconnect(sid)


app = socketio.ASGIApp(
    sio,
    other_asgi_app=App(),
    static_files={
        "/main.js": map_static_file("dist/main.js"),
        "/main.js.map": map_static_file("dist/main.js.map")
    }
)


def is_port_in_use(bind_address, port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((bind_address, port)) == 0


def serve(bind_address, port):

    if is_port_in_use(bind_address, port):
        print(f" port {port} is used by another application, use --port=X to listen on another port")
        return

    print(f"Web Monitor running on: http://{bind_address}:{port} (Press CTRL+C to quit)")

    uvicorn.run(
        #"dry_pipe.server:app",
        app,
        host=bind_address, port=port,
        log_level="error"
        #reload=True,
        #reload_dirs=["/home/maxou/dev/dry_pipe/dry_pipe"],
        #reload_delay=2.0
    )
