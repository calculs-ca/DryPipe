import glob
import importlib
import inspect
import json
import pathlib
import re
import sys
import os
import traceback
import click
from dry_pipe import DryPipe
from dry_pipe.internals import env_from_sourcing
from dry_pipe.janitors import Janitor
from dry_pipe.monitoring import fetch_task_groups_stats
from dry_pipe.pipeline import Pipeline, PipelineInstance
from dry_pipe.pipeline_state import PipelineState
from dry_pipe.task_state import NON_TERMINAL_STATES



@click.group()
@click.option('-v', '--verbose')
@click.pass_context
def cli_group(ctx, verbose):
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj["ctx"] = ctx


def _bad_entrypoint(ctx):

    if "entrypoint" in ctx.obj and ctx.obj["entrypoint"] == "run_task":
        click.echo("Can't launch a pipeline with cli.run_task(), use cli.run(pipeline=<your pipeline>)", err=True)
        return True

    return False

@click.command()
@click.pass_context
@click.option('--instance-dir', type=click.Path(), default=None)
def clean(ctx, instance_dir):
    if _bad_entrypoint(ctx):
        return
    pipeline_func = ctx.obj["pipeline_func"]
    pipeline = _pipeline_from_pipeline_func(pipeline_func, instance_dir)
    # TODO: prompt
    pipeline.clean()


def _pipeline_from_pipeline_func(pipeline_func, instance_dir):

    pipeline_func_sig = inspect.signature(pipeline_func)

    nargs = len(pipeline_func_sig.parameters)

    if nargs == 0:

        if instance_dir is not None:
            raise Exception(f"when specifying instance_dir, task generator must take a dry_pipe.DryPipe.dsl() argument")

        return DryPipe.create_pipeline(pipeline_func)
    elif nargs == 1:
        #dsl = DryPipe.dsl_for(pipeline_func)
        pipeline_code_dir = DryPipe.pipeline_code_dir_for(pipeline_func)
        pipeline_instance_dir = pipeline_code_dir

        #if instance_dir is not None:
        #    dsl = dsl.with_defaults(pipeline_instance_dir=pipeline_code_dir)
        return DryPipe.create_pipeline(pipeline_func, pipeline_code_dir)
    else:
        m = inspect.getmodule(pipeline_func)
        raise Exception(f"function {pipeline_func.__name__} in {m.__file__} takes {nargs} arguments,\n"
                        f"it should take a instance created with dry_pipe.DryPipe.dsl()")


@click.command()
@click.pass_context
@click.option('--clean', is_flag=True)
@click.option('--instance-dir', type=click.Path(), default=None)
def prepare(ctx, clean, instance_dir):

    if _bad_entrypoint(ctx):
        return

    pipeline_func = ctx.obj["pipeline_func"]

    pipeline = _pipeline_from_pipeline_func(pipeline_func, instance_dir)

    if clean:
        pipeline.clean_all()

    pipeline.init_work_dir()

    pipeline.get_state().touch()

    tasks_total = 0
    work_done = 0
    tasks_in_non_terminal_states = 0
    tasks_completed = 0

    for task in pipeline.tasks:

        tasks_total += 1

        task_state = task.get_state()

        if task_state is None:
            task.create_state_file_and_control_dir()
            work_done += 1
            task_state = task.get_state()

        if task_state.state_name in NON_TERMINAL_STATES:
            tasks_in_non_terminal_states += 1

        if task_state.is_waiting_for_deps():

            if not task.has_unsatisfied_deps():
                if task_state.is_prepared():
                    continue
                else:
                    task_state.transition_to_prepared(task)
                    task_state = task.get_state()
                    task_state.transition_to_queued(task)
                    task.prepare()
                    tasks_in_non_terminal_states += 1
                    work_done += 1

        elif task_state.is_completed():
            if task_state.action_if_exists() is None:
                tasks_completed += 1

    if tasks_total == tasks_completed:
        pipeline_state = pipeline.get_state()
        if not pipeline_state.is_completed():
            pipeline_state.transition_to_completed()


def _pipeline_from_modul_func(instance_dir, module_func_pipeline, write_hint_file_if_not_exists=True):

    mod, func_name = module_func_pipeline.split(":")

    module = importlib.import_module(mod)

    func = getattr(module, func_name, None)

    if func is None:
        raise Exception(f"function {func_name} not found in module {mod}")

    pipeline = func()

    if not isinstance(pipeline, Pipeline):
        raise Exception(f"function {func_name} in {mod} should return a Pipeline, not {type(pipeline).__name__}")

    if write_hint_file_if_not_exists:
        PipelineInstance.write_hint_file_if_not_exists(instance_dir, module_func_pipeline)

    return pipeline


def _pipeline_instance_creater(instance_dir, pipeline_spec):

    if pipeline_spec is None:
        pipeline_spec = PipelineInstance.guess_pipeline_from_hints(instance_dir)

    pipeline = _pipeline_from_modul_func(instance_dir, pipeline_spec)

    return lambda: pipeline.create_pipeline_instance(instance_dir)


@click.command()
@click.option('-p', '--pipeline', help="a_module:a_func, a function that returns a pipeline.")
@click.option('--instance-dir', type=click.Path(), default=None)
def mon(pipeline, instance_dir):

    from dry_pipe.cli_screen import CliScreen

    if instance_dir is None:
        instance_dir = os.getcwd()

    if pipeline is None:
        pipeline = PipelineInstance.guess_pipeline_from_hints(instance_dir)
        if pipeline is None:
            click.echo("must specify pipeline, ex: -p <aModule>:<aPipelineFunction>")

    #pipeline_instance = _pipeline_instance_creater(instance_dir, pipeline)()

    screen_state = CliScreen(instance_dir, is_monitor_mode=True)

    err_msg = screen_state.start_and_wait()

    if err_msg is not None:
        click.echo(err_msg, err=True)


@click.command()
@click.option('-p', '--pipeline', help="a_module:a_func, a function that returns a pipeline.")
@click.option('--instance-dir', type=click.Path(), default=None)
@click.option('--web-mon', is_flag=True)
@click.option('--port', default=5000, help="port for --web-mon")
@click.option('--bind', default="0.0.0.0", help="bind address for --web-mon")
@click.option('--clean', is_flag=True)
@click.option('--single', type=click.STRING, default=None,
              help="launches a single task, specified by the task key, in foreground, ex.: launch --single=TASK_KEY")
@click.option('--restart-failed', is_flag=True)
@click.option('--reset-failed', is_flag=True)
@click.option('--no-confirm', is_flag=True)
def run(pipeline, instance_dir, web_mon, port, bind, clean, single, restart_failed, reset_failed, no_confirm):

    pipeline_mod_func = pipeline

    if instance_dir is None:
        instance_dir = os.getcwd()

    if PipelineState.from_pipeline_instance_dir(instance_dir, none_if_not_exists=True) is None:
        if not no_confirm:
            if not click.confirm(f"no pipeline instance exists in {instance_dir}, do you want to create one ?"):
                click.echo("no pipeline instance created, exiting.")
                return

    pipeline_instance = _pipeline_instance_creater(instance_dir, pipeline_mod_func)()

    if clean:
        pipeline_instance.clean()

    pipeline_instance.init_work_dir()

    if single is not None:
        launch_single(pipeline_instance, single)
        return

    pipeline_instance.regen_tasks_if_stale(force=True)

    for task in pipeline_instance.tasks:
        task_state = task.get_state()
        if task_state is not None:

            if task_state.is_failed() and not task.has_unsatisfied_deps():
                task.prepare()

                if restart_failed:
                    task.re_queue()

    janitor = Janitor(
        pipeline_instance=pipeline_instance,
        min_sleep=0,
        max_sleep=5
    )

    janitor.start(stay_alive_when_no_more_work=True)
    janitor.start_remote_janitors()

    if web_mon:
        from dry_pipe.websocket_server import WebsocketServer
        from dry_pipe.server import PipelineUIServer
        server = PipelineUIServer.init_single_pipeline_instance(pipeline_instance.get_state())
        server.start()

        WebsocketServer.start(bind, port)
        # Stopping threads break CTRL+C, let the threads die with the process exit
        #server.stop()
        #janitor.do_shutdown()
    else:
        from dry_pipe.cli_screen import CliScreen
        rich_screen = CliScreen(pipeline_instance.pipeline_instance_dir, is_monitor_mode=False)
        err_msg = rich_screen.start_and_wait()
        if err_msg is not None:
            click.echo(err_msg, err=True)

    os._exit(0)


def launch_single(pipeline, single):

    task = pipeline.tasks.get(single)

    if task is None:
        raise Exception(f"pipeline has no task with key '{single}'")

    task.re_queue()
    task.prepare()
    task_state = task.get_state()
    task_state.transition_to_launched(task, wait_for_completion=True)
    task_state = task.get_state()
    task_state.transition_to_completed(task)

    print(f"Task {single} completed.")


@click.command()
@click.pass_context
@click.option('--instance-dir', type=click.Path(), default=None)
@click.option('--single', type=click.STRING, default=None,
              help="apply to single task, specified by the task key, in foreground, ex.: launch --single=TASK_KEY")
def requeue(ctx, single, instance_dir):

    if _bad_entrypoint(ctx):
        return

    pipeline_func = ctx.obj["pipeline_func"]

    pipeline = _pipeline_from_pipeline_func(pipeline_func, instance_dir)

    if single is None:
        raise Exception(f"unimplemented.")

    requeue_single(pipeline, single)


def requeue_single(pipeline, single):

    task = pipeline.tasks.get(single)

    if task is None:
        raise Exception(f"pipeline has no task with key '{single}'")

    task.re_queue()
    task.prepare()

    print(f"Task {single} requeued")


@click.command()
@click.pass_context
@click.option('-p', '--pipeline', help="a_module:a_func, a function that returns a pipeline.")
@click.option('--env', type=click.STRING, default=None)
@click.option('--instances-dir', type=click.STRING)
def watch(ctx, pipeline, env, instances_dir):

    if instances_dir is None:
        raise Exception(
            f"serve-multi requires mandatory option --instances-dir=<directory of pipeline instances>"
        )

    pipeline = _pipeline_from_modul_func(None, pipeline, write_hint_file_if_not_exists=False)

    if not os.path.exists(instances_dir):
        pathlib.Path(instances_dir).mkdir(parents=True)

    janitor = Janitor(pipeline, pipeline_instances_dir=instances_dir)

    thread = janitor.start()

    janitor.start_remote_janitors()

    thread.join()

    os._exit(0)


def _validate_task_generator_callable_with_single_dsl_arg_and_get_module_file(task_generator):

    a = inspect.signature(task_generator)

    f = os.path.dirname(os.path.abspath(inspect.getmodule(task_generator).__file__))

    if len(a.parameters) != 1:
        raise Exception(f"function given to cli.run() should take a singe DryPipe.dsl() as argument, and yield tasks" +
                        f" was given {len(a.parameters)} args")

    return f


@click.command()
@click.pass_context
@click.option('--instances-dir', type=click.STRING)
@click.option('--bind', default="0.0.0.0", help="bind address for --web-mon")
@click.option('--port', default=5000)
def serve_ui(ctx, instances_dir, bind, port):
    from dry_pipe.websocket_server import WebsocketServer

    if instances_dir is None:
        raise Exception(
            f"serve-multi requires mandatory option --instances-dir=<directory of pipeline instances>"
        )

    if not os.path.exists(instances_dir):
        pathlib.Path(instances_dir).mkdir(parents=True)

    WebsocketServer.start(bind, port, instances_dir)

    os._exit(0)


def _func_from_mod_func(mod_func):

    mod, func_name = mod_func.split(":")

    if not mod.startswith("."):
        module = importlib.import_module(mod)
    else:
        module = importlib.import_module(mod[1:])

    python_task = getattr(module, func_name, None)
    if python_task is None:
        raise Exception(f"function {func_name} not found in module {mod}")

    return python_task


@click.command()
@click.pass_context
@click.argument('mod-func', type=click.STRING)
def test(ctx, mod_func):
    python_task = _func_from_mod_func(mod_func)

    if len(python_task.tests) == 0:
        click.echo(f"No tests for python_task {mod_func}")
        return

    for test in python_task.tests:
        python_task.func(*test["args"], test=test)


@click.command()
@click.pass_context
@click.argument('mod-func', type=click.STRING)
@click.option('--task-env', type=click.Path(exists=True, dir_okay=False))
def call(ctx, mod_func, task_env):

    python_task = _func_from_mod_func(mod_func)

    env = os.environ

    if task_env is not None:
        # or if pipeline provided --task=key
        env = env_from_sourcing(task_env)

    #TODO: Validate missing args
    args = [
        env.get(k)
        for k, v in python_task.signature.parameters.items()
    ]

    out_vars = None

    try:
        out_vars = python_task.func(* args)
    except Exception as ex:
        traceback.print_exc()
        exit(1)

    # TODO: complain if missing task __X vars

    control_dir = env["__control_dir"]

    try:
        if out_vars is not None:
            with open(env["__output_var_file"], "a+") as f:
                for k, v in out_vars.items():
                    try:
                        v = json.dumps(v)
                    except TypeError as ex0:
                        print(
                            f"task call {control_dir}.{mod_func} returned var {k} of unsupported type: {type(v)}" +
                            " only primitive types are supported.",
                            file=sys.stderr
                        )
                        exit(1)
                    f.write(f"{k}={v}\n")

        for pid_file in glob.glob(os.path.join(control_dir, "*.pid")):
            os.remove(pid_file)

    except Exception as ex:
        traceback.print_exc()
        exit(1)


@click.command()
@click.pass_context
@click.option('--downstream-of', type=click.STRING)
@click.option('--not-matching', type=click.STRING)
@click.option('--matching', type=click.STRING)
@click.option('--instance-dir', type=click.Path(), default=None)
def reset(ctx, downstream_of, not_matching, matching, instance_dir):

    pipeline_func = ctx.obj["pipeline_func"]

    pipeline = _pipeline_from_pipeline_func(pipeline_func, instance_dir)

    if matching is not None or not_matching is not None:

        if matching is not None and not_matching is not None:
            raise Exception("--matching and --not-matching can't be both specified")

        def do_reset(key):
            if matching is not None:
                return re.match(matching, key)
            return not re.match(not_matching, key)

        for task in pipeline.tasks:
            if do_reset(task.key):
                if click.confirm(f"reset {task.key} ?"):
                    task.clean()
                    print(f"{task.key} reset")

    elif downstream_of is not None:

        after_task_including = pipeline.tasks.get(downstream_of)

        if after_task_including is None:
            raise Exception(f"pipeline has no task with key '{downstream_of}'")

        def all_upstream_deps(task):
            yield task
            for upstream_task, _1, _2 in task.upstream_deps_iterator():
                for t in all_upstream_deps(upstream_task):
                    yield t

        raise Exception("not implemented")
    else:
        if click.confirm("reset all tasks ?"):
            print("boom")

@click.command()
@click.pass_context
@click.option('--instance-dir', type=click.Path(), default=None)
def stats(ctx, instance_dir):

    the_stats = None

    if instance_dir is not None:
        the_stats = fetch_task_groups_stats(instance_dir)
    else:
        pipeline_func = ctx.obj["pipeline_func"]
        pipeline = _pipeline_from_pipeline_func(pipeline_func, instance_dir)
        the_stats = fetch_task_groups_stats(pipeline.pipeline_instance_dir)

    for stat_row in the_stats:
        print("\t".join([str(v) for v in stat_row]))


@click.command()
@click.pass_context
@click.option('-p', '--pipeline', help="a_module:a_func, a function that returns a pipeline.")
def prepare_remote_sites(pipeline):

    pipeline_spec = pipeline

    if pipeline_spec is None:
        click.echo("must specify pipeline, ex: -p <aModule>:<aPipelineFunction>")
        return

    pipeline = _pipeline_from_modul_func(None, pipeline_spec, write_hint_file_if_not_exists=False)

    pipeline.prepare_remote_sites()


@click.command()
@click.pass_context
def status(ctx):

    raise Exception(f"not implemented")

    if _bad_entrypoint(ctx):
        return

    pipeline_func = ctx.obj["pipeline_func"]

    pipeline = DryPipe.create_pipeline(pipeline_func)
    console = Console()
    console.print(_status_table(pipeline))


def _register_commands():
    cli_group.add_command(run)
    cli_group.add_command(mon)
    cli_group.add_command(prepare)
    cli_group.add_command(call)
    cli_group.add_command(test)
    cli_group.add_command(status)
    cli_group.add_command(watch)
    cli_group.add_command(serve_ui)
    cli_group.add_command(clean)
    cli_group.add_command(stats)
    cli_group.add_command(reset)
    cli_group.add_command(requeue)

def run_cli():
    _register_commands()
    cli_group()

if __name__ == '__main__':
    run_cli()
