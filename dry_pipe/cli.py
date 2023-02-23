import glob
import importlib
import inspect
import json
import logging
import logging.config
import pathlib
import re
import shutil
import signal
import sys
import os
import time
import traceback

import click
from dry_pipe import DryPipe
from dry_pipe.internals import PythonCall
from dry_pipe.janitors import Janitor
from dry_pipe.monitoring import fetch_task_groups_stats
from dry_pipe.pipeline import PipelineInstance, Pipeline
from dry_pipe.pipeline_state import PipelineState
from dry_pipe.script_lib import env_from_sourcing, parse_in_out_meta, create_task_logger, iterate_out_vars_from, \
    write_out_vars
from dry_pipe.task_state import NON_TERMINAL_STATES

logger = logging.getLogger(__name__)


@click.group()
@click.option('-v', '--verbose')
@click.version_option()
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


def _configure_logging(log_conf_file):

    if log_conf_file is None:
        log_conf_file = os.environ.get("DRYPIPE_LOGGING_CONF")

    loggin_conf_in_current_dir = os.path.join(os.getcwd(), "drypipe-logging-conf.json")

    if log_conf_file is None and os.path.exists(loggin_conf_in_current_dir):
        log_conf_file = loggin_conf_in_current_dir

    if log_conf_file is not None:
        with open(log_conf_file) as f:
            conf = json.loads(f.read())

            handlers = conf["handlers"]

            for k, handler in handlers.items():
                handler_class = handler["class"]
                if handler_class == "logging.FileHandler":
                    filename = handler.get("filename")
                    if filename is None:
                        raise Exception(f"logging.FileHandler '{k}' has no filename attribute in {log_conf_file}")
                    if "$" in handler["filename"]:
                        raise Exception(
                            f"filename attribute of '{k}': {filename} has undefined env variables, in {log_conf_file}"
                        )
                    handler["filename"] = os.path.expandvars(filename)

            logging.config.dictConfig(conf)
        logger.info("using logging conf: %s", log_conf_file)


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
@click.option('-p', '--pipeline', help="a_module:a_func, a function that returns a pipeline.")
@click.option('--instance-dir', type=click.Path(), default=None)
@click.option('--regen-all', is_flag=True, default=False)
@click.option('--env', type=click.STRING, default=None)
def prepare(pipeline, instance_dir, regen_all, env):

    pipeline_instance = _pipeline_instance_creater(instance_dir, pipeline, env)()

    for task in pipeline_instance.tasks:

        if regen_all:
            task.prepare()
            print(f"regenerated {task.key}")


@click.command()
@click.option('-p', '--pipeline', help="a_module:a_func, a function that returns a pipeline.")
@click.option('--instance-dir', type=click.Path(), default=None)
@click.option('--dry-run', is_flag=False)
def change_keys(pipeline, instance_dir, dry_run):

    pipeline_instance = _pipeline_instance_creater(instance_dir, pipeline)()

    if dry_run:
        print("dry run, nothing will change !")

    for do_it, old_key, new_key in pipeline_instance.change_keys():

        if not dry_run:
            do_it()

        print(f"{old_key} -> {new_key}")

def _pipeline_instance_creater(instance_dir, module_func_pipeline, env=None):

    env_vars = None
    if env is not None:
        env_vars = {}
        for k_v in env.split(","):
            k, v = k_v.split("=")
            env_vars[k] = v

    if module_func_pipeline is None:
        module_func_pipeline = PipelineInstance.guess_pipeline_from_hints(instance_dir)

    pipeline = Pipeline.load_from_module_func(module_func_pipeline)

    PipelineInstance.write_hint_file_if_not_exists(instance_dir, module_func_pipeline)

    return lambda: pipeline.create_pipeline_instance(instance_dir, env_vars=env_vars)


@click.command()
@click.option('-p', '--pipeline', help="a_module:a_func, a function that returns a pipeline.")
@click.option('--instance-dir', type=click.Path(), default=None)
@click.option(
    '--group-by', type=click.STRING, default="by_task_type",
    help="""
    name of grouper in Drypipe.create_pipeline(...,task_groupers={'my_custom_grouper': group_func})
    """
)
def mon(pipeline, instance_dir, group_by):

    _configure_logging(None)

    from dry_pipe.cli_screen import CliScreen

    if instance_dir is None:
        instance_dir = os.getcwd()

    if pipeline is None:
        pipeline = PipelineInstance.guess_pipeline_from_hints(instance_dir)
        if pipeline is None:
            click.echo("must specify pipeline, ex: -p <aModule>:<aPipelineFunction>")

    #pipeline_instance = _pipeline_instance_creater(instance_dir, pipeline)()

    def _quit_listener(cli_screen):
        logger.debug("quit listener")
        logging.shutdown()
        cli_screen.cleanup_tty()
        os._exit(0)

    cli_screen = CliScreen(
        instance_dir,
        None,
        is_monitor_mode=True,
        quit_listener=_quit_listener,
        group_by=group_by
    )

    cli_screen.start()
    def _sigint(p1, p2):
        logger.info("received SIGINT")
        cli_screen.request_quit()

    signal.signal(signal.SIGINT, _sigint)
    signal.pause()

    logging.shutdown()
    exit(0)


@click.command()
@click.option('-p', '--pipeline', help="a_module:a_func, a function that returns a pipeline.")
@click.option('--instance-dir', type=click.Path(), default=None)
@click.option('--web-mon', is_flag=True)
@click.option('--port', default=5000, help="port for --web-mon")
@click.option('--bind', default="0.0.0.0", help="bind address for --web-mon")
@click.option('--clean', is_flag=True)
@click.option('--single', type=click.STRING, default=None,
              help="launches a single task, specified by the task key, in foreground, ex.: launch --single=TASK_KEY")
@click.option('--restart', is_flag=True, help="restart failed or killed tasks")
@click.option('--restart-failed', is_flag=True, help="restart failed tasks")
@click.option('--restart-killed', is_flag=True, help="restart killed tasks")
@click.option('--reset-failed', is_flag=True)
@click.option('--no-confirm', is_flag=True)
@click.option('--logging-conf', type=click.Path(), default=None, help="log configuration file (json)")
@click.option('--env', type=click.STRING, default=None)
@click.option(
    '--group-by', type=click.STRING, default="by_task_type",
    help="""
    name of grouper in Drypipe.create_pipeline(...,task_groupers={'my_custom_grouper': group_func})
    """
)
@click.option(
    '--regex-grouper', type=click.STRING, default=None,
    help="""
    <regex>,<format>
    example: --regex-grouper=.*\_(\w)\.(\d),group-{0}-{1}
    """
)
def run(
    pipeline, instance_dir, web_mon, port, bind,
    clean, single, restart, restart_failed, restart_killed, reset_failed,
    no_confirm, logging_conf, env, regex_grouper, group_by
):

    _configure_logging(logging_conf)

    pipeline_mod_func = pipeline

    if instance_dir is None:
        instance_dir = os.getcwd()

    if not os.path.isabs(instance_dir):
        instance_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(instance_dir)))

    if PipelineState.from_pipeline_instance_dir(instance_dir, none_if_not_exists=True) is None:
        if not no_confirm:
            if not click.confirm(f"no pipeline instance exists in {instance_dir}, do you want to create one ?"):
                click.echo("no pipeline instance created, exiting.")
                return

    pipeline_instance = _pipeline_instance_creater(instance_dir, pipeline_mod_func, env)()

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

            if (task_state.is_failed() or task_state.is_killed()) and not task.has_unsatisfied_deps():
                task.prepare()

                if restart:
                    task.re_queue()
                elif restart_failed and task_state.is_failed():
                    task.re_queue()
                elif restart_killed and task_state.is_killed():
                    task.re_queue()

    janitor = Janitor(
        pipeline_instance=pipeline_instance,
        min_sleep=0,
        max_sleep=5
    )

    janitor.start()
    janitor.start_remote_janitors()

    if web_mon:
        from dry_pipe.websocket_server import WebsocketServer
        from dry_pipe.server import PipelineUIServer
        server = PipelineUIServer.init_single_pipeline_instance(pipeline_instance.get_state())
        server.start()

        WebsocketServer.start(bind, port, instances_dir_if_has_local_janitor=pipeline_instance.pipeline_instance_dir)
        # Stopping threads break CTRL+C, let the threads die with the process exit
        #server.stop()
        #janitor.do_shutdown()
    else:
        from dry_pipe.cli_screen import CliScreen

        def _quit_listener(cli_screen):
            logger.debug("quit listener")
            logging.shutdown()
            cli_screen.cleanup_tty()
            os._exit(0)

        cli_screen = CliScreen(
            pipeline_instance.pipeline_instance_dir,
            None if regex_grouper is None else _display_grouper_func_from_spec(regex_grouper),
            is_monitor_mode=False,
            quit_listener=_quit_listener,
            group_by=group_by
        )

        cli_screen.start()
        def _sigint(p1, p2):
            logger.info("received SIGINT")

            if janitor is not None:
                janitor.request_shutdown()
            cli_screen.request_quit()

        signal.signal(signal.SIGINT, _sigint)
        signal.pause()

        logging.shutdown()
        exit(0)



    #os._exit(0)


def _display_grouper_func_from_spec(display_grouper):
    reg_fmt = display_grouper.split(",")
    if len(reg_fmt) != 2:
        raise Exception(
            f"bad format for arg --display-grouper, expects <regex>,<format> got {display_grouper}"
        )
    reg, fmt = reg_fmt
    try:
        r = re.compile(reg)
    except Exception as ex:
        raise Exception(f"--display-grouper has bad regex: {reg} ")

    fmt_insert_count = fmt.count("{")

    if r.groups != fmt_insert_count:
        raise Exception(
            f"regex match group and placeholders in format must match, got: {r.groups} != {fmt_insert_count}"
        )

    def _regex_display_grouper(task_key):
        m = r.search(task_key)
        if m is None:
            return "<Other Tasks>"
        else:
            gr = m.groups()
            if len(gr) != fmt_insert_count:
                return "<Other Tasks>"
            else:
                return fmt.format(*m.groups())
    return  _regex_display_grouper


def launch_single(pipeline, single):

    task = pipeline.tasks.get(single)

    if task is None:
        raise Exception(f"pipeline has no task with key '{single}'")

    task.re_queue()
    task.prepare()
    task_state = task.get_state()
    task_state.transition_to_launched(task.task_conf.create_executer(), task, wait_for_completion=True)
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

def _parse_instances_dir_to_pipelines(instances_dir_to_pipelines, env=None):

    for d_to_p in instances_dir_to_pipelines.split(","):

        instances_dir, mod_func = d_to_p.split("=")

        if not os.path.exists(instances_dir):
            pathlib.Path(instances_dir).mkdir(parents=True)

        pipeline = Pipeline.load_from_module_func(mod_func)
        pipeline.env_vars = env
        yield instances_dir, pipeline, mod_func

def _version():

    frame = inspect.currentframe()
    f_back = frame.f_back if frame is not None else None
    f_globals = f_back.f_globals if f_back is not None else None
    # break reference cycle
    # https://docs.python.org/3/library/inspect.html#the-interpreter-stack
    del frame

    package_name = f_globals.get("__name__")

    if package_name == "__main__":
        package_name = f_globals.get("__package__")

    if package_name:
        package_name = package_name.partition(".")[0]

    try:
        from importlib import metadata
    except ImportError:
        # Python < 3.8
        import importlib_metadata as metadata
    try:
        return metadata.version(package_name)
    except metadata.PackageNotFoundError:
        raise RuntimeError("Could not get version")


@click.command()
@click.pass_context
@click.option(
    '--instances-dir-to-pipelines',
    type=click.STRING,
    help="see 'watch' command"
)
@click.option(
    '-o',
    type=click.STRING,
    help="output file"
)
def dump_remote_task_confs(ctx, instances_dir_to_pipelines, o):

    def g():
        for instances_dir, pipeline, mod_func in _parse_instances_dir_to_pipelines(instances_dir_to_pipelines):
            yield {
                "constructor_func": mod_func,
                "pipeline_instances_dir": instances_dir,
                "remote_task_confs": [
                    tc.as_json() for tc in pipeline.remote_task_confs
                ]
            }

    if False:
        res = json.dumps({
            "dry_pipe_version": _version(),
            "watched_pipelines_config": list(g())
        }, indent = 2)
    else:
        res = list(g())

    if o is None:
        print(res)
    else:
        with open(o, "w") as o_f:
            o_f.write(res)


@click.command()
@click.pass_context
@click.option(
    '--instances-dir-to-pipelines',
    type=click.STRING,
    required=True,
    help="""
    dir1=module1:func1,...,dirN=moduleN:funcN    
      + moduleX:funcX is a function that returns a dry_pipe.Pipeline    
      + dirX is the parent directory where all pipeline_instance_dirs of PipelineX will reside  
    """
)
@click.option('--env', type=click.STRING, default=None)
def watch(ctx, instances_dir_to_pipelines, env):

    _configure_logging(None)

    janitor = Janitor(
        pipeline_instances_iterator=Pipeline.pipeline_instances_iterator({
            k: v
            for k, v, _ in _parse_instances_dir_to_pipelines(instances_dir_to_pipelines, env)
        })
    )

    janitor.start()

    janitor.start_remote_janitors()

    def _sigterm(p1, p2):
        logger.info("received SIGTERM")
        janitor.request_shutdown()
        logging.shutdown()
        time.sleep(3)
        os._exit(0)

    signal.signal(signal.SIGTERM, _sigterm)
    signal.pause()






def _validate_task_generator_callable_with_single_dsl_arg_and_get_module_file(task_generator):

    a = inspect.signature(task_generator)

    f = os.path.dirname(os.path.abspath(inspect.getmodule(task_generator).__file__))

    if len(a.parameters) != 1:
        raise Exception(f"function given to cli.run() should take a singe DryPipe.dsl() as argument, and yield tasks" +
                        f" was given {len(a.parameters)} args")

    return f


@click.command()
@click.pass_context
@click.option(
    '--instances-dir-to-pipelines',
    type=click.STRING,
    required=True,
    help="""
    dir1=module1:func1,...,dirN=moduleN:funcN    
      + moduleX:funcX is a function that returns a dry_pipe.Pipeline    
      + dirX is the parent directory where all pipeline_instance_dirs of PipelineX will reside  
    """
)
@click.option('--bind', default="0.0.0.0", help="bind address for --web-mon")
@click.option('--port', default=5000)
def serve_ui(ctx, instances_dir_to_pipelines, bind, port):
    from dry_pipe.websocket_server import WebsocketServer

    _configure_logging(None)

    def g():
        for t in instances_dir_to_pipelines.split(","):

            instances_dir, mod_func = t.split("=")

            if not os.path.exists(instances_dir):
                pathlib.Path(instances_dir).mkdir(parents=True)

            pipeline = Pipeline.load_from_module_func(mod_func)
            yield instances_dir, pipeline

    WebsocketServer.start(bind, port, dict(g()))

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
@click.argument('test-case-mod-func', type=click.STRING)
def test(ctx, test_case_mod_func):

    idx = None

    if test_case_mod_func.count(":") == 1:
        mod, func_name = test_case_mod_func.split(":")
    elif test_case_mod_func.count(":") == 2:
        mod, func_name, idx = test_case_mod_func.split(":")
        idx = int(idx)
    else:
        raise Exception(
            f"expected an argument of the form <module>:<function_name>, or <module>:<function_name>:<number>"
        )

    module = importlib.import_module(mod)

    python_call = getattr(module, func_name, None)

    if python_call is None:
        raise Exception(f"function {func_name} not found in module {mod}")

    if not isinstance(python_call, PythonCall):
        raise Exception(f"function {func_name} should be decorated with @DryPipe.python_call(tests=[...])")

    if len(python_call.tests) == 0:
        click.echo(f"python_task {test_case_mod_func} has no tests")
        return

    def run_test(python_test_case):
        init_func = python_test_case.get("init_func")

        if init_func is not None:
            init_func()

        res = python_call.func(*python_test_case["args"], test=python_test_case)

        expects = python_test_case.get("expects")
        if expects is not None:
            if res != expects:
                raise Exception(f"expected {json.dumps(expects, indent=2)}\ngot:\n{json.dumps(res, indent=2)}")

    c = 0

    if idx is None:
        for python_test_case in python_call.tests:
            run_test(python_test_case)
            print(f"test passed: {mod}:{func_name}:{c}")
            c += 1
    else:
        run_test(python_call.tests[idx])
        print(f"test passed: {test_case_mod_func}")


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

    control_dir = env["__control_dir"]

    task_logger = create_task_logger(control_dir)

    var_type_dict = {}

    meta = {
        k : v
        for k, v in env.items()
        if k.startswith("__meta_")
    }

    for producing_task_key, var_metas, _ in parse_in_out_meta(meta):
        #if producing_task_key != "":
        #    continue

        for var_meta in var_metas:
            name_in_producing_task, var_name, typez = var_meta
            var_type_dict[var_name] = typez


    def get_and_parse_arg(k, allow_none):
        v = env.get(k)
        if allow_none and v is None:
            return None
        if v is None and k != "test":
            tk = os.environ['__task_key']
            raise Exception(
                f"Task {tk} called {mod_func} with None assigned to arg {k}\n" +
                f"make sure task has var {k} declared in it's consumes clause. Ex:\n" +
                f"  dsl.task(key={tk}).consumes({k}=...)"
            )
        typez = var_type_dict.get(k)
        if typez == "int":
            return int(v)
        elif typez == "str":
            return v
        elif typez == "float":
            return float(v)
        elif typez == "glob_expression":

            class GlobExpression:
                def __call__(self, *args, **kwargs):
                    return glob.glob(os.path.expandvars(v))

                def __str__(self):
                    return os.path.expandvars(v)

            return GlobExpression()

        return v

    args_tuples = [
        (k, get_and_parse_arg(k, allow_none=False))
        for k, v in python_task.signature.parameters.items()
        if not k == "kwargs"
    ]

    args = [v for _, v in args_tuples]

    if "kwargs" not in python_task.signature.parameters:
        kwargs = {}
    else:

        args_names = [k for k, _ in args_tuples]

        task_logger.debug("args list: %s", args_names)

        kwargs = {
            k : get_and_parse_arg(k, allow_none=True)
            for k, _ in var_type_dict.items()
            if k not in args_names
        }

        for k, v in meta.items():
            if v.startswith("file") and k not in kwargs:
                f = k[7:]
                if f not in args_names:
                    kwargs[f] = env.get(f)


    task_logger.info("will invoke PythonCall: %s(%s,%s)", mod_func, args, kwargs)

    out_vars = None

    try:
        out_vars = python_task.func(* args, ** kwargs)
    except Exception as ex:
        traceback.print_exc()
        logging.shutdown()
        exit(1)

    if out_vars is not None and not isinstance(out_vars, dict):
        raise Exception(
            f"function {python_task.mod_func()} called by task {os.environ.get('__task_key')} {type(out_vars)}" +
            f"@DryPipe.python_call() can only return a python dict, or None"
        )

    try:
        if out_vars is not None:
            prev_out_vars = dict(iterate_out_vars_from(os.environ["__output_var_file"]))

            for k, v in out_vars.items():
                try:
                    v = json.dumps(v)
                    prev_out_vars[k] = v
                except TypeError as ex0:
                    print(
                        f"task call {control_dir}.{mod_func} returned var {k} of unsupported type: {type(v)}" +
                        " only primitive types are supported.",
                        file=sys.stderr
                    )
                    logging.shutdown()
                    exit(1)

            write_out_vars(prev_out_vars)

        #for pid_file in glob.glob(os.path.join(control_dir, "*.pid")):
        #    os.remove(pid_file)

    except Exception as ex:
        traceback.print_exc()
        logging.shutdown()
        exit(1)

    logging.shutdown()


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
def prepare_remote_sites(ctx, pipeline):

    pipeline_spec = pipeline

    if pipeline_spec is None:
        click.echo("must specify pipeline, ex: -p <aModule>:<aPipelineFunction>")
        return

    pipeline = Pipeline.load_from_module_func(pipeline_spec)

    from rich.console import Console
    from rich.panel import Panel
    console = Console()

    for f, msg in pipeline.prepare_remote_sites_funcs():
        console.print(Panel(f"[turquoise2]{msg}"))
        with console.status("rsync upload in progress..."):
            f()

    console.print(f"done.")

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
    cli_group.add_command(dump_remote_task_confs)
    cli_group.add_command(prepare_remote_sites)

def run_cli():
    _register_commands()
    cli_group()

if __name__ == '__main__':
    run_cli()
