import collections
import datetime
import glob
import hashlib
import json
import os
import pathlib
import shutil
import time
from itertools import groupby

import textwrap

from dry_pipe.script_lib import parse_in_out_meta, PortablePopen
from dry_pipe.actions import TaskAction
from dry_pipe import bash_shebang
from dry_pipe.internals import \
    IndeterminateFile, ProducedFile, IncompleteVar, Val, \
    OutputVar, InputFile, InputVar, FileSet, OutFileSet, ValidationError, flatten, TaskProps

from dry_pipe.script_lib import task_script_header, iterate_out_vars_from
from dry_pipe.task_state import TaskState, tail


class Task:

    @staticmethod
    def key_grouper(task_key):
        if "." not in task_key:
            return task_key
        return task_key.split(".")[0]

    @staticmethod
    def load_from_task_state(task_state):
        return Task(None, task_state)

    def __init__(self, task_builder, task_state=None):

        if task_state is not None:
            self.key = task_state.task_key

            class ShallowPipelineInstance:
                def __init__(self):
                    self.pipeline_instance_dir = task_state.pipeline_instance_dir()

            self.pipeline_instance = ShallowPipelineInstance()

            from dry_pipe import TaskConf

            self.task_conf = TaskConf.from_json_file(os.path.join(
                task_state.control_dir(), "task-conf.json"
            ))
            self.executer = self.task_conf.create_executer()
            return

        self.python_bin = None
        self.conda_env = None
        self.key = task_builder.key
        self.pipeline_instance = task_builder.pipeline_instance
        self.task_steps = task_builder.task_steps
        self.props = TaskProps(self, task_builder._props)
        self.upstream_task_completion_dependencies = task_builder._upstream_task_completion_dependencies
        self.produces = task_builder._produces
        self.task_conf = task_builder.task_conf

        self.has_python_step = False

        for s in self.task_steps:
            if s.python_call is not None:
                self.has_python_step = True
                break

        def gen_produced_items_dict():
            fileset_count = 0
            for k, v in task_builder._produces.items():
                if isinstance(v, IncompleteVar):
                    yield k, OutputVar(v.type, v.may_be_none, k, self)
                elif isinstance(v, IndeterminateFile):
                    yield k, v.produced_file(k, self)
                elif isinstance(v, FileSet):
                    yield k, v.out_file_set(self, k)
                else:
                    raise Exception(f"produced type {v}")

        self.produces = dict(gen_produced_items_dict())

        vals = {}
        pre_existing_files = {}
        input_vars = {}
        task_matchers = {}

        def gen_dependent_items_dict():
            for k, v in task_builder._consumes.items():
                if isinstance(v, IndeterminateFile):
                    pre_existing_files[k] = v.pre_existing_file()
                elif isinstance(v, OutputVar):
                    iv = v.input_var(k)
                    input_vars[k] = iv
                    yield k, iv
                elif isinstance(v, ProducedFile):
                    yield k, v.input_file(k)
                elif isinstance(v, Val):
                    vals[k] = v
                else:
                    raise Exception(f"unknown dep type {v}")

        self.upstream_deps_iterator = Task._create_upstream_deps_iterator(dict(gen_dependent_items_dict()))

        def sort_dict(d):
            return collections.OrderedDict(sorted([(k, v) for k, v in d.items()]))

        self.vals = sort_dict(vals)

        self.pre_existing_files = sort_dict(pre_existing_files)

        self.input_vars = sort_dict(input_vars)

        self.task_matchers = sort_dict(task_matchers)

        self.out = TaskOut(self, self.produces)

    def __repr__(self):
        return f"Task(key={self.key})"

    def __getattr__(self, name):
        self.out.check_for_mis_understanding_of_dsl(name)

    def suffix(self):
        return self.key.split(".")[1]

    def as_json(self):
        raise Exception(f"deprecated, use get_state().as_json()")

    def all_produced_files(self):
        for f, item in self.out.produces.items():
            if isinstance(item, ProducedFile):
                yield item

    @staticmethod
    def _create_upstream_deps_iterator(consumes_items):

        def gen_input_vars():
            for _can_ignore_var_name_in_consuming_task, input_var in consumes_items.items():
                if isinstance(input_var, InputVar):
                    yield input_var

        def group_func(dv):
            return dv.output_var.producing_task.key

        def upstream_tasks_from_input_var_deps():
            for _task_key, input_vars in groupby(sorted(gen_input_vars(), key=group_func), group_func):
                input_vars = list(input_vars)
                upstream_task = input_vars[0].output_var.producing_task
                yield upstream_task, 0, input_vars

        def gen_input_files():
            for _can_ignore_var_name_in_consuming_task, input_file in consumes_items.items():
                if isinstance(input_file, InputFile):
                    yield input_file

        def group_func_2(input_file):
            return input_file.produced_file.producing_task.key

        def upstream_tasks_from_input_file_deps():
            for _task_key, input_files in groupby(sorted(gen_input_files(), key=group_func_2), group_func_2):
                input_files = list(input_files)
                upstream_task = input_files[0].produced_file.producing_task
                yield upstream_task, 1, input_files

        merged_tasks = list(upstream_tasks_from_input_file_deps()) + list(upstream_tasks_from_input_var_deps())

        def merge_grouper(t):
            return t[0].key

        def sort_deps(deps):
            return list(sorted(flatten(deps), key=lambda d: d.var_name_in_consuming_task))

        def gen_it():
            for _k, tu3 in groupby(sorted(merged_tasks, key=merge_grouper), key=merge_grouper):
                tu3 = list(tu3)
                upstream_input_vars = sort_deps([l for _k, i, l in tu3 if i == 0])
                upstream_input_files = sort_deps([l for _k, i, l in tu3 if i == 1])
                upstream_task = tu3[0][0]

                yield upstream_task, upstream_input_files, upstream_input_vars

        cached_it = list(sorted(
            gen_it(),
            key=lambda t: t[0].key
        ))

        return lambda: cached_it

    def _input_meta_data(self):
        # export __meta_<var-name>="(int|str|float):<producing-task-key>:<name_in_producing_task>"
        # export __meta_<file-name>="file:<producing-task-key>:<name_in_producing_task>"
        for _, upstream_input_files, upstream_input_vars in self.upstream_deps_iterator():
            for input_file in upstream_input_files:
                produced_file = input_file.produced_file
                yield (
                    f"__meta_{input_file.var_name_in_consuming_task}",
                    ":".join([
                        "file",
                        produced_file.producing_task.key,
                        produced_file.file_path
                    ])
                )

            for input_var in upstream_input_vars:
                output_var = input_var.output_var
                yield (
                    f"__meta_{input_var.var_name_in_consuming_task}",
                    ":".join([
                        output_var.type_str(),
                        output_var.producing_task.key,
                        output_var.name
                    ])
                )

        for name, produced_item in self.produces.items():
            if isinstance(produced_item, ProducedFile):
                rel_path = produced_item.absolute_path(self)
                yield (
                    f"__meta_{produced_item.var_name}",
                    ":".join([
                        "file",
                        "",
                        rel_path
                    ])
                )
            elif isinstance(produced_item, OutputVar):
                yield (
                    f"__meta_{name}",
                    ":".join([
                        produced_item.type_str(),
                        "",
                        produced_item.name
                    ])
                )

        for name, val in self.vals.items():
            yield (
                f"__meta_{name}",
                ":".join([
                    "glob_expression" if val.is_glob_expression else val.type_str(),
                    "",
                    name
                ])
            )

        for name, file in self.pre_existing_files.items():
            yield (
                f"__meta_{name}",
                ":".join([
                    "file",
                    "",
                    ""
                ])
            )

        yield "END_META", "END_META"

    def get_env_vars(self, writer, collect_deps_and_outputs_func=None):

        yield "__task_key", self.key

        for k, v in self._input_meta_data():
            yield k, v

        gen_input_var_call = f'$__pipeline_instance_dir/.drypipe/script_lib gen-input-var-exports'
        writer.write(f'eval "$({gen_input_var_call})"\n')

        def abs_from_pipeline_instance_dir(p):
            return f"$__pipeline_instance_dir/{p}"

        yield "__is_remote", str(self.is_remote())

        yield "__control_dir", abs_from_pipeline_instance_dir(f"{self.control_dir()}")
        yield "__task_output_dir", abs_from_pipeline_instance_dir(f"{self.work_dir()}")
        yield "__pipeline_instance_name", os.path.basename(self.pipeline_instance.pipeline_instance_dir)

        #if isinstance(self.executer, Slurm):
        #    yield "__is_slurm", "True"

        if self.task_conf.uses_singularity():
            yield "__is_singularity", "True"
            #yield "__container_image", self.container.image_path

            #if self.container.binds:
            #    binds = ",".join([
            #        f"{in_dir}={out_dir}"
            #        for in_dir, out_dir in self.container.binds.items()
            #    ])
            #    yield "SINGULARITY_BIND", binds

        yield "__scratch_dir", self.v_exp_scratch_dir()
        yield "__output_var_file", self.v_exp_output_var_file()
        yield "__sig_dir", "out_sigs",
        yield "__out_log", self.v_exp_out_log()
        yield "__err_log", self.v_exp_err_log()

        out_files = []

        for k, v in self.out.produces.items():
            if isinstance(v, ProducedFile):
                yield k, abs_from_pipeline_instance_dir(v.absolute_path(self))
                out_files.append(abs_from_pipeline_instance_dir(v.absolute_path(self)))
                if collect_deps_and_outputs_func is not None:
                    collect_deps_and_outputs_func(None, v.absolute_path(self))
            elif isinstance(v, OutFileSet):
                if collect_deps_and_outputs_func is not None:
                    collect_deps_and_outputs_func(None, os.path.join(self.work_dir(), v.file_set.glob_pattern))
                yield "__fileset_to_sign", f"$__task_output_dir/{v.file_set.glob_pattern}"

        yield "__file_list_to_sign", ",".join(out_files)

        for k, v in self.vals.items():
            yield k, v.serialized_value()

        for k, v in self.pre_existing_files.items():
            ap = v.absolute_path(self)
            if collect_deps_and_outputs_func is not None:
                collect_deps_and_outputs_func(ap, None, v.remote_cache_bucket)
            if os.path.isabs(ap):
                yield k, ap
            else:
                yield k, abs_from_pipeline_instance_dir(ap)

        for upstream_task, upstream_input_files, upstream_input_vars in self.upstream_deps_iterator():
            for input_file in upstream_input_files:

                if collect_deps_and_outputs_func is not None:
                    collect_deps_and_outputs_func(input_file.produced_file.absolute_path(upstream_task), None)

                yield input_file.var_name_in_consuming_task, \
                      abs_from_pipeline_instance_dir(input_file.produced_file.absolute_path(upstream_task))

            #for k, v in upstream_task.resolve_output_vars_for_consuming_task(self):
            #    yield k, v

    def read_out_signatures_file_into_dict(self):
        def d():
            with open(self._single_glob_in_control_dir("out.*.sig")) as f:
                for line in f.readlines():
                    file_name, sig, path = line.split("\t")
                    if path != "":
                        yield file_name, sig

        return dict(d())

    def _glob_in_control_dir(self, pattern):
        return glob.glob(os.path.join(self.v_abs_control_dir(), pattern))

    def _single_glob_in_control_dir(self, pattern):
        file_names = list(self._glob_in_control_dir(pattern))
        if len(file_names) != 1:
            raise Exception(f"expected 1 file for pattern '{pattern}' in {self.v_abs_control_dir()}, got {len(file_names)}")
        return file_names[0]

    def _delete_glob_if_exists(self, pattern, expect_exactly_1=True):

        if expect_exactly_1:
            f = self._single_glob_in_control_dir(pattern)
            os.remove(f)

        for f in self._glob_in_control_dir(pattern):
            os.remove(f)

    def calc_input_signature(self):

        sha1sum = hashlib.sha1()

        def add_sig(s):
            if s is not None:
                sha1sum.update(s.encode('utf-8'))

        hash_els = list(
            sorted(self.iterate_input_signature_elements(), key=lambda t: (t[3], t[0]))
        )

        for k, v, _i1, _i2 in hash_els:
            add_sig(k)
            add_sig('\t')
            add_sig(v)

        h = sha1sum.hexdigest()

        def signature_file_writer(write_changed_flag=False):

            changed_flat_file_name = self._input_signature_changed_file()

            if write_changed_flag:
                file_name = changed_flat_file_name
            else:
                if os.path.exists(changed_flat_file_name):
                    os.remove(changed_flat_file_name)

                file_name = self.input_signature_file()

            with open(file_name, "w") as f:
                f.write(h)
                f.write("\n")
                for k, v, path_if_file, _i3 in hash_els:
                    p = ''

                    if path_if_file is not None:
                        p = os.path.relpath(path_if_file, self.pipeline_instance.pipeline_instance_dir)

                    f.write(f"{k}\t{v}\t{p}\n")

        return h, signature_file_writer

    def write_input_signature(self):
        s, write_file = self.calc_input_signature()
        write_file()

    @staticmethod
    def _signature_from_sha1sum_file(sig_file):
        if os.path.exists(sig_file):
            with open(sig_file) as f:
                sig = f.read().split(" ")[0]
                return sig
        else:
            #TODO: FIXME !
            return "da39a3ee5e6b4b0d3255bfef95601890afd80709"

    def clear_input_changed_flag(self):
        f = self._input_signature_changed_file()
        if os.path.exists(f):
            os.remove(f)

    def signature_of_produced_file(self, produced_file_base_name, fail_func=lambda: None):

        sig_file = os.path.join(
            self.v_abs_control_dir(), "out_sigs",
            f"{os.path.basename(produced_file_base_name)}.sig"
        )

        if os.path.exists(sig_file):
            return Task._signature_from_sha1sum_file(sig_file)
        else:
            fail_func()

    def iterate_input_signature_elements(self):

        for k, v in self.vals.items():
            yield k, v.serialized_value(), None, 0

        for file_name, pre_existing_file in self.pre_existing_files.items():

            fn = os.path.basename(pre_existing_file.file_path)

            sig_file = os.path.join(
                self.pipeline_instance.pipeline_instance_dir, ".drypipe", "in_sigs", f"{fn}.sig")

            sig = Task._signature_from_sha1sum_file(sig_file)

            yield file_name, sig, None, 1

        for upstream_task, upstream_input_files, upstream_input_vars in self.upstream_deps_iterator():

            task_state = upstream_task.get_state()

            #if task_state.is_stale():
            #    raise UpstreamDepsChanged()

            for input_file in upstream_input_files:

                produce_file_abs_path = self.abs_from_pipeline_instance_dir(
                    input_file.produced_file.absolute_path(self)
                )

                sig = upstream_task.signature_of_produced_file(produce_file_abs_path)

                yield input_file.var_name_in_consuming_task, sig, produce_file_abs_path, 1

            for k, v in upstream_task.resolve_output_vars_for_consuming_task(self):
                yield k, str(v), None, 2

        for k, m in self.task_matchers.items():
            for upstream_task in self.pipeline_instance.tasks:
                task_state = upstream_task.get_state()
                if pathlib.PurePath(upstream_task.key).match(m.task_keys_glob_pattern):
                    if not task_state.is_completed():
                        raise MissingUpstreamDeps(
                            f"{self} can't calc input_hash, upstream task {upstream_task} has not completed"
                        )
                    #elif task_state.is_stale():
                    #    raise UpstreamDepsChanged()
                    else:
                        yield f"task:{upstream_task.key}", upstream_task.output_signature(), None, 2

    def _calc_output_signature(self):

        def iterate_output_signature_elements():

            for k, v in self.iterate_out_vars():
                yield k, str(v or ""), "", 0

            sig_files = os.path.join(
                self.v_abs_control_dir(), "out_sigs", "*.sig"
            )

            for sig_file in glob.glob(sig_files):

                def fail():
                    raise Exception(f"expected {sig_file} to exist")

                # strip .sig
                sig_file = sig_file[:-4]
                sig = self.signature_of_produced_file(sig_file, fail)
                path_within_pipeline_instance = os.path.relpath(sig_file, self.pipeline_instance.pipeline_instance_dir)
                yield os.path.basename(sig_file), sig, path_within_pipeline_instance, 1

        rows = list(
            sorted(iterate_output_signature_elements(), key=lambda t: (t[3], t[0]))
        )

        sha1sum = hashlib.sha1()

        def add_to_sig(s):
            sha1sum.update(s.encode('utf-8'))

        for name, item_sig, path_or_none, _i in rows:
            add_to_sig(name)
            add_to_sig("\t")
            add_to_sig(item_sig)

        task_out_sig = sha1sum.hexdigest()

        def rewrite_file_func():
            with open(self.v_abs_output_signature_file(), "w") as f:
                f.write(sha1sum.hexdigest())
                f.write("\n")
                for name, sig, path_or_none, _i in rows:
                    f.write(f"{name}\t{sig}\t{'' or path_or_none}\n")

            #sf = self._output_signature_stale_file()
            #if os.path.exists(sf):
            #    os.remove(sf)

        return task_out_sig, rewrite_file_func

    def write_output_signature_file(self):
        sig, write_file_func = self._calc_output_signature()
        write_file_func()

    def iterate_out_vars(self):
        of = self.v_abs_output_var_file()
        if os.path.exists(of):
            yield from iterate_out_vars_from(of)

    def out_vars_by_name(self):
        return dict(self.iterate_out_vars())

    def resolve_output_vars_for_consuming_task(self, consuming_task):

        consuming_vars = list([
            input_var
            for k, input_var in consuming_task.input_vars.items()
            if input_var.output_var.producing_task.key == self.key
        ])

        if len(consuming_vars) == 0:
            return

        produced_vars_values_by_name = self.out_vars_by_name()

        for consuming_input_var in consuming_vars:

            var_name_in_this_producing_task = consuming_input_var.output_var.name

            v = produced_vars_values_by_name.get(var_name_in_this_producing_task)

            if v is not None:
                name = consuming_input_var.var_name_in_consuming_task
                yield name, consuming_input_var.output_var.unformat_for_python(v)
                #if self.has_python_step:
                #    yield f"{name}_python", consuming_input_var.output_var.format_for_python(v)

            elif not consuming_input_var.output_var.may_be_none:
                msg = f"unmet dependency: {consuming_task}._consumes(" + \
                      f"{consuming_input_var.var_name_in_consuming_task}={self.key}.out." + \
                      f"{var_name_in_this_producing_task})" + \
                      f"\n{self} has NOT produced variable '{var_name_in_this_producing_task}' " + \
                      f"as declared in produces() clause"
                raise MissingOutvars(msg)

    def prepare(self):

        is_remote = self.is_remote()
        external_dep_file_list = []
        external_dep_file_pipeline_instance_list = []
        dependent_file_list = []
        output_file_list = []

        def collect_deps_and_outputs(dep_file, output_file, external_dep_type=None):
            if dep_file is not None:
                if os.path.isabs(dep_file):
                    if external_dep_type is None:
                        external_dep_file_list.append(dep_file)
                    elif external_dep_type == "pipeline-instance":
                        external_dep_file_pipeline_instance_list.append(dep_file)
                    else:
                        raise Exception(f"invalid remote_cache_bucket: {external_dep_type}")
                else:
                    dependent_file_list.append(dep_file)
            elif output_file is not None:
                output_file_list.append(output_file)
            else:
                raise Exception("collect_deps_and_outputs(None, None)")

        #self.write_input_signature()

        task_conf_file = self.v_abs_task_conf_file()

        with open(task_conf_file, "w") as f:
            f.write(json.dumps(self.task_conf.as_json(), indent=2))

        setenv_file = self.v_abs_task_env_file()

        with open(setenv_file, "w") as f:
            f.write(f"{bash_shebang()}\n\n")
            f.write('__script_location=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )\n')
            f.write("export __pipeline_instance_dir=$( dirname $( dirname $__script_location))\n")

            f.write("\n")

            f.write(". $__pipeline_instance_dir/.drypipe/pipeline-env.sh\n")

            if self.task_conf.init_bash_command is not None:
                f.write(self.task_conf.init_bash_command)
                f.write("\n")

            for k, v in self.get_env_vars(f, collect_deps_and_outputs if is_remote else None):
                f.write(f'export {k}={v}\n')

        os.chmod(setenv_file, 0o764)

        shell_script_file = self.v_abs_script_file()

        with open(shell_script_file, "w") as f:
            f.write(task_script_header())
            f.write("env = script_lib.source_task_env(os.path.join(__script_location, 'task-env.sh'))\n")
            f.write("task_conf_dict = script_lib.load_task_conf_dict()\n")
            f.write("script_lib.ensure_upstream_tasks_completed(env)\n\n")

            f.write("\n\ndef go():\n")
            f.write("\n    step_number, control_dir, state_file, state_name = script_lib.read_task_state()\n")

            step_number = 0
            for task_step in self.task_steps:
                task_step.write_invocation(f, self, step_number)
                step_number += 1

            f.write('    script_lib.transition_to_completed(state_file)\n\n\n')

            f.write('script_lib.handle_main(go)\n')
            f.write('logging.shutdown()\n')

        os.chmod(shell_script_file, 0o764)

        if self.task_conf.is_slurm():
            pid_name = os.path.basename(self.pipeline_instance.pipeline_instance_dir)

            with open(self.v_abs_sbatch_launch_script(), "w") as f:
                f.write(f"{bash_shebang()}\n\n")

                f.write(textwrap.dedent("""
                    __script_location=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )
                                                                            
                    __arg_1=$1
                """))

                f.write("\n".join([
                    f"__job_id=$(sbatch \\",
                    f"    {' '.join(self.task_conf.sbatch_options)} \\",
                    f"    --account={self.task_conf.slurm_account} \\",
                     "    --output=$__script_location/out.log \\",
                     "    --error=$__script_location/err.log \\",
                     "    --export=__script_location=$__script_location,__is_slurm=True,DRYPIPE_TASK_DEBUG=$DRYPIPE_TASK_DEBUG \\",
                     "    --signal=B:USR1@50 \\",
                     "    --parsable \\",
                    f"    --job-name={self.key}-{pid_name} $SBATCH_EXTRA_ARGS \\",
                     "    $__script_location/task)"
                ]))

                f.write("\n\n")

                f.write("echo $__job_id > $__script_location/slurm_job_id\n")
                f.write('echo "scancel --signal=SIGTERM $__job_id" > $__script_location/kill\n')
                f.write('chmod u+x $__script_location/kill\n')

            os.chmod(self.v_abs_sbatch_launch_script(), 0o764)

        if is_remote:
            local_deps = os.path.join(self.v_abs_control_dir(), "local-deps.txt")

            def write_external_deps_file(dep_list, out_file_name):
                if len(dep_list) > 0:
                    out_file = os.path.join(self.v_abs_control_dir(), out_file_name)
                    with open(out_file, "w") as _out_file:
                        for d in dep_list:
                            _out_file.write(d)
                            _out_file.write("\n")
                    return out_file
                else:
                    return None

            external_files_deps_file = write_external_deps_file(
                external_dep_file_list, "external-deps.txt"
            )

            external_files_deps_pipeline_instance_file = write_external_deps_file(
                external_dep_file_pipeline_instance_list, "external-deps-pipeline-instance.txt"
            )

            with open(local_deps, "w") as f:

                for dep_file in dependent_file_list:
                    f.write(dep_file)
                    f.write("\n")

                step_number = 0
                for step in self.task_steps:
                    if step.shell_snippet is not None:
                        f.write(self.step_script_file(step_number))
                        f.write("\n\n")
                    step_number += 1

                f.write(self.history_file())
                f.write("\n")
                f.write(self.task_env_file())
                f.write("\n")
                f.write(self.script_file())
                f.write("\n")
                f.write(self.task_conf_file())
                f.write("\n")
                if external_files_deps_file is not None:
                    f.write(os.path.join(self.control_dir(), os.path.basename(external_files_deps_file)))
                    f.write("\n")
                if external_files_deps_pipeline_instance_file is not None:
                    f.write(
                        os.path.join(self.control_dir(), os.path.basename(external_files_deps_pipeline_instance_file))
                    )
                    f.write("\n")

                if self.task_conf.is_slurm():
                    f.write(self.sbatch_launch_script())
                    f.write("\n")

            remote_outputs = os.path.join(self.v_abs_control_dir(), "remote-outputs.txt")

            with open(remote_outputs, "w") as f:

                for dep_file in output_file_list:
                    if not "*" in dep_file:
                        f.write(dep_file)
                        f.write("\n")

                f.write(self.history_file())
                f.write("\n")
                f.write(os.path.join(self.control_dir(), "out_sigs/"))
                f.write("\n")
                f.write(os.path.join(self.control_dir(), "output_vars"))
                f.write("\n")
                f.write(self.err_log())
                f.write("\n")
                f.write(self.out_log())
                f.write("\n")

            remote_output_filesets = os.path.join(self.v_abs_control_dir(), "remote-output-filesets.txt")
            output_filesets = [
                dep_file for dep_file in output_file_list if "*" in dep_file
            ]

            if len(output_filesets) > 0:
                with open(remote_output_filesets, "w") as f:
                    for dep_file in output_filesets:
                        f.write("+ ")
                        f.write(os.path.basename(dep_file))
                        f.write("\n")

    def seconds_since_last_activity(self):

        def st_mtime(f):
            p = pathlib.Path(f)
            if not p.exists():
                return 0

            return p.stat().st_mtime

        t_history_file, t_err_log, t_out_log = map(st_mtime, [
            self.v_abs_history_file(),
            self.v_abs_err_log(),
            self.v_abs_out_log()
        ])

        last_activity_time = max([t_history_file, t_err_log, t_out_log])

        if last_activity_time == 0:
            return 0

        delta = datetime.datetime.now() - datetime.datetime.fromtimestamp(last_activity_time)

        return delta.seconds

    def __is_zombie(self):

        raise Exception("implementation in progress")

        t = self.seconds_since_last_activity()

        if t == 0:
            return False

        slurm_ex = self.slurm_executor_or_none()

        if slurm_ex is None:
            return False

        slurm_job_id = self.slurm_job_id()

        if slurm_job_id is None:
            # WARNING
            raise Exception(f"")

        squeue_line = call_squeue_for_job_id(slurm_job_id)

        if squeue_line is not None:
            pathlib.Path(self.v_abs_history_file()).touch()
            return False

        task_state = self.get_state()

        if task_state.is_step_started() or task_state.is_launched() or task_state.is_scheduled():
            return True

        return False

    def is_remote(self):
        return self.task_conf.is_remote()

    def control_dir(self):
        return os.path.join(".drypipe", self.key)

    def work_dir(self):
        return os.path.join("output", self.key)

    def slurm_job_id(self):

        slurm_job_id_file = self.v_abs_slurm_job_id_file()

        if not os.path.exists(slurm_job_id_file):
            return None

        with open(slurm_job_id_file) as f:
            return f.read().strip()

    def slurm_job_id_file(self):
        return os.path.join(self.control_dir(), "slurm_job_id")

    def input_signature_file(self):
        return os.path.join(self.v_abs_control_dir(), "in.sig")

    def _input_signature_changed_file(self):
        return os.path.join(self.v_abs_control_dir(), "in.sig.changed")

    def is_input_signature_flagged_as_changed(self):
        return os.path.exists(self._input_signature_changed_file())

    def output_signature_file(self):
        return os.path.join(self.v_abs_control_dir(), "out.sig")

    def _output_signature_stale_file(self):
        return os.path.join(self.v_abs_control_dir(), "out.sig.stale")

    def _read_signature_from_sig_file(self, sig_file):
        #TODO: FIX ME:

        if not os.path.exists(sig_file):
            return "fixme"

        with open(sig_file) as f:
            return f.readline().strip()

    def output_signature(self):
        return self._read_signature_from_sig_file(self.output_signature_file())

    def input_signature(self):
        return self._read_signature_from_sig_file(self.input_signature_file())

    def output_var_file(self):
        return os.path.join(self.control_dir(), "output_vars")

    def task_env_file(self):
        return os.path.join(self.control_dir(), "task-env.sh")

    def task_conf_file(self):
        return os.path.join(self.control_dir(), "task-conf.json")

    def pid_file(self, pid):
        return os.path.join(self.control_dir(), f"{pid}.pid")

    def scratch_dir(self):
        if not self.task_conf.is_slurm():
            return os.path.join(self.work_dir(), "scratch")

        return None

    def script_file(self):
        return os.path.join(self.control_dir(), "task")

    def step_script_file(self, step_number):
        return os.path.join(self.control_dir(), f"step-{step_number}.sh")

    def sbatch_launch_script(self):
        return os.path.join(self.control_dir(), "sbatch-launcher.sh")

    def history_file(self):
        return os.path.join(self.control_dir(), "history.tsv")

    def input_hash_file(self, input_hash):
        return os.path.join(self.control_dir(), f"in.{input_hash}.sig")

    def abs_from_pipeline_instance_dir(self, path):
        return os.path.join(self.pipeline_instance.pipeline_instance_dir, path)

    def abs_from_pipeline_code_dir(self, path):
        return os.path.join(self.dsl.pipeline_code_dir, path)

    def pipeline_code_dir(self):
        return self.dsl.pipeline_code_dir

    def v_abs_control_dir(self):
        return self.abs_from_pipeline_instance_dir(self.control_dir())

    def v_abs_work_dir(self):
        return self.abs_from_pipeline_instance_dir(self.work_dir())

    def abs_path_of_produced_file(self, file_name):
        return os.path.join(self.v_abs_work_dir(), file_name)

    def max_retries(self):
        return 1

    def __getattr__(self, name):

        def fail():
            raise AttributeError(f"{self.__class__} has no attribute {name}")

        is_v_abs = name.startswith("v_abs_")
        is_v_exp = name.startswith("v_exp_")

        if not (is_v_abs or is_v_exp):
            fail()

        func_name = name[6:]
        func = getattr(self, func_name)

        if func is None:
            fail()

        if is_v_abs:
            def f(*args, **kwargs):
                d = func(*args, **kwargs)
                if d is None:
                    return None
                return os.path.join(self.pipeline_instance.pipeline_instance_dir, d)
            return f
        elif is_v_exp:
            def f(*args, **kwargs):
                return f"$__pipeline_instance_dir/{func(*args, **kwargs)}"
            return f
        else:
            raise Exception(f"how have we got here ?")

    def re_queue(self):
        task_state = self.get_state()
        task_state.transition_to_prepared(force=True)

    def has_completed(self):

        control_dir = self.v_abs_control_dir()

        if not os.path.exists(control_dir):
            return False

        f = [f for f in glob.glob(os.path.join(control_dir, "state.*"))]

        cnt = len(f)

        if cnt == 0:
            return False

        task_state = TaskState(os.path.abspath(f[0]))

        return task_state.is_completed()

    def get_state(self):
        control_dir = self.v_abs_control_dir()
        if not os.path.exists(control_dir):
            return None

        f = [f for f in glob.glob(os.path.join(control_dir, "state.*"))]

        cnt = len(f)

        if cnt == 1:
            return TaskState(os.path.abspath(f[0]))
        elif cnt == 0:
            return None

        raise Exception(f"expected one task state file (state.*) in {self}, got {len(f)}")

    def create_state_file_and_control_dir(self):

        for d in [
            self.v_abs_control_dir(),
            os.path.join(self.v_abs_control_dir(), "out_sigs"),
        ]:
            pathlib.Path(d).mkdir(parents=True, exist_ok=True)

        if not os.path.exists(self.v_abs_work_dir()):
            pathlib.Path(self.v_abs_work_dir()).mkdir(parents=True, exist_ok=True)

        if self.v_abs_scratch_dir() is not None:
            pathlib.Path(self.v_abs_scratch_dir()).mkdir(parents=True, exist_ok=True)

        return TaskState.create_non_existing(self.v_abs_control_dir())

    def _is_completed_and_input_hash_changed(self):

        task_state = self.get_state()

        if not task_state.is_completed():
            return False

        last_input_hash = self.input_signature()
        current_input_hash, file_writer = self.calc_input_signature()

        return last_input_hash != current_input_hash

    def verify_output_files_produced(self):

        if False:
            for name, produced_file in self.produces.items():
                if isinstance(produced_file, ProducedFile):
                    rel_path = produced_file.absolute_path(self)
                    file = self.abs_from_pipeline_instance_dir(rel_path)
                    if not os.path.exists(file):
                        raise Exception(f"{self} did not produce file '{name}':'{file}' as specified.")

            for producing_task_key, var_metas, file_metas in parse_in_out_meta(self.get_state().gen_meta_dict()):
                if producing_task_key != "":
                    continue

                for name_in_producing_task, var_name, typez in file_metas:
                    print("!")


    def reset_logs(self):

        if os.path.exists(self.v_abs_err_log()):
            os.remove(self.v_abs_err_log())

        if os.path.exists(self.v_abs_out_log()):
            os.remove(self.v_abs_out_log())

        if os.path.exists(self.v_abs_control_error_log()):
            os.remove(self.v_abs_control_error_log())

    def has_unsatisfied_deps(self):

        def iterator_is_empty(i):
            try:
                next(i)
                return False
            except StopIteration as e:
                return True

        return not iterator_is_empty(self.iterate_unsatisfied_deps())


    MISSING_PRE_EXISTING_FILE = 0
    UPSTREAM_TASK_NOT_COMPLETED = 1

    def iterate_unsatisfied_deps(self):

        for upstream_task in self.upstream_task_completion_dependencies:
            task_state = upstream_task.get_state()
            if not task_state.is_completed():
                yield f"{self} depends on {upstream_task} to be completed, it is in state: {task_state.state_name}", \
                    Task.UPSTREAM_TASK_NOT_COMPLETED, self, [], []

        for k, pre_existing_file in self.pre_existing_files.items():
            p = self.abs_from_pipeline_instance_dir(pre_existing_file.absolute_path(self))
            if not os.path.exists(p):
                yield f"pre existing file '{k}'='{p}' not found", Task.MISSING_PRE_EXISTING_FILE, self, [], []

        for upstream_task, upstream_input_files, upstream_input_vars in self.upstream_deps_iterator():

            task_state = upstream_task.get_state()

            if task_state is None or not task_state.is_completed():
                yield f"upstream task {upstream_task} has not completed", \
                      Task.UPSTREAM_TASK_NOT_COMPLETED, upstream_task, upstream_input_files, upstream_input_vars

        for k, m in self.task_matchers.items():

            matched_tasks = [
                upstream_task
                for upstream_task in self.pipeline_instance.tasks
                if pathlib.PurePath(upstream_task.key).match(m.task_keys_glob_pattern)
            ]

            if len(matched_tasks) == 0:
                yield f"aggregate task {self} has none of it's upstream task completed", \
                      Task.UPSTREAM_TASK_NOT_COMPLETED, None, [], []
                break

            for t in matched_tasks:
                if not t.get_state().is_completed():
                    yield f"at least one task required for {self} has not completed ({t.key})", \
                          Task.UPSTREAM_TASK_NOT_COMPLETED, t, [], []
                    break


    def out_log(self):
        return os.path.join(self.control_dir(), "out.log")

    def err_log(self):
        return os.path.join(self.control_dir(), "err.log")

    def control_error_log(self):
        return os.path.join(self.control_dir(), "control-err.log")

    def log_control_error(self, text, truncate=False):

        mode = "a"

        if truncate:
            mode = "w"

        with open(self.v_abs_control_error_log(), mode) as f:
            f.write("\n")
            f.write(text)

    def pid_file(self, pid):
        return os.path.join(self.control_dir(), f"{pid}.pid")

    def clean(self):
        shutil.rmtree(self.v_abs_control_dir(), ignore_errors=True)
        shutil.rmtree(self.v_abs_work_dir(), ignore_errors=True)

    def launch(self, executer, wait_for_completion=False, fail_silently=False):

        def touch_pid_file(pid):
            #pathlib.Path(self.v_abs_pid_file(pid)).touch(exist_ok=False)
            #TODO: figure out if PID tracking should be done
            pass

        executer.execute(self, touch_pid_file, wait_for_completion, fail_silently)

    """
        Recomputes out.sig (output signatures) of the task         
    """
    def recompute_output_singature(self, recalc_hash_script):

        with PortablePopen(
            f"bash -c '. {self.v_abs_task_env_file()} && {recalc_hash_script}'",
            env={
                "__sig_dir": "out_sigs",
                "__pipeline_instance_dir": self.pipeline_instance.pipeline_instance_dir
            },
            shell=True
        ) as p:
            p.wait_and_raise_if_non_zero()
            previously_computed_out_sig = self.output_signature()

            up_to_date_sig, out_sig_file_writer = self._calc_output_signature()

            if up_to_date_sig != previously_computed_out_sig:
                out_sig_file_writer()


class TaskStep:

    def __init__(self, task_conf, shell_script=None, python_call=None, shell_snippet=None):
        self.task_conf = task_conf
        self.shell_script = shell_script
        self.python_call = python_call
        self.shell_snippet = shell_snippet

    def write_invocation(self, file_writer, task, step_number):

        container = self.task_conf.container
        python_bin = self.task_conf.python_bin

        file_writer.write(f"\n    if step_number <= {step_number}:\n")

        def indented_line(line):
            file_writer.write("        ")
            file_writer.write(line)
            file_writer.write("\n")

        indented_line(
            'state_file, step_number = script_lib.transition_to_step_started(state_file, step_number)'
        )

        if container is None:
            container_arg = ""
        else:
            container_arg = f",'{container}'"

        if self.python_call is not None:
            indented_line(
                f"script_lib.run_python(task_conf_dict, '{self.python_call.mod_func()}'{container_arg})"
            )
        else:
            if self.shell_snippet is not None:
                script_or_snippet_file = \
                    f"os.path.join(os.environ['__pipeline_instance_dir'], '{task.step_script_file(step_number)}')"
                step_script = task.v_abs_step_script_file(step_number)
                with open(step_script, "w") as _step_script:
                    _step_script.write(self.shell_snippet)
                os.chmod(step_script, 0o764)
            elif self.shell_script is not None:
                script_or_snippet_file = f"os.path.join(os.environ['__pipeline_code_dir'], '{self.shell_script}')"
            else:
                raise Exception("shouldn't have got here")

            indented_line(f"script_lib.run_script({script_or_snippet_file}{container_arg})")

        indented_line(
            'state_file, step_number = script_lib.transition_to_step_completed(state_file, step_number)'
        )

        is_last_step = step_number == (len(task.task_steps) - 1)

        if is_last_step:
            indented_line("script_lib.sign_files()")

        file_writer.write("\n")


class TaskOut:

    def __init__(self, task, produces):
        self.task = task
        self.produces = produces

    def __getattr__(self, name):
        p = self.produces.get(name)

        if p is None:
            raise ValidationError(
                f"task {self.task} does not declare a variable '{name}' in it's produces() clause.\n" +
                f"Use task({self.task.key}).produces({name}=...) to specify output"
            )

        return p

    def check_for_mis_understanding_of_dsl(self, name):
        produced_file = self.produces.get(name)
        if produced_file is not None:
            raise ValidationError(
                f"please refer to task produced vars with task.out.{name}, not task.{name}."
            )


class MissingOutvars(Exception):
    def __init__(self, message):
        super(MissingOutvars, self).__init__(message)
        self.message = message


class MissingUpstreamDeps(Exception):
    pass


class UpstreamDepsChanged(Exception):
    pass
