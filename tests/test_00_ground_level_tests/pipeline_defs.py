import os
import pathlib
import shutil

from dry_pipe import DryPipe, cli


def single_task_pipeline(dsl):
    yield dsl.task(
        key="single-task"
    ).consumes(
        fasta_file=dsl.file("dummy.fasta")
    ).produces(
        inflated_output=dsl.file("inflated-dummy.fasta"),
        huge_variable=dsl.var(int)
    ).calls("fasta-inflater.sh")()


def single_task_pipeline_with_inline_script(dsl):
    yield dsl.task(
        key="single-task"
    ).consumes(
        fasta_file=dsl.file("dummy.fasta")
    ).produces(
        inflated_output=dsl.file("inflated-dummy.fasta"),
        huge_variable=dsl.var(int)
    ).calls("""
        #!/usr/bin/env bash            
        if [[ "${PLEASE_CRASH}" == "$__task_key" ]]; then
          exit 1
        fi
        echo mul\\
             ti\\
             line \\
             message                    
        cat $fasta_file > $inflated_output            
        cat $fasta_file >> $inflated_output            
        echo "huge_variable=123" >> $__output_var_file                
    """)()


def pipeline_exerciser(v1, f1, v2, f2, pdir, skip_reset=False, dsl_tweak=lambda i: i):

    pipeline_instance_dir = os.path.join(
        os.path.dirname(__file__),
        pdir
    )

    if not skip_reset:
        shutil.rmtree(pipeline_instance_dir, ignore_errors=True)
        pathlib.Path(pipeline_instance_dir).mkdir(parents=True)

    def create_pre_ex_file(content, file_name):
        with open(os.path.join(pipeline_instance_dir, file_name), "w") as _f:
            _f.write(f"{content}")

    create_pre_ex_file(f1, "f1.txt")

    create_pre_ex_file(f2, "f2.txt")

    def gen_tasks(dsl):

        t1 = dsl.task(
            key="t1"
        ).consumes(
            v1=dsl.val(v1),
            f1=dsl.file("f1.txt")
        ).produces(
            t1_out_v=dsl.var(int),
            t1_out_f=dsl.file("t1_out_f.txt")
        ).calls(
            pipeline_exerciser_func1
        )()

        yield t1

        t2 = dsl.task(
            key="t2"
        ).consumes(
            v2=dsl.val(v2),
            f2=dsl.file("f2.txt"),
            t1_out_v=t1.out.t1_out_v,
            t1_out_f=t1.out.t1_out_f,
        ).produces(
            t2_out_v=dsl.var(int),
            t2_out_f=dsl.file("t2_out_f.txt")
        ).calls(
            pipeline_exerciser_func2
        )()

        yield t2

    pipeline = DryPipe.create_pipeline(gen_tasks, dsl_tweak(DryPipe.dsl(pipeline_code_dir=pipeline_instance_dir)))

    def signature_loader():
        t1, t2 = pipeline.tasks
        return [t1.input_signature(), t1.output_signature(), t2.input_signature(), t2.output_signature()]

    def inputs_are_stale_loader():
        t1, t2 = pipeline.tasks

        return [t1.get_state().is_input_signature_changed(), t2.get_state().is_input_signature_changed()]

    return pipeline, signature_loader, inputs_are_stale_loader


def load_int_from_file(f):
    with open(f) as f:
        return int(f.read().strip())



test_args_for_pipeline_exerciser_func1 = {

}
@DryPipe.python_call(tests=[])
def pipeline_exerciser_func1(v1, f1, t1_out_f, test=None):

    #TODO: format variables so that they so that they survive the bash export :
    v1 = int(v1)

    _f1 = load_int_from_file(f1)

    with open(t1_out_f, "w") as f:
        f.write(str(_f1 + v1))

    return {
        "t1_out_v": (_f1 + v1)
    }


@DryPipe.python_call()
def pipeline_exerciser_func2(t1_out_v, t1_out_f, v2, f2, t2_out_f):

    v2 = int(v2)

    _t1_out_f = load_int_from_file(t1_out_f)

    _f2 = load_int_from_file(f2)

    t1_out_v = int(t1_out_v)

    with open(t2_out_f, "w") as f:
        f.write(str( + _t1_out_f))

    return {
        "t2_out_v": (t1_out_v + _t1_out_f) + v2
    }

