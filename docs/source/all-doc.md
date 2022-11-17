# Getting Started

## Install

DryPipe is installed in a virtualenv like any python library

```shell
pyton3 -m venv your_venv 
source your_venv/bin/activate
pip install dry-pipe
```

Once installed the [DryPipe CLI](section_drypipe_cli) is available

```shell
drypipe --help
```

# Writing Pipelines

## DSL elements 

### Dividing work into smaller chunks

Here's an example of a 2 task pipeline. 

Note: tasks t1 and t2 are independent (no data is passed between them)
as a result, DryPipe will run them in parallel.

```python

def my_pipeline_dag_generator(dsl):

  yield dsl.task(
        key="t1"
    ).produces(
        a_file=dsl.file("a-file.txt")
    ).calls("""
        #!/usr/bin/env bash
        echo 'hello' > $a_file
    """)()

  yield dsl.task(
        key="t2"
    ).produces(
        other_file=dsl.file("another-file.txt")
    ).calls("""
        #!/usr/bin/env bash
        echo 'world' > $other_file
    """)()
```

### File dependencies between Tasks

Most pipelines have tasks that depend on data produced by other tasks.

In the following example, task t2 consumes a file produced by t1.

DryPipe will launch task t2, when (and only when) task t1 has successfully completed. 

Check out [how data is passed between tasks] to see how that works under the hood.

```python

def my_pipeline_dag_generator(dsl):

  t1 = dsl.task(
        key="t1"
    ).consumes(
        x=dsl.val(123)
    ).produces(
        result=dsl.file("f.txt")
    ).calls("""
        echo $(( x * x )) > $result
    """)()
  yield t1

  yield dsl.task(
        key="t2"
    ).consumes(
        r=t1.out.result
    ).produces(
        f=dsl.file("final-result.txt")
    ).calls("""
        result_from_t1="$(cat $r)"
        echo "we got data from t1: $result_from_t1" > $f 
    """)()
```

The calls(...) clause can also take the name of a bash script, ex: ```calls("my-script.sh")```, the path of the script
is resolved by ```$__pipeline_code_dir/my-script.sh``` (see [environment variables](section_environment_variables))

### Variable dependencies between Tasks

When a task only consumes a variable (int, float, str) from an upstream task, it is more convenient to just pass the variable, 
as opposed to having the producing file write it to a file and then reading from the consuming task.  

```python
def my_pipeline_dag_generator(dsl):

  t1 = dsl.task(
        key="t1"
    ).consumes(
        x=dsl.val(123),
        y=dsl.val(3.14159265359),
        z=dsl.val('abc')
    ).produces(
        result=dsl.var(int)
    ).calls("""
        echo "all variables in the consumes(...) clause are in the env" $x, $y, $z
        # 'export' is used to assign output variable in the produces(...) clause 
        export result=$(( x * y ))
    """)()
  yield t1

  yield dsl.task(
        key="t2"
    ).consumes(
        r=t1.out.result
    ).produces(
        f=dsl.file("final-result.txt")
    ).calls("""      
        echo "we got data from t1: $r" > $f 
    """)()
```

### Calling Python functions from Tasks

The calls(...) clause of tasks, can take bash snippets (as in previous examples), or Python functions, provided that they
are annotated with ```@DryPipe.python_call()```

The following pipeline does the same things as the previous, but uses python functions instead of bash snippets.

Note1: functions have their var arguments correctly parsed, according to the types specified in the generator function. 

Note2: also how output vars from the produces(...) clause are also passed as arguments.

Note3: output variables (declared in the produces(...) clause) are returned by functions with a python ```dict()```

Note4: print("abc") will write to in $__pipeline_instance_dir/.drypipe/$__task_key/out.log (see [DryPipe assigned environment variables of tasks])

```python
import sys
from dry_pipe import DryPipe


@DryPipe.python_call()
def f1(x, y, z):
    print(f"all variables in the consumes(...) clause are in the env {x}, {y}, {z}")

    if isinstance(x, int):
        print("Wow, x has the correct type !")
    else:
        # will write to  $__pipeline_instance_dir/.drypipe/$__task_key/err.log
        print("Something is seriously wrong !", file=sys.stderr)

    x_from_env = os.environ["x"]
    if isinstance(x_from_env, str):
        print("""one could grab vars from the environment, but
                 receiving pre parsed arguments is more convenient !""")

    print("")
    return {
        "result": x * y
    }


@DryPipe.python_call()
def f2(r, f):
    with open(f) as f_handle:
        f_handle.write(f"we got data from t1: {r}")


def my_pipeline_dag_generator(dsl):
    t1 = dsl.task(
        key="t1"
    ).consumes(
        x=dsl.val(123),
        y=dsl.val(3.14159265359),
        z=dsl.val('abc')
    ).produces(
        result=dsl.var(int)
    ).calls(
        f1
    )()

    yield t1

    yield dsl.task(
        key="t2"
    ).consumes(
        r=t1.out.result
    ).produces(
        f=dsl.file("final-result.txt")
    ).calls(
        f2
    )()
```

# Directory structure

DryPipe tasks run in separate processes, and have two dedicated directories.

Every task in a pipeline instance has:

1. an output directory: $__pipeline_instance_dir/output/$task_key
2. a working directory: $__pipeline_instance_dir/.drypipe/$task_key

The name of the task directories is equal to the task key.

It's one of the reason DryPipe enforces uniqueness of task keys returned by the dag generator function. 

The following table describing DryPipe assigned environment variables helps understanding the directory structure of a pipeline instance 

Warning: it is considered bad practice for a task to write in a directory other than it's dedicated $__task_output_dir (see [well behaved tasks]())

# Environment Variables
(section_environment_variables)=

| variable name            | descriptio                              | equivalent                                                                    |
|--------------------------|-----------------------------------------|-------------------------------------------------------------------------------|
| $__pipeline_instance_dir | path of the pipeline instance directory |                                                                               |
 | $__pipeline_code_dir     | path to the instance's code directory   |                                                                               |
 | $__task_output_dir       | the task's dedicated output dir         | $__pipeline_instance_dir/output/$task_key                                     |
| $__out_log               | destination of the task'S stdout        | $__pipeline_instance_dir/.drypipe/$task_key/out.log                           |
| $__err_log               | destination of the task'S stderr        | $__pipeline_instance_dir/.drypipe/$task_key/err.log                           |
 | $__scratch_dir           | temp working directiry                  | $SLURM_TMPDIR if tasks running in Slurm, otherwise $__task_output_dir/scratch |

# DryPipe CLI
(section_drypipe_cli)=

## Running a pipeline instance

```shell
drypipe run -p my_module:my_pipeline_creator_function
```

## Launch pipeline instances from a watch dir

todo: write the doc

## monitor pipelines with the Web interface

todo: write the doc