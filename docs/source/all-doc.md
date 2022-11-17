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

Here's an example of a two task pipeline. 

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
  
  # we keep a reference to t1 so we can refer to t1.out.result
  
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

(user_env_vars_python)=

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

Every task in a pipeline instance has two dedicated directories:

1. an output directory: $__pipeline_instance_dir/output/$__task_key
2. a working directory: $__pipeline_instance_dir/.drypipe/$__task_key

Where $__task_key is the key assigned in the task declaration, ex: ```dsl.task(key="t123")``` 

To avoid name clashes, DryPipe enforces uniqueness of task keys. 

```{admonition} Warning
It's considered bad practice for a task to write in a directory other than it's dedicated $__task_output_dir (see [well behaved tasks]())
```

A two task pipeline with task keys "t1" and "t2", would have the directory structure below.

```
$__pipeline_instance_dir
│
└───.drypipe
│   │
│   └────t1
│      │   task
│      │   (pid|slurm_job_id)
│      │   task-env.sh
│      │   task-conf.json
│      │   state.(completed|launched|step-started.0|failed.0|killed.0|...)
│      │   out.log
│      │   err.log
│      │   drypipe.log
│      └─t2
│      │   task
│      │   ...
... 
└───output
│   │
│   └───t1
│       │   results.txt
│   └───t2
│       │   f.tsv
```

# Task launch script and variables

Environment variables are the mechanism by which DryPipe orchestrator parametrizes the tasks in a pipeline instance.

Tasks run as separate processes, and user code (passed in calls(...) clause) transparently receive their inputs and outputs as env variables.

## User Variables
(section_environment_variables)=

Task variables are declared with the consumes(...) and produces(...) clauses.

[Bash Calls] simply refer to them as $my_var, while [python calls] have their vars injected in function args, ex: ```def f(my_var)``` ([see example](user_env_vars_python))

All variables defined in these clauses end up as env variables in the task process.

## Task custom scripts

DryPipe generates the following scripts for every task in a pipeline instance:

1. $__pipeline_instance_dir/.drypipe/task: the script that runs the task
2. $__pipeline_instance_dir/.drypipe/task-env.sh: the script that loads the task env vars
3. $__pipeline_instance_dir/.drypipe/sbatch-launcher.sh: launches ./task as a slurm job 

Users normally don't have to deal with these scripts, but they can be useful for debugging.

A task can be manualy launched by executing the task script $__pipeline_instance_dir/.drypipe/task

Additionally, the task script has these other useful commands: 

+ ```task kill```: will kill the task 
+ ```task tail```: will do a multi tail -f of all three logs of the task (out.log, err.log, drypipe.log) 
+ ```task ps```: equivalent to calling "ps -p <pid of the task>" 


## DryPipe variables

The following table describing DryPipe assigned environment variables, pipeline coders will rarely need to access them.

They are documented here for the rare cases where they are needed and to help making the documentation more concise. 
Ex: we can refer to $__task_output_dir instead instead of "the task's output directory". 

| variable name            | description                                                 | equivalent                                                                                                                                               |
|--------------------------|-------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| $__pipeline_instance_dir | path of the pipeline instance directory                     |                                                                                                                                                          |
 | $__task_key              | ```dsl.tasl(key="a-unique-str-123")```                      |                                                                                                                                                          |
 | $__task_output_dir       | the task's dedicated output dir                             | $__pipeline_instance_dir/output/$__task_key                                                                                                              |
| $__out_log               | destination of the task's stdout                            | $__pipeline_instance_dir/.drypipe/$__task_key/out.log                                                                                                    |
| $__err_log               | destination of the task's stderr                            | $__pipeline_instance_dir/.drypipe/$__task_key/err.log                                                                                                    |
 | $__scratch_dir           | temp working directory                                      | $SLURM_TMPDIR if tasks running in Slurm, otherwise $__task_output_dir/scratch                                                                            |
 | $__pipeline_code_dir     | path to the instance's code directory                       | defaults to the directory of the python file where the DAG generator is coded, can be overriden. Useful for refering to scripts in a task's bash snippet |
| $__containers_dir        | the directory where containers used by the pipeline reside  |                                                                                                                                                          |

Note: $__pipeline_code_dir, and $__containers_dir are defined at the pipeline level, and can be overriden (see ...) 

# Runtime configuration

## Task level configs

Tasks in a pipeline have runtime configurations, that determin things such as:

1. where/how tasks are executed (locally launched processes ? Slurm node, ? remote ssh accessible machine)
2. how dependencies are managed, does a task run in a virtualenv, a conda env, a container ?
 
A pipeline instance can have tasks run in more than one host (and more than one slurm cluster), and DryPipe will
ensure that 

+ the input of every task (specified in it's consumes(...) clause) is rsynced on the host where it's executed
+ the output of every task is downloaded (rsynced) on the orchestrating host (the one running ```drypipe run```)

```python
from dry_pipe import TaskConf

def my_pipeline_dag_generator(dsl):
 
    # Task will run on the local machine (default task_conf of tasks)
    yield dsl.task(
        key="t1",
        task_conf=TaskConf(
            executer_type="process"
        )
    ).calls(f1)

    # Task will run as a slurm job (orchestrating host must be a slurm login node)
    yield dsl.task(
        key="t2",
        task_conf=TaskConf(
            executer_type="slurm",            
            slurm_account="my-slurm-account",
            sbatch_options= [
                "--time=0:5:00"
            ],
            container="my-container.sif"            
        )
    ).calls(f2)

    yield dsl.task(
        key="t3",
        task_conf=TaskConf(
            executer_type="slurm",
            ssh_specs="me@a-slurm-login-host:~/.ssh/id_rsa",
            container="my-container.sif",            
            remote_base_dir="/remote-parent-dir-of-pipeline-instance",
            remote_containers_dir="/remote-parent-dir-of-container-sif-files",         
            slurm_account="my-slurm-account",
            sbatch_options= [
                "--time=0:5:00"
            ]
        )
    ).calls(f3)

    yield dsl.task(
        key="t1",
        task_conf=TaskConf(
            executer_type="process",
            ssh_specs="me@a-slurm-login-host:~/.ssh/id_rsa"
        )
    ).calls(f4)

```

Note1: TaskConf of t3 will causes the following:

It will run on host "a-slurm-login-host" in the following directories
```
/remote-parent-dir-of-pipeline-instance/$__pipeline_instance_dir/output/t3
/remote-parent-dir-of-pipeline-instance/$__pipeline_instance_dir/.drypipe/t3
```

using the container "my-container.sif" in the remote host in the directory $__remote_containers_dir: 
```
/remote-parent-dir-of-container-sif-files/my-container.sif
```

## Pipeline level configs

The following example shows how to override $__pipeline_code_dir and $__containers_dir, as well as the pipeline's default TaskConf.

+ task-123 will get the pipeline default TaskConf
+ task-z will get it's own TaskConf

```python
from dry_pipe import TaskConf, DryPipe

def my_dag(dsl):
    yield dsl.task(key="task-123").calls("""
        #!/usr/bin/env bash
        echo "hello world"
    """)
    
    yield dsl.task(
        key="task-z", 
        task_conf=TaskConf(executer_type="process")
    ).calls("""
        #!/usr/bin/env bash
        echo "hello world"
    """)    

def my_pipeline():
    return DryPipe.create_pipeline(
        my_dag, 
        pipeline_code_dir="x/y/z",
        containers_dir="a/b/c",
        task_conf=TaskConf(
            executer_type="slurm",            
            slurm_account="my-slurm-account",            
        )
    )  

```


# DryPipe CLI
(section_drypipe_cli)=

## Running a pipeline instance

```shell
drypipe run -p my_module:my_pipeline_creator_function --instance-dir=/a/b/c
```

## Launch pipeline instances from a watch dir

todo: write the doc

## monitor pipelines with the Web interface

todo: write the doc

# Definitions

+ Orchestrating Host: the computer where ```drypipe run``` or ```drypipe watch``` is executed
+ Pipeline: refers to the code of a pipeline, and to the object returned by ```DryPipe.create_pipeline(my_dag_gen)``` 
+ Pipeline Instance: the execution of a pipeline over a dataset. A pipeline run over two datasets have two instances
+ Bash call: a bash snippet in the calls(...) clause of a task
+ PythonCall: a python function annotated with DryPipe.python_call() in the calls(...) clause of a task