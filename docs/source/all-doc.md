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

## Github repo

Issues can be submited on the [github repo](https://github.com/calculs-ca/DryPipe)

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

## Dynamic DAGs

Most interesting pipelines will have a DAG that changes depending on the instances datasets.

In DryPipe, the logic that drives te DAG is encoded in the generator function.

The following examples show various scenarios of how DAGs can change depending on the dataset of a pipeline instance. 

### Semi hardcoded DAGs

When a pipeline is meant to run a very few data inputs, it can make sense to hard code every instance.

The code below uses created two pipelines (pipeline_1_2_3 and pipeline_7_8_9_543), that work over two hardcoded datasets 
(a dataset that consists of a sequence of numbers)

Both pipelines can then be called with:

```shell
drypipe run -p my_modue:pipeline_1_2_3
drypipe run -p my_modue:pipeline_7_8_9_543
```

```python

def create_dag_generator(beautiful_numbers):
    def dag_gen(dsl):
        for i in beautiful_numbers:
            yield dsl.task(
                key=f"square-{i}"
            ).consumes(
                x=dsl.val(i)
            ).produces(
                f=dsl.file("file_with_squared_number.txt")
            ).calls("""
                #!/usr/bin/env bash
                echo $(( $x * $x)) > $f
             """)()
     
    return dag_gen

def pipeline_1_2_3():
    return DryPipe.create_pipeline(
        create_dag_generator([1,3,4]),        
    )  

def pipeline_7_8_9_543():
    return DryPipe.create_pipeline(
        create_dag_generator([7, 8, 9, 543]),        
    )

```

### Input file driven DAGs

Some Pipelines DAGs are driven by files from the pipeline dataset.

The DAG generator function being a standard python, it's up to the pipeline developer to decide on the file format, 
and how data from the file is parsed, in order to generate Tasks.

In the following example, the generator function reads a TSV file, and yields a task for every line. 

The path of the file is taken from an environment variable set before running the pipeline.

TSV input file example

```shell
export MY_INPUT_DATA_FILE_TSV=<a tsv file>
drypipe run -p my_modue:dag_gen
```


```python
def dag_gen(dsl):    
    with open(os.environ['MY_INPUT_DATA_FILE_TSV']) as f:
        for line in f.readlines():
            i, other_arg = line.split("\t")
            yield dsl.task(
                key=f"t-{i}"
            ).consumes(
                x=dsl.val(other_arg)
            ).produces(
                f=dsl.file("result.txt")
            ).calls("""
               #!/usr/bin/env bash
               echo "compute data for $x" > $f
            """)()
```

In a variation on this pattern, the TSV file lives the $__pipeline_instance_dir 

```python
def dag_gen(dsl):     
    with open(dsl.file_in_pipeline_instance_dir("dataset.tsv")) as f:
        for line in f.readlines():
            i, other_arg = line.split("\t")
            yield dsl.task(
                key=f"t-{i}"
            )...

def my_pipeline():
    return DryPipe.create_pipeline(my_pipeline)
```

With this approach, the $__pipeline_instance_dir is seeded with the input tsv (ex: dataset.tsv) file, before running the instance: 

```shell
cp dataset.tsv ./pipeline-instance-dir-123
drypipe run -p my_modue:my_pipeline --instance-dir=./pipeline-instance-dir-123
```

### DAG driven by task execution

The following example introduces three new DSL elements:

+ ```dsl.fileset('work-chunk.*.fasta')``` in a produces(...) clause
+ ```dsl.wait_for_tasks(task1, task2, ...)```
+ ```dsl.wait_for_matching_tasks(task1, task2, ...)```

The DAG is a bit more complicated, so we'll illustrate it with a diagram:  

```{mermaid}
flowchart LR
    prepare_chunks(["prepare-chunks\n[input_fasta=chimp.fasta]"])
    w1(["task-for-chunk-1"])        
    w2(["task-for-chunk-2"])
    wN(["task-for-chunk-N"])
    analyze(["analyze-all"])
    prepare_chunks-->|"work-chunk.1.fasta"|w1
    prepare_chunks-->|"work-chunk.2.fasta"|w2
    prepare_chunks-->|"work-chunk.N.fasta"|wN
    w1-->|"results.json"|analyze    
    w2-->|"results.json"|analyze
    wN-->|"results.json"|analyze   
```


```python

@DryPipe.python_call()
def create_n_chunks_of_work(input_fasta, __task_output_dir):
    with open(input_fasta) as f:
        c = 0
        for w in get_next_chunk_from(f.readlines()):
            with open(os.path.join(__task_output_dir, f"work-chunk-{c}.fasta")) as chunk_file:
                write_chunk_into(w, chunk_file)
            c += 1


def dag_gen(dsl):
    prepare_chunks = dsl.task(
        key=f"prepare-chunks"
    ).consumes(
        input_fasta=dsl.file('chimp.fasta')
    ).produces(
        work_chunks=dsl.fileset('work-chunk.*.fasta')
    ).calls(
        create_n_chunks_of_work
    )()

    yield prepare_chunks
         
    for _ in dsl.wait_for_tasks(prepare_chunks):
     
        # dsl.wait_for_tasks ensures that we can only get here when prepare_chunks
        # has successfully completed
     
        for work_chunk_file_handle in prepare_chunks.out.work_chunks.fetch():
            # extract number from file name, i.e. 
            # work-chunk.3.fasta  -> 3
            chunk_number = work_chunk_file_handle.basename().split(".")[1]
            yield dsl.task(
                key=f"task-for-chunk-{chunk_number}"
            ).consumes(
                f=work_chunk_file_handle
            ).produces(
                results_file=dsl.file("results.json")
            ).calls("""
                #!/usr/bin/env bash
                echo "work on $f" 
            """)()

        # wait_for_matching_tasks("task-for-chunk-*") ensures that we can only get here when AKK tasks
        # matching task-for-chunk-* have successfully completed
            
        for matcher in dsl.wait_for_matching_tasks("task-for-chunk-*"):
            yield dsl.task(
                key="analyze-all-work-pieces"
            ).consumes(
                # pattern_for_all_chunks is $__pipeline_instance_dir/output/task-for-chunk-*/results.json
                pattern_for_all_chunks=matcher.all.results_file.as_glob_expression()
            ).produces(
                a_result_file=dsl.var("final-result-file")
            ).calls("""
                #!/usr/bin/env bash
                
                # the two following commands are equivalent:                
                  
                ls $__pipeline_instance_dir/output/task-for-chunk-*/results.json                
                ls $pattern_for_all_chunks
                
                # the second is way more DRY (Don't Repeat Yourself) !
                
                echo "work on $f" 
            """).calls(
                analyze_all
            )()


@DryPipe.python_call()
def analyze_all(pattern_for_all_chunks, a_result_file):
    with open(a_result_file, "w") as _a_result_file:
     
        # Because we are in a Python Call, we can iterate over pattern_for_all_chunks().  
        # It is equivalent to calling : 
        # glob.glob(os.path.expandvars("$__pipeline_instance_dir/output/task-for-chunk-*/results.json"))
        # but way more DRY !
   
        for chunk_file in pattern_for_all_chunks():
            some_results = read_from(chunk_file)
            a_result_file.write(f"got {some_results} from chunk {chunk_file}")
```


### Waiting for upstream tasks

Previous example show how producer/consumer dependencies between tasks can be expressed by declaring a consumed variable or file, 
referring to the producing task's output, ex: 

```python
    dsl.task("key=t2").consumes(z=t1.out.abc)
``` 

Sometimes, the generator function needs to access the actual result produced in an upstream task 

The DryPipe DSL provides the following functions for this:

+ `dsl.wait_for_tasks(t_1, ..., t_n)` where ti is ta task or task key
+ `dsl.wait_for_matching_tasks(task_key_glob_pattern_1, ..., task_key_glob_pattern_n)` where  task_key_glob_pattern_i is a glob pattern matched against task keys

The following example shows how it works

```python

def my_dag_generator(dsl):
    
    task_a = dsl.task(
        key="task-a"
    ).produces(
        x=dsl.var(int) 
    ).calls(
        some_func
    )
        
    yield task_a
    
    for _ in dsl.wait_for_tasks(task_a, task_b):
     
        # because taskA has completed, we can fetch values from it's produces clause:
        
        actual_x_loaded_from_completed_task = task_a.out.x.fetch()
        
        assert isinstance(actual_x_loaded_from_completed_task, int)
     
        for matcher1, matcher2 in dsl.wait_for_matching_tasks("task-prefix-*", "other-task-prefix-*"):
            yield dsl.task(
                key="highly-dependent-task",
                task_conf=TaskConf(
                    executer_type="slurm",
                    slurm_account="me",
                    slurm_options=[
                        f"--time={estimate_time(actual_x_loaded_from_completed_task)}"
                    ]
                )
            ).consumes(
                x=dsl.val
            )...        
```

## Pipeline composition

In any programming languages, composition is a key for minimalism, expressiveness and reusability. It's also true for DSLs.

Composing DryPipe pipelines is greatly simplified by the fact that they are generator functions.

The next example shows how the DAG generator of two pipelines are combined to create a new DAG generator. 

Note1: The tasks from composed sub pipeline will live in the same $__pipeline_instance_dir 

Note2: `dsl.sub_pipeline(super_duper_pipeline_dag_generator, "s_")`

Tasks generated by super_duper_pipeline_dag_generator will have their keys prefixed by "super_duper", 
to ensure uniquenes of Task Keys of the new pipeline. 

dsl.sub_pipeline returns a sub pipeline that needs to be yielded.

Note3: `super_duper.wait_for_tasks("a-super-task")`

This will wait for the completion of the task with key="a-super-task" in the sub pipeline.

It's a shorthand for calling `dsl.wait_for_tasks("s_a-super-task")`, as the actual task key in the new composed pipeline will be "s_a-super-task"

The example waits for tasks in other_sub_pipeline with  `other_sub_pipeline.wait_for_matching_tasks("do-it-*")`
and for "task-dependent-on-a-super-task" and results are fed to task "grande-finale"

```python

import super_duper_pipeline_dag_generator
import other_pipeline_dag_gen

def my_coposite_dag_generator(dsl):
 
    super_duper = dsl.sub_pipeline(super_duper_pipeline_dag_generator, "s_")
    
    yield super_duper
    
    other_sub_pipeline = dsl.sub_pipeline(other_pipeline_dag_gen, "o_")
    
    yield other_sub_pipeline
    
    for super_task in super_duper.wait_for_tasks("a-super-task"):
        v = super_task.out.a_variable
        yield dsl.task(
            "task-dependent-on-a-super-task"
        ).consumes(
            y=dsl.val(v)
        ).produces(
            z=dsl.var(int)
        ).calls(
            a_func
        )

    for matcher in other_sub_pipeline.wait_for_matching_tasks("do-it-*"):
        for task_dependent_on_a_super_task in dsl.wait_for_tasks("task-dependent-on-a-super-task"):
            yield dsl.task(
                "grande-finale"
            ).consumes(
                z=task_dependent_on_a_super_task.out.z,
                a_pattern=matcher.all.a_file_defined_in_do_it_tasks.as_glob_expression()
            ).calls(
                grande_finale_func
            )
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

+ DAG: directed acyclic graph
+ Orchestrating Host: the computer where ```drypipe run``` or ```drypipe watch``` is executed
+ Pipeline: refers to the code of a pipeline, and to the object returned by ```DryPipe.create_pipeline(my_dag_gen)``` 
+ Pipeline Instance: the execution of a pipeline over a dataset. A pipeline run over two datasets have two instances
+ Bash call: a bash snippet in the calls(...) clause of a task
+ PythonCall: a python function annotated with DryPipe.python_call() in the calls(...) clause of a task