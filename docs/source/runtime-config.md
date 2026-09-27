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
    ).calls(f1)()

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
    ).calls(f2)()

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
    ).calls(f3)()

    yield dsl.task(
        key="t1",
        task_conf=TaskConf(
            executer_type="process",
            ssh_specs="me@a-slurm-login-host:~/.ssh/id_rsa"
        )
    ).calls(f4)()

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


