# Tasks

The DryPipe DSL is meant to express two things: 

1. Tasks and their attributes (in/out arguments, the code they execute, where and how they execute).
2. Producer/Consumer relationships between Tasks

Producer/consumer relationships, are often inherently related to their arguments, ex: 

```python
t1 = dsl.task(key="t1").outputs(x=int).calls(f1)()

t2 = dsl.task(key="t2").inputs(t1.outputs.x).outputs(f=dsl.file("f.tsv")).calls(f2)()
```

The above code expresses in a (mostly) _declarative style_ the following:

1. task t1 produces a variable x of type int
2. task t2 consumes the variable x produced by t1
3. tasks t1 and t2 call functions f1 and f2
4. task t2 produces a file: "f.tsv"

An executable pipeline can be created with t1 and t2 with the following code:

```python
from dry_pipe import DryPipe

def my_tasks(dsl):
 
    t1 = dsl.task(key="t1").outputs(x=int).calls(f1)()
    yield t1
   
    yield dsl.task(key="t2").inputs(t1.outputs.x).outputs(f=dsl.file("f.tsv")).calls(f2)()

@DryPipe.python_call()
def f1():
    return {"x": 123}

@DryPipe.python_call()
def f2(x, f):
    with open(f) as _f:
        _f.write(f"...{x}")
    
def my_pipeline():
    return DryPipe.create_pipeline(my_tasks)
```

Assuming the function my_pipeline lives in module my_module the pipeline can be executed with the DryPipe CLI:  

```shell
. /a-virtual-env-with-drypipe-installed/bin/activate
$ drypipe run -p my_module:my_pipeline --instance-directory=/x/y/z
```


## inputs

### remote input files

An input file that lives on another host can declare where to fetch it from:

```python
.inputs(
    spectra=dsl.file("PXD002516/mgf/131023_Sunil_PPT_Brain_A_05.mgf").rsync(
        "ip40:/home-vhost32/xroucou_group/no_backup/public_data/"
    )
)
```

The file path is relative to the rsync source, and the same relative path is used in the pipeline instance dir.
The above task reads its input from `<pipeline_instance_dir>/PXD002516/mgf/131023_Sunil_PPT_Brain_A_05.mgf`.

The `into` argument imports the file in another directory, relative to the pipeline instance dir unless absolute:

```python
dsl.file("PXD002516/mgf/131023_Sunil_PPT_Brain_A_05.mgf").rsync("ip40:/.../public_data/", into="raw")
```

is read by the task from `<pipeline_instance_dir>/raw/PXD002516/mgf/131023_Sunil_PPT_Brain_A_05.mgf`.

#### import-inputs

The remote input files of the tasks are imported with:

```shell
drypipe import-inputs --pipeline-instance-dir=/x/y/z -g my_module:my_tasks
```

One rsync is run per source and destination, the task filters (ex: `--filter`, `--filter-ready`) select
the tasks whose inputs get imported. `--into` changes the destination of the files that don't have an `into`.
With `--fs-generator` instead of `-g`, the inputs of the tasks already created (by `run` or `prepare`) are imported
without running the DAG generator.

## outputs

### tags


Tags can be assigned to produced output files, for bulk operations, ex: 


```
.outputs(
    peptideshaker_report=dsl.file(f"{experiment_name}_Extended_PSM_Annotation_Report.txt", tags=['keepers']),
    certificate_of_analysis=dsl.file(f'{experiment_name}_Certificate_of_Analysis.txt', tags=['keepers']),    
    exhaustive_report=dsl.file("report-all.tsv", tags=['heavy', 'for-debug']),
    filtered_peptideshaker_report=dsl.file("filtered-pepshake.tsv")
)

```


#### rsync

```

drypipe rsync-push --tags=keepers,for-debug --to=my-host:/a/b/my-pipeline/ --filter-completed --include-drypipe-files
