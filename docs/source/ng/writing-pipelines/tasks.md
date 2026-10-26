# Defining tasks

A task is declared by a chain of calls on `dsl`, ending with `()`, and yielded by the DAG generator:

```python
def generate_my_tasks(dsl):

    trim = dsl.task(
        key="trim-S1"
    ).inputs(
        r1=dsl.file("samples/S1_R1.fastq.gz"),
        r2=dsl.file("samples/S1_R2.fastq.gz")
    ).outputs(
        trimmed_r1=dsl.file("S1_R1.trimmed.fastq.gz"),
        trimmed_r2=dsl.file("S1_R2.trimmed.fastq.gz"),
        report=dsl.file("S1.fastp.html")
    ).calls(
        "trim.sh"
    )()

    yield trim
```

| clause       | declares                                         |
|--------------|--------------------------------------------------|
| `task(...)`  | the key of the task, and how it runs             |
| `.inputs()`  | what the task reads                              |
| `.outputs()` | what the task produces                           |
| `.calls()`   | the code the task runs, one call per step        |
| `()`         | ends the declaration, returns the task           |

A task that isn't yielded is not part of the pipeline.
Forgetting the final `()` is an error: a declaration must be called before it is yielded.

## dsl.task()

    dsl.task(key, task_conf=None, downstream_resets=())

### key

The unique identity of the task in the pipeline instance. It names the task's directories:
`output/<key>` and `.drypipe/<key>`.

+ must be unique within the pipeline instance
+ can't contain spaces, tabs, line breaks, quotes (`'`, `"`) or braces (`{`, `}`)

### task_conf

How and where the task runs: locally or on Slurm, in a container, with which python, etc.
Either a `TaskConf`, or a function that takes the key and returns a `TaskConf`.
When omitted, the task runs as a local process.

% EDITOR NOTE: TaskConf deserves its own reference page

### downstream_resets

Tasks (or task keys) whose directories are deleted when this task is restarted,
so they run again with its new outputs.

## .inputs()

Each input is a name, and a value. The name becomes an env var in bash calls, and an argument in python calls.

### Constants

    .inputs(sample="S1", min_quality=20, ratio=0.5)

`str`, `int` and `float` values.

### Files

    .inputs(samplesheet=dsl.file("samplesheet.tsv"))

`dsl.file(path)`: a path relative to the pipeline instance directory, or an absolute path.

### Outputs of other tasks

    .inputs(r1=trim.outputs.trimmed_r1)

Makes the task depend on `trim`: it runs after `trim` completes.
The input is the file (or value) produced by `trim`, here bound to the name `r1`.

Given as a positional argument, the input keeps its name in the upstream task:

    .inputs(trim.outputs.trimmed_r1)          # same as trimmed_r1=trim.outputs.trimmed_r1

### Dependency without data

    .inputs(trim)

A task given as a positional argument: the task runs after `trim` completes, but receives nothing from it.

A name given twice in the same `.inputs()` is an error.

## .outputs()

Each output is a name, and a type.

### Files

    .outputs(report=dsl.file("S1.fastp.html"))

A file the task writes in its output directory, `output/<key>`.
The env var (or python argument) `report` is the full path of the file.

`tags` label files for bulk operations, ex: `rsync-push --tags`, `archive`, `purge-outputs`:

    .outputs(report=dsl.file("S1.fastp.html", tags=["keepers"]))

### File sets

    .outputs(fastqc_reports=dsl.file_set("*_fastqc.zip"))

All files matching a glob, relative to the task's output directory, known only once the task completes.
`exclude_pattern` excludes files matching a glob:

    dsl.file_set("*.zip", exclude_pattern="*.tmp.zip")

Downstream tasks, and the DAG generator, can iterate over the files of a completed task's file set.

### Values

    .outputs(read_count=int, mean_quality=float, adapter=str)

A value the task produces, readable by downstream tasks as inputs.

+ a bash call produces it by exporting an env var of the same name: `export read_count=12345`
+ a python call produces it by returning a dict: `return {"read_count": 12345}`

## .calls()

The code of the task. Each `.calls()` adds a **step**, steps run in order, numbered from 0.
A failed task restarts at the step that failed, ex: a task in state `failed.2` restarts at step 2.

```python
.calls("trim.sh").calls(count_reads)
```

### A bash script

    .calls("trim.sh")

A file ending in `.sh`, relative to the pipeline code directory (`$__pipeline_code_dir`).
Inputs and outputs are env vars.

### An inline bash snippet

    .calls("""
        #!/usr/bin/env bash
        fastp -i $r1 -I $r2 -o $trimmed_r1 -O $trimmed_r2 --html $report
    """)

Must start with a shebang (`#!`).

### A python function

```python
@DryPipe.python_call()
def count_reads(trimmed_r1):
    with gzip.open(trimmed_r1, "rt") as f:
        return {"read_count": sum(1 for _ in f) // 4}
```

    .calls(count_reads)

The function must be decorated with `@DryPipe.python_call()`.

+ arguments are matched by name with the inputs, the outputs, and the env vars of the task
+ an argument with no match is an error
+ a `**kwargs` argument receives everything not matched by the other arguments
+ it returns a dict of output values, or `None`

### Options

    .calls("trim.sh", container="fastp.sif")
    .calls(count_reads, python_bin="/opt/venv/bin/python")
    .calls("quant.sh", sbatch_options=["--mem=64G"])

| option           | for the step                                                                  |
|------------------|-------------------------------------------------------------------------------|
| `container`      | runs it in an Apptainer container                                             |
| `python_bin`     | the python that runs a python call                                            |
| `sbatch_options` | slurm options, not allowed on step 0 (it uses the options of the `task_conf`) |

### Values between steps

Output values produced by a step are visible to the following steps, as env vars and python arguments.

## Env vars of a task

Besides its inputs and outputs, every call sees:

| env var                    | value                                                     |
|----------------------------|-----------------------------------------------------------|
| `__task_key`               | the key of the task                                       |
| `__task_output_dir`        | `output/<key>`                                            |
| `__task_control_dir`       | `.drypipe/<key>`                                          |
| `__scratch_dir`            | `$SLURM_TMPDIR` on slurm, else `output/<key>/scratch`     |
| `__pipeline_instance_dir`  | the pipeline instance directory                           |
| `__pipeline_code_dir`      | the directory of the pipeline code                        |

% EDITOR NOTE: should __control_dir, __pipeline_work_dir, __output_var_file, etc should be documented
