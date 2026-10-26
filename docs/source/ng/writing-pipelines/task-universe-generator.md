# The task universe generator

A pipeline is a python generator function, it takes a `dsl` argument, and yields the tasks of the pipeline:

```python
def generate_my_tasks(dsl):

    with open("samplesheet.tsv") as f:
        for line in f:
            sample, r1, r2 = line.strip().split("\t")
            yield dsl.task(key=f"trim-{sample}")...()
```

The tasks it yields, and the dependencies between them, are the **task universe** of the pipeline instance.

The generator is given to drypipe commands by `--task-universe module:function`,
or by the `DRYPIPE_TASK_UNIVERSE` environment variable:

    export DRYPIPE_TASK_UNIVERSE=my_pipeline:generate_my_tasks

### What it yields

| yield                           | see                                                  |
|---------------------------------|------------------------------------------------------|
| a task: `dsl.task(...)...()`    | [](tasks.md)                                         |
| a slurm array task              | {ref}`section_slurm_arrays`                          |

A task that isn't yielded is not part of the pipeline.

## How it is invoked

Drypipe invokes the generator repeatedly while the pipeline runs, a few times per minute,
and by every command that needs the task universe (`list`, `array-submit`, etc).

Each invocation yields the tasks it can define *at that moment*.
A task is identified by its key: yielding a task that was yielded by a previous invocation does not create it twice.

This has three consequences:

+ deterministic: given the same input dataset and the same completed tasks, the generator yields the same tasks
+ no real work: code meant to run once, or that produces results, belongs in a task, not in the generator
+ fast: a slow generator slows down every drypipe command

What the generator yields should depend only on:

+ the pipeline's input dataset, ex: a samplesheet
+ the outputs of completed tasks, see {ref}`section_when_all_completed`

## Conditional yields

The generator is plain python: an `if` decides whether a task is part of the universe.

```python
            if sample not in excluded_samples:
                yield dsl.task(key=f"trim-{sample}")...()
```

It is the low level way of choosing tasks. A task that is not yielded doesn't exist for drypipe,
to act on some tasks of the universe for one command, use a selection, see [](../running-pipelines/selecting-tasks.md).

(section_dynamic_pipelines)=
## Dynamic pipelines

Pipelines come in three levels of complexity:

| level | the tasks                                                              | the generator needs      |
|-------|------------------------------------------------------------------------|--------------------------|
| 1     | can all be launched up front                                           | nothing special          |
| 2     | are all known up front, some wait for others before being launched    | `.inputs()` of tasks     |
| 3     | can't all be known up front, some depend on the results of others      | `dsl.when_all_completed` |

Level 3 is the general form of a pipeline, levels 1 and 2 are special cases of it.

### Level 1: independent tasks

Every task is known before the pipeline starts, and none depends on another:

```python
def generate_my_tasks(dsl):

    with open("samplesheet.tsv") as f:
        for line in f:
            sample, r1, r2 = line.strip().split("\t")

            yield dsl.task(
                key=f"trim-{sample}"
            ).inputs(
                r1=dsl.file(r1),
                r2=dsl.file(r2)
            ).outputs(
                trimmed_r1=dsl.file(f"{sample}_R1.trimmed.fastq.gz"),
                trimmed_r2=dsl.file(f"{sample}_R2.trimmed.fastq.gz")
            ).calls("trim.sh")()
```

All tasks are launched at once.

### Level 2: tasks that wait for others

Every task is still known before the pipeline starts, but some consume the outputs of others:

```python
            trim = dsl.task(
                key=f"trim-{sample}"
            )...()

            yield trim

            yield dsl.task(
                key=f"quant-{sample}"
            ).inputs(
                r1=trim.outputs.trimmed_r1,
                r2=trim.outputs.trimmed_r2
            ).outputs(
                quant=dsl.file("quant.sf")
            ).calls("quant.sh")()
```

Both tasks are yielded up front, DryPipe launches `quant-S1` when `trim-S1` completes.
Nothing more is needed: the `.inputs()` of a task declare what it waits for.

### Level 3: tasks defined from the results of others

Some tasks can't be yielded before other tasks have completed, because their definition depends on results:
how many tasks there are, their inputs, or which kind of task to yield.

Ex: `demultiplex` splits a sequencing run into samples, and writes `samples.tsv`, one row per sample:

    sample   library   r1                  r2
    S1       rna       S1_R1.fastq.gz      S1_R2.fastq.gz
    S2       dna       S2_R1.fastq.gz      S2_R2.fastq.gz
    ...

The number of samples is unknown until `demultiplex` completes, and the `library` of a sample decides
which task analyzes it:

```python
def generate_my_tasks(dsl):

    demultiplex = dsl.task(
        key="demultiplex"
    ).inputs(
        run_dir=dsl.file("run-2026-10-01")
    ).outputs(
        samples=dsl.file("samples.tsv"),
        fastqs=dsl.file_set("*.fastq.gz")
    ).calls("demultiplex.sh")()

    yield demultiplex

    for _ in dsl.when_all_completed("demultiplex"):

        def task_for(sample, library, r1, r2):
            if library == "rna":
                return dsl.task(key=f"quant-{sample}")...()
            else:
                return dsl.task(key=f"call-variants-{sample}")...()

        with open(demultiplex.outputs.samples) as f:
            next(f)
            for line in f:
                yield task_for(*line.strip().split("\t"))

    for group in dsl.when_all_completed("quant-*"):
        yield dsl.task(
            key="merge-quants"
        ).inputs(
            *group.tasks,
            sample_count=len(group.tasks)
        ).calls("merge-quants.sh")()
```

| time | yielded by the generator [r=running], [c=completed]                                |
|------|------------------------------------------------------------------------------------|
| t0   | demultiplex[r]                                                                     |
| t1   | demultiplex[c], quant-S1[r], call-variants-S2[r], ..., quant-SN[r]                 |
| t2   | demultiplex[c], quant-S1[c], call-variants-S2[r], ..., quant-SN[c]                 |
| t3   | demultiplex[c], quant-S1[c], call-variants-S2[r], ..., quant-SN[c], merge-quants[r] |

(section_when_all_completed)=
### dsl.when_all_completed()

    for group in dsl.when_all_completed(glob):
        ...

The body of the `for` runs once, if every task whose key matches `glob` is completed, and not at all otherwise.

+ `group.tasks`: the matching tasks, sorted by key
+ a glob without wildcards matches a single task, ex: `when_all_completed("demultiplex")`
+ a glob that matches no task: the body doesn't run

#### Reading the results of completed tasks

Inside the body, the outputs of the matched tasks (and of their upstream tasks) hold their actual values:

| output                     | read it with                         |
|----------------------------|--------------------------------------|
| `dsl.file(...)`            | `open(t.outputs.samples)`            |
| `dsl.file_set(...)`        | `for f in t.outputs.fastqs: ...`     |
| `int`, `float`, `str`      | `int(t.outputs.read_count)`, etc.    |

Outside the body, the outputs of a task that isn't completed have no value: reading them is an error.

#### Why all, and not some

A task's definition can't change once it is created.
If `merge-quants` were yielded when only some `quant-*` tasks were completed, it would have been created with only some of its inputs.
`when_all_completed` guarantees that a task defined from a group is defined once, from the complete group.

#### Rules

+ the tasks matched by `glob` must all be yielded *before* `when_all_completed(glob)`, in the same invocation
  of the generator. A matching task yielded later is an error: the group would not be complete
+ the generator must yield the same tasks in the body each time it is invoked:
  outputs of completed tasks don't change, so code that only reads them complies

(section_slurm_arrays)=
### Slurm arrays

An array task takes its children as an iterable of tasks. DryPipe yields the children: the generator only yields the array task.

```python
def generate_my_tasks(dsl):

    def trim_tasks():
        with open("samplesheet.tsv") as f:
            for line in f:
                sample, r1, r2 = line.strip().split("\t")
                yield dsl.task(key=f"trim-{sample}")...()

    yield dsl.task(
        key="trim-array",
        task_conf=slurm_conf
    ).slurm_array(
        trim_tasks(),
        max_simultaneous_jobs=50
    )()
```

Since the array task receives all its children at once, it needs no `when_all_completed`.
An array of level 3 tasks goes in the body of a `when_all_completed`, like any level 3 task.

## The dsl object

| member                                  | returns                                                                    |
|-----------------------------------------|----------------------------------------------------------------------------|
| `dsl.task(key, ...)`                    | the start of a task declaration, see [](tasks.md)                          |
| `dsl.file(path)`                        | a file, as an input or an output of a task, see [](tasks.md)              |
| `dsl.file_set(glob)`                    | a set of output files, see [](tasks.md)                                    |
| `dsl.when_all_completed(glob)`          | a group of completed tasks, see {ref}`section_when_all_completed`         |
| `dsl.pipeline_instance_dir()`           | the directory of the pipeline instance                                     |
| `dsl.info(msg)`, `dsl.debug(msg)`       | writes to the instance log, `.drypipe/instance.log`                        |

% EDITOR NOTE: relative paths in the generator (ex: open("samplesheet.tsv")): relative to what ? cwd, or the instance dir ?
% EDITOR NOTE: pipeline_instance_args() (args.json) is only written by the service, document it or drop it
% EDITOR NOTE: sub_pipeline (composition), download_outputs (remote): document here, or on their own pages
