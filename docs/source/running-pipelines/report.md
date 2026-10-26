# report (spec, not implemented)

`drypipe report` groups tasks by a signature computed by a `TaskClassifier`, a generalization of the
`LogClassifier` used by `analyze-logs` and `status-db`. Besides the logs, the classifier can look at
the task's inputs, outputs and runtime metrics.

## Motivation

On a large pipeline (hundreds to thousands of tasks, typically slurm arrays), the useful unit of work
is not a task but a group of tasks that share a problem, because each group gets one fix: more memory,
a longer time limit, a bug fix, a rerun. `analyze-logs` already does this for failures that leave a
trace in `out.log`, ex: 1174 failed tasks reduced to 2 signatures, both `OutOfMemoryError`, each
resubmitted with `--filter-from=failed.3-1174.md:N` and different `--sbatch-options`.

Some problems are not visible in `out.log`:

* a task that completed without producing a usable output (missing or empty file): the program
  exited 0, the log looks normal
* tasks that completed but used most of their memory or time limit, and will fail on larger inputs
* tasks whose duration or resource use depends on an input (ex: input file size), to choose
  `--sbatch-options` per group instead of sizing every task for the worst case

The output of `analyze-logs` is also an input: `--filter-from=<file>.md:N` selects the tasks of a
signature, for `array-submit`, `rewind`, `reset`, etc. This loop (classify, then act on a group) is what
a custom classifier extends. Ex, sizing the resources of a tool: a pipeline written for the purpose
generates tasks over a range of input sizes, a classifier groups them by input size and measured
consumption, and each group is relaunched with other `--sbatch-options`, or after a code change.

### Realistic value

* **Small pipelines** gain nothing: `status` and reading a log answer the same questions.
* **Output checks** are a workaround. A task that validates its outputs (ex: `test -s $out` at the end
  of the step) fails, and `analyze-logs` already reports it. `report` is useful for auditing an existing
  run without changing and rerunning its tasks, and for checks too costly or too specific to belong in
  the task.
* **Runtime metrics** are the most valuable part (memory is the most common failure in practice).
  Their blind spot: nothing is recorded when `TaskProcess` itself is killed (SIGKILL of the job, node
  failure), see [Runtime metrics](#runtime-metrics).
* **Overlap**: `status-db` with `log_signature` plus SQL already covers grouping by log signature,
  state and step, and a query can write a file of keys for `--filter-from`. `report` adds inputs,
  outputs and metrics, and shortens the loop: no query to write, the group number is enough.
* **Resource sizing** relies on elapsed time, CPU time and peak memory. Peak memory is a lower bound
  for programs that start several processes.
* **Generalization has its own costs**: options to learn, less flexibility, code paths that rot when
  nobody uses them.
* **Cost to the user**: a custom classifier is Python code written for one pipeline. Most users will
  only ever use the default classifier, which means the default must stay good on its own.

## TaskClassifier

`LogClassifier` is renamed `TaskClassifier`, its log logic (regex lists, masks) is unchanged and remains
the default signature.

```python
def signature(self, key, state, step, context):
    ...
```

Returns the signature, a string, or `None` to leave the task out of the report.

`context` is a `TaskClassifyContext`, nothing is read until used:

| member                            | value                                                         |
|-----------------------------------|---------------------------------------------------------------|
| `has_out_log()`                   | `False` when missing, or not read (`status-db --lean` on completed tasks) |
| `has_drypipe_log()`               | same, for `drypipe.log`                                       |
| `out_log_last_step_extract()`     | `out.log` from the last step banner, at most 1000 lines after it |
| `drypipe_log_last_step_extract()` | `drypipe.log` from the last step start, at most 100 lines after it |
| `full_out_log()`                  | all of `out.log`                                              |
| `full_drypipe_log()`              | all of `drypipe.log`                                          |
| `inputs`, `outputs`               | see below                                                     |
| `runtime_metrics`                 | see [Runtime metrics](#runtime-metrics)                       |

A missing log is `None`.

`inputs` and `outputs` are lazy: nothing is read until an attribute is accessed, then the task's
`task-conf.json` is loaded (once per task). An undeclared name raises an exception, as in the DSL.

| declared as            | `outputs.name` / `inputs.name`                    |
|------------------------|---------------------------------------------------|
| file                   | `Path`, absolute, possibly not existing            |
| file_set               | list of `Path` matching the pattern                |
| str, int, float (var)  | the value, `None` if not yet produced              |

Inputs produced upstream resolve to the upstream task's output, without requiring the upstream task
to be completed.

Example, the main use case:

```python
def my_classifier(c):

    class MyClassifier(type(c)):
        def signature(self, key, state, step, context):
            if state == "completed":
                if not context.outputs.peptides.exists():
                    return "FATAL_DID_NOT_PRODUCE_RESULTS"
                return None
            return super().signature(key, state, step, context)

    return MyClassifier().compile()
```

`status-db` stores the logs that the classifier extracts, a custom classifier can override:

| method                                        | default                                                        |
|-----------------------------------------------|----------------------------------------------------------------|
| `reads_logs(key, state, step, is_lean)`       | `False` for completed tasks under `--lean`, logs are then missing |
| `extract_out_log(context, is_lean)`           | `out_log_last_step_extract()` under `--lean`, else `full_out_log()` |
| `extract_drypipe_log(context, is_lean)`       | `drypipe_log_last_step_extract()` under `--lean`, else `full_drypipe_log()` |

## Runtime metrics

Recorded per step in `drypipe.log` by `TaskProcess`, considered available for this spec, see
`todo/task-runtime-metrics-recording.md` for how, and for the metrics left out.

| attribute                    | meaning                                                  |
|------------------------------|----------------------------------------------------------|
| `elapsed`, `elapsed_by_step` | wall time, seconds                                       |
| `cpu_time`                   | user + system CPU seconds                                |
| `max_rss`                    | peak memory of the largest process, bytes                |
| `cpus`                       | CPUs the task was given                                  |
| `exit_code`                  | return code or signal of the last step                   |
| `hostname`                   | node the task ran on                                     |

An unavailable metric is `None` (task never ran, `TaskProcess` killed by SIGKILL, run before metrics
were recorded). `max_rss` is a lower bound for programs that start several processes.

Signatures are strings, grouping by a numeric metric means bucketing it in the classifier, ex:
`f"max_rss {context.runtime_metrics.max_rss // 2**30 + 1}G"`.

## analyze-logs and report

The two commands remain separate. `analyze-logs` keeps its layout (one file per (state, step), signature
table, tails of `out.log`) and uses the `TaskClassifier` given by `--log-classifier`, like `status-db`.
The output of `report` is to be defined.

No tsv output in either: `status-db --tsv` already gives one row per task with its signature.

## Command

```
drypipe report --pipeline-instance-dir=... (--generator=... | --fs-generator) [--classifier=...] [filters]
               [--dir=...]
```

* `--classifier`: same format as `--log-classifier` (`module:function`, or a bare name in the
  `--generator` module), the function receives the default `TaskClassifier`.
* selection: all tasks matching the filters, the classifier drops the uninteresting ones by returning
  `None`.

## status-db

`status-db --log-classifier` uses the same `TaskClassifier`. The default classifier only reads `out.log`,
so it costs what it costs today: `task-conf.json` and metrics are only read by a custom classifier that
accesses `context.inputs`, `context.outputs` or `context.runtime_metrics`.

## Open questions

* Eight positional arguments, and every new one breaks existing classifiers. A single `task` argument
  with lazy attributes (`task.key`, `task.out_log`, `task.outputs`, ...) avoids that, and makes the
  logs lazy too.
