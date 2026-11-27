# Running pipelines on multiple clusters

1. **large pipelines**: too big for one cluster, they run on several at once.
   The pipeline has a home, where results are gathered, and clusters are its spokes.
2. **nomadic pipelines**: in times of compute allocation austerity, a pipeline often can't finish where it started,
   the allocation runs out, or a new one is granted on another cluster. It moves midway through its execution.

Both need the same tools, `rsync-push` and `rsync-pull`: a pipeline's execution is in the files of its instance dir,
the task outputs and states, copying them to another cluster moves the work there, and completed tasks don't run again.

A pipeline instance can be copied, in whole or in part, between any computers: a laptop, a lab server, clusters.
Ex: tasks run on one cluster, their results are pulled to a lab server, the remaining tasks are pushed to another cluster.

`rsync-push` copies files of a pipeline instance to another computer, `rsync-pull` copies them from it.
Both work the same way, they differ only by direction:

    drypipe rsync-push --to my-host:/a/b/my-pipeline/ --where-state completed --tags keepers
    drypipe rsync-pull --from my-host:/a/b/my-pipeline/ --where-state completed --tags keepers

The remote path is the pipeline instance dir on the other computer, files keep their path relative to it,
ex: `output/<task-key>/S1.fastp.html` is copied to `/a/b/my-pipeline/output/<task-key>/S1.fastp.html`.

`--to` can also be set with env var `DRYPIPE_RSYNC_PUSH_TO`, `--from` with `DRYPIPE_RSYNC_PULL_FROM`.

## Which files are copied

Files are selected in two stages:

+ the **tasks**: by the usual selection options, see [](running-pipelines/selecting-tasks.md)
+ the **files** of these tasks: by the options below

| Option | Values | What it selects | Default | Rules |
|---|---|---|---|---|
| `--outputs` | `declared`, `all`, `none` | Which files under `output/<task-key>/` are copied.<br>`declared`: only the files the task declares as outputs.<br>`all`: every file in `output/<task-key>/`, declared or not. Files at the destination that don't exist at the source are deleted.<br>`none`: no output files. | `declared` | `--tags` and `--exclude-tags` can only be used with `declared`. |
| `--tags` | Comma-separated tag names, ex: `--tags=keepers,for-debug` | Declared outputs that have **at least one** of the given tags. The summary shows a count for each requested tag, so a count of 0 points to a misspelled tag. | All declared outputs | Can't be combined with `--exclude-tags`, `--outputs all` or `--outputs none`. |
| `--exclude-tags` | Comma-separated tag names, ex: `--exclude-tags=intermediate` | Declared outputs that have **none** of the given tags. Untagged outputs are included. | No outputs excluded | Can't be combined with `--tags`, `--outputs all` or `--outputs none`. |
| `--drypipe-files` | `none`, `state`, `logs`, `all` | Which drypipe working files under `.drypipe/<task-key>/` are copied. Each level includes the one before it.<br>`none`: nothing.<br>`state`: the task state file, so the destination shows the same task states as the source. An old state file at the destination is replaced.<br>`logs`: the state file, plus `out.log` (task output), `drypipe.log` and `drypipe-remote.log`.<br>`all`: every file in `.drypipe/<task-key>/`. | `none` | `--outputs none --drypipe-files none` selects nothing and is an error. |

Tags are given to output files in the task definition, see [](writing-pipelines/tasks.md).

Ex: all files of the completed tasks, to continue the pipeline on another cluster:

    drypipe rsync-push --to cluster-b:/scratch/my-pipeline/ --where-state completed --outputs all --drypipe-files all

Ex: the reports of the completed tasks, and nothing else:

    drypipe rsync-pull --from cluster-a:/scratch/my-pipeline/ --where-state completed --tags reports

Ex: the logs of the failed tasks, to read them locally:

    drypipe rsync-pull --from cluster-a:/scratch/my-pipeline/ --where-state unhealthy --outputs none --drypipe-files logs

```{admonition} States without outputs
With `--outputs none`, the destination gets task states, but not the outputs of these tasks.
It is meant for following a pipeline from another computer, or for reading logs.
Don't continue running the pipeline at such a destination: tasks shown as completed have no outputs there.
```

## Before copying: the summary

Before copying anything, the command prints what it selected, and asks for confirmation:

    tasks: 1203
    output files to copy: 1203
      for-debug: 0
      keepers: 1203
    output files not copied: 2406
      (untagged): 1203
      heavy: 1203
    (a file with many tags is counted once per tag)
    drypipe files: logs
    to: cluster-b:/scratch/my-pipeline/
    continue ? [y/N]

Here `for-debug: 0` reveals a misspelled tag, or tasks that don't produce it.

`--no-confirm` skips the confirmation, ex: in a script.
`--dry-run` shows what rsync would copy, without copying anything.
`--info-progress2` shows the overall progress during the copy.

## Copying again is cheap

Only files that changed since the last copy are transferred, so a command can be rerun as tasks complete,
ex: pull the states and logs of a running pipeline every 10 minutes, to follow it from a lab server.

Copying from a pipeline while tasks are running is safe: a state file can change during the copy,
the next copy picks up the new one.

## Requirements

rsync 3.2.7 or later, on the computer running the command.
