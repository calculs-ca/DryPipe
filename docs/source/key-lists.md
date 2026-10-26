# Key lists

A key list is a text file of task keys. Any file in this format can be given to any command that takes a list
of keys, whatever wrote it: a DryPipe command, a script, or you.

## Format

- one task key per line
- tab separated columns are allowed, only the first one is read, so `list` output is a valid key list
- lines starting with `#` are comments
- blank lines are ignored
- keys can't contain spaces

## Commands that read key lists

| option                     | commands                    | effect                                                |
|----------------------------|-----------------------------|-------------------------------------------------------|
| `--where-key=@FILE`        | all commands that select tasks | selects the tasks of the file, `@F1,@F2` for a union, `@FILE[:100]` for a slice |
| `+ @FILE`, `- @FILE`       | rules of a `--task-set` file | adds or removes the tasks of the file from the task set |

`--where-key` also reads the markdown files written by `analyze-logs`, see `cli analyze-logs --help`,
and array submissions as `@array.3` or `@array.last`, see [Selecting tasks](ng/running-pipelines/selecting-tasks.md).

## Files that are key lists

| file                                         | written by                 | contains                                          |
|----------------------------------------------|----------------------------|---------------------------------------------------|
| output of `list-keys`                        | `cli list-keys > FILE`     | the selected tasks                                |
| output of `list`                             | `cli list > FILE`          | the selected tasks, with their state and step     |
| output of `extract-keys`                     | `cli extract-keys`         | the tasks of the chosen error signatures          |
| `.drypipe/<array-key>/task-keys.tsv`         | the array's creation       | all tasks of the array                            |
| `.drypipe/<array-key>/array.<n>.tsv`         | `array-submit`             | the tasks of the n-th array submitted             |
| `.drypipe/<array-key>/loop.<n>.<state>.tsv`  | `array-submit --loop`      | the tasks of the n-th loop, see [Keeping the slurm queue full](array-launch-loop.md) |

Examples:

    # status of the tasks of the 3rd array submitted
    cli status --task-key=my-array --where-key=@array.3

    # submit again the tasks of a loop
    cli array-submit --task-key=my-array --where-key=@.drypipe/my-array/loop.2.complete.tsv
