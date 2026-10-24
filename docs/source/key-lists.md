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
| `--filter-from=FILE`       | all commands with filters   | selects the tasks of the file, repeat it for a union  |
| `+ @FILE`, `- @FILE`       | rules of a `--task-set` file | adds or removes the tasks of the file from the task set |

`--filter-from` also reads the markdown files written by `analyze-logs`, see `cli analyze-logs --help`.

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
    cli status --filter-from=.drypipe/my-array/array.3.tsv

    # submit again the tasks of a loop
    cli array-submit --task-key=my-array --filter-from=.drypipe/my-array/loop.2.complete.tsv
