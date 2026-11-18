# Selecting tasks

Most drypipe commands act on a set of tasks: `list`, `status`, `reset`, `rewind`, `array-submit`, `rsync-push`, etc.
Which tasks they act on is decided in two stages:

+ the **task universe**: the tasks yielded by the generator, see [](../writing-pipelines/task-universe-generator.md)
+ the **selection**: which tasks of the universe this command acts on

## The selection

A selection narrows the universe. Its options all start with `--where`, like the WHERE clause of an SQL query,
which separates them from the options that say what a command does, ex: `--at-step` of `rewind` is the step to restart at:

    drypipe rewind --where-state failed --where-step 3 --at-step 2

| option                          | selects tasks                          |
|---------------------------------|----------------------------------------|
| `--where EXPR`                  | for which a python expression is true  |
| `--where-key GLOB,@LIST,...`    | by key                                 |
| `--where-state STATE,...`       | by state                               |
| `--where-step N,...`            | by the step they are at                |
| `--where-key-not`, `--where-state-not`, `--where-step-not` | all except those |
| `--where-func module:f(args)`   | for which a python function is true    |
| `--limit N`                     | at most N tasks                        |

With no selection option, all tasks of the universe are selected.

### --where

A python boolean expression, with variables `key`, `state` and `step`:

    --where "state == 'failed' and step >= 3"
    --where "key.endswith('_R2') or step == 0"
    --where "key.like('quant-*')"

`key.like()` uses shell wildcards: `*` matches any text, `?` matches one character
(not `%` and `_`, as in SQL's LIKE).

`@` followed by a key list is the set of keys of that list:

    --where "key in @failed.3-423.md:2,3"         tasks of signatures 2 and 3 of an analyze-logs report
    --where "key in @failed.3-423.md"             all tasks of the report
    --where "key in @to-retry.tsv"                a tsv file, the first column is the task key
    --where "key in @array.9"                     tasks of submission array.9 of the array task given by `-k`
    --where "key in @array.last"                  tasks of its latest submission

Array submissions are as listed by `array-summary`.
A relative path is relative to the current directory.

A key list can be sliced like a python list, in the order of its lines,
or for an analyze-logs report, in the order of its tasks:

    --where "key in @to-retry.tsv[:100]"          the first 100 keys
    --where "key in @to-retry.tsv[100:]"          all keys after the first 100
    --where "key in @to-retry.tsv[:-40]"          all keys except the last 40
    --where "key in @array.last[:10]"             the first 10 tasks of the latest submission
    --where "key in @failed.3-1174.md:1[:20]"     the first 20 tasks of signature 1
    --where "key in @failed.3-1174.md:1[20:]"     all remaining tasks of signature 1

The slice applies to the whole list: `@failed.3-1174.md:1,2[:50]` is the first 50 tasks of signatures 1 and 2 together.

Ex: test a fix on 5 tasks of a failure cause, before resubmitting all of them:

    drypipe array-submit -k msdmo-array --where-key '@failed.3-1174.md:1[:5]'

Steps are zero based. Comparing `state` with an unknown state is an error, ex: `state == 'faild'`.

`--where` can express any selection, the options below are shortcuts for the most common ones.

### Shortcuts: --where-key, --where-state, --where-step, and their -not

They are shorter, need less quoting, and catch typos before anything runs.
Each one is rewritten as a `--where`, and the {ref}`explanation <section_explain>` shows the rewritten form.

+ each `--where-X` adds one `and` condition
+ commas inside a value mean `or`
+ a `-not` option selects the tasks that match none of its values

| shortcut                              | rewritten as                                                  |
|---------------------------------------|---------------------------------------------------------------|
| `--where-state failed`                | `state == 'failed'`                                           |
| `--where-state failed,timed-out`      | `state in ('failed', 'timed-out')`                            |
| `--where-state-not completed,killed`  | `state not in ('completed', 'killed')`                        |
| `--where-step 3`                      | `step == 3`                                                   |
| `--where-step 2,3`                    | `step in (2, 3)`                                              |
| `--where-key 'quant-*'`               | `key.like('quant-*')`                                         |
| `--where-key-not 'quant-S7*'`         | `not key.like('quant-S7*')`                                   |
| `--where-key @to-retry.tsv,'trim-*'`  | `key in @to-retry.tsv or key.like('trim-*')`                  |

States are:

    waiting, ready, _step-started, step-started, step-completed, completed, failed, timed-out, killed, crashed

and state groups are expanded in place:

| group          | states                                     |
|----------------|--------------------------------------------|
| `unhealthy`    | failed, timed-out, killed, crashed         |
| `incomplete`   | all states except completed                |

An unknown state is an error.

Ex: rerun the tasks of signature 2 of an analyze-logs report, unless they completed since:

    --where-key @failed.3-423.md:2 --where-state-not completed

    key in @failed.3-423.md:2 and state != 'completed'

Ex: unhealthy tasks of the latest array submission, at step 2 or 3:

    --where-key @array.last --where-state unhealthy --where-step 2,3

    key in @array.last and state in ('failed', 'timed-out', 'killed', 'crashed') and step in (2, 3)

Ex: shortcuts and a `--where` together:

    --where-key 'trim-*' --where-state failed --where "step >= 2 or key.endswith('_R2')"

    key.like('trim-*') and state == 'failed' and (step >= 2 or key.endswith('_R2'))

### --where-func

A factory function, in `module:function` format, called with python literals as arguments.
It returns a filter function `(key, state, step) -> bool`:

    --where-func "a.b.c:make_filter(threshold=3)"

    def make_filter(threshold=0):
        def f(key, state, step):
            return step > threshold
        return f

A bare function name (no module) is looked up in the module of the `--task-universe` generator.
Like the other options, it adds one `and` condition.

### --limit

Selects at most N tasks, after all other options are applied.

When `--limit` leaves tasks out, drypipe offers to save their keys, in a file it proposes:

    --limit 100 left out 457 tasks, save their keys in (clear the name to cancel): remain-457_1.tsv

ENTER saves, the name can be edited before. The file starts with the command and its explanation,
as `#` comments, followed by the keys, so the left out tasks can be selected later:

    drypipe array-submit -k quant-array --where-key @remain-457_1.tsv


(section_explain)=
## Explaining a selection

Every command that selects tasks explains its selection: for each stage, its condition as a `--where`,
how many tasks remain, and how many it removed:

    drypipe list --where-key 't_a*' --where-state failed,timed-out --where "step >= 3" --limit 10

    generator                                                   1203
    mask           drypipe-task-universe.mask                   1163   (-40)
    --where-key    key.like('t_a*')                              310  (-853)
    --where-state  state in ('failed', 'timed-out')               22  (-288)
    --where        step >= 3                                      17    (-5)
    --limit        10                                             10    (-7)

A stage that removes all remaining tasks is where to look when a selection is empty.

With `--stop-after-step`, tasks already past the step are left out, before `--limit`:

    --stop-after-step  step <= 0                                   12    (-5)

The `mask` line is shown when a mask file exists, with `--no-mask` it reads:

    mask           disabled (--no-mask)                         1203     (0)

The explanation is printed on stderr, so piping the output of a command is not affected.

`--explain-off` turns the explanation off, `--quiet` turns it off along with all other informative output.

A selection can be tested with a read only command, such as `list` or `summary`,
before being given to a command that modifies tasks.

(section_masks)=
## Advanced: masks, for high volume pipelines

In a pipeline with thousands of tasks, it is sometimes useful to work only on a subset of tasks.

Although it can be done with `--where` options, it's sometimes more convenient to define inclusion and exclusion rules in a file, that is what a mask does.

A mask is a file named `drypipe-task-universe.mask` in the pipeline instance dir, removing it unmasks all tasks.

Option `--no-mask` disables the mask file if one is defined.

```{admonition} A mask can break the DAG
Tasks downstream of a masked task can lose their inputs, drypipe doesn't prevent it:
a task can depend on another without drypipe knowing, ex: its bash code reads another task's output dir.
Masking safely is up to the user. In a pipeline made of a single array, there is no DAG to break.
```

### Mask rules

One rule per line, `#` starts a comment:

    - <glob>      masks tasks whose key matches the glob
    + <glob>      unmasks them
    - @<file>     masks tasks whose key is in the file
    + @<file>     unmasks them

Rules are applied top to bottom: the last matching rule wins, keys matched by no rule are not masked.
Ex: mask the tasks in high-ram.tsv, except those in tests.tsv:

    - @high-ram.tsv
    + @tests.tsv

`<file>` is either:

+ a tsv file, the first column is the task key, other columns are ignored
+ a report written by `analyze-logs`, optionally followed by signature numbers: `failed.3-423.md:2,3`

A relative path is relative to the directory of the mask file.

### Mask rules are on keys only

A work phase is on a fixed set of tasks: a rule on state would change the set as tasks complete or fail.
To act on tasks by state, use a selection.
