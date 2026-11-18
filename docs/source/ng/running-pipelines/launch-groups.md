# Launch groups

The tasks of an array rarely all need the same resources: most run with 16G, a few need 64G and a longer time limit.
A launch group is a set of tasks submitted together, with the slurm options and env vars they need.

It is a key list file, as in [](selecting-tasks.md), with the options in its header:

    # --sbatch-options="--mem=64G --time=12:00:00"
    # --env="JVM_MEM=56g JVM_STACK=8m"
    sample-S17
    sample-S23
    sample-S41

It is submitted by selecting its keys:

    drypipe array-submit -k quant-array --where-key @high-mem.tsv

The header lines have the same effect as giving the options on the command line:

    drypipe array-submit -k quant-array --where-key @high-mem.tsv \
      --sbatch-options="--mem=64G --time=12:00:00" --env="JVM_MEM=56g JVM_STACK=8m"

The file keeps the options with the tasks they are meant for, so a launch can be repeated, or reviewed before it runs.

## --sbatch-options

Space separated slurm options, each one replaces the option of the same name in the task's `sbatch_options`,
the others are kept.

## --env

Space separated `NAME=VALUE` env vars, a value with spaces is quoted:

    # --env="JVM_MEM=56g JAVA_OPTS='-XX:+UseG1GC -XX:MaxGCPauseMillis=200'"

They have the last word: they override env vars of the same name set by the task,
in `extra_env`, its inputs, or `command_before_task`.
An `extra_env` var that refers to one sees its value, ex: with `extra_env={"JAVA_OPTS": "-Xmx${JVM_MEM}"}`
the task above gets `-Xmx56g`.

Values can't contain `,` or `|`.

## Rules

+ only `array-submit` reads the header lines
+ an option given in the header can't also be given on the command line
+ when `--where-key` has several key lists, the ones that give an option must give the same value
+ with `--loop`, the options are saved with the loop and used when it is resumed

## Tuning launch groups with an agent

Launch groups are plain text, an AI agent can write them. A typical workflow:

1. submit the tasks
2. ask an agent to read the summaries of `analyze-logs`, and propose launch groups, with the slurm options and env vars each one needs
3. review the files, submit them
4. repeat with the tasks that still fail
