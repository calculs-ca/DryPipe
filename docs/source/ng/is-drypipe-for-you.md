# Is DryPipe for you ?

## You'll like DryPipe if

+ you want to fix things with `ls`, `cat` and `grep` when they go wrong
+ your tasks are scripts you also want to run by hand
+ you already write python
+ you run thousands of tasks, and failures are routine

(section_out_of_the_way)=
### It gets out of the way

Some tools help with one hand (do things for you), and burden with the other hand (impose rigid structures, hard requirements, complex config, syntax, etc)

+ pipelines break in ways no tool anticipated: corrupted inputs, cluster outages, a tool upgraded mid-run
+ a tool that hides its state (databases, hashed directories, caches) can only be fixed with its own commands,
  when they don't cover the case, you are stuck
+ a rigid structure makes the usual things easy and the unusual ones hard, pipelines spend most of their time on the unusual ones
+ a tool that gets out of the way lets you fix things with what you already know: `ls`, `cat`, `grep`, a bash loop
+ in DryPipe:
  + a task is a directory, named by its key
  + its state is a file: `state.failed.3`
  + its logs are text files: `out.log`
  + its outputs are whatever files the task produces, in `output/<key>`, usable by any program, with or without DryPipe
  + its "orchestration contract" with DryPipe (inputs and outputs) are defined in plain Python.

(section_env_vars)=
### Env vars are the interface of tasks

Env vars let you work on a task script with no constraint imposed by DryPipe.

In the server world, it is now common practice to configure a service through its environment
(see [The Twelve-Factor App](https://12factor.net/config)). Pipelines benefit from it even more:

+ every language reads env vars (bash, python, R, a compiled binary), no library or DryPipe import needed
+ schedulers and containers (Slurm, Docker, Apptainer) already pass the environment to the jobs they run
+ a task can be tested by setting a few env vars by hand, see {doc}`writing-my-first-pipeline`

Env vars carry paths and small values, the data itself stays in files.

### Plain python, no new language

% EDITOR NOTE: the DSL paradox, see the editor note in intro.md

### Failures are routine

% EDITOR NOTE: analyze-logs, selections, rerun one error signature

## DryPipe is probably not for you if

+ your pipeline can't fit one directory per task

% EDITOR NOTE: other real misfits

## Why yet another pipeline tool ?

% EDITOR NOTE: many tools, no convergence on what a good one is, what DryPipe does differently
