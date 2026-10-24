# Keeping the slurm queue full

Clusters limit how many jobs a user can have in the queue at once, often around 1000 (each element of an
array counts as one job). An array with more tasks than that can't be submitted in one go.

`--tasks-per-job` gets around the limit by running several tasks in each job, but each job's wall time is
then multiplied by the number of tasks it runs. Large tasks (lots of RAM, many CPUs, long wall times) wait
a long time in the queue as a result.

`array-submit --loop` submits the tasks in small arrays instead, each time the queue has room for them:

    cli array-submit --task-key=my-array --filter-not-completed --loop --max-queued=1000

## How it works

1. When the loop starts, it selects the tasks with the `--filter-*` options, in the order of the array's tasks,
   and saves that list. The list never changes after that, so a task that fails while the loop runs is not
   submitted again.
2. Every `--sleep-interval` minutes, it counts your jobs in the queue on the array's slurm account, all of them,
   not only this pipeline's, since the limit applies to all your jobs on the account. The array must have a
   slurm account.
3. When there are at least `--min-batch` free slots, it submits the next tasks of the list as one array,
   enough to fill the free slots. It also submits when fewer than `--min-batch` tasks are left.
4. When all tasks of the list have been submitted, the loop exits. It does not wait for the tasks to finish.

If squeue fails, or sbatch rejects an array, the loop tries again at the next check.

Each array the loop submits is a normal array submit, it shows up in `array-squeue` like any other.

## Options

| option                 | default                                      | description                                                                 |
|------------------------|----------------------------------------------|-----------------------------------------------------------------------------|
| `--loop`               |                                              | submit in small arrays, as the queue frees up                               |
| `--max-queued=N`       | required                                     | maximum number of your jobs in the queue on the account, ex: your `MaxSubmitJobs` limit |
| `--margin=N`           | 10                                           | free slots left unused, for jobs submitted by other means while the loop runs |
| `--min-batch=N`        | 10% of `--max-queued`                        | smallest array the loop submits                                             |
| `--sleep-interval=M`   | 5                                            | minutes between two checks of the queue                                     |
| `--abandon-loop`       |                                              | abandon an interrupted loop, see below                                      |

Your limit is shown by `sacctmgr show assoc user=$USER format=MaxSubmitJobs`, ask your cluster's support
if it shows none.

A small `--min-batch` keeps the queue full, but submits many small arrays. A larger one submits fewer, bigger
arrays.

All other `array-submit` options work as usual. With `--tasks-per-job`, each free slot receives that many
tasks.

The loop asks for confirmation once, when it starts, unless `--yes` is given. With `--dry-run`, it prints how
many tasks it would submit, and with which settings, then exits.

## Running the loop

The loop runs until all its tasks are submitted, which can take days. Run it in `tmux` or `screen`, so that it
survives the loss of your ssh session.

## Interrupted loops

If the loop is killed, or your session ends, run `array-submit --loop` again, without `--filter-*` options. It
resumes where it stopped, with the tasks it had not submitted yet.

To start a new loop on the same array instead, abandon the interrupted one first:

    cli array-submit --task-key=my-array --abandon-loop

While a loop is unfinished, `array-submit --loop` with `--filter-*` options refuses to start a new loop, and
`array-submit` without `--loop` warns that the loop will submit some tasks later.

## One loop per array

Only one loop can run on an array at a time. If a loop is already running, `array-submit --loop` exits and
prints how to stop it:

    loop 3 is being run by pid 41237 on login2 (user maxl), started 2026-10-24 09:12
    to stop it:  ssh login2 kill 41237

Prefer `kill` to `kill -9`, it lets the loop finish the array it is submitting before it exits.

On some cluster file systems, the check only works when both loops run on the same login node.

## Tasks launched twice

The loop does not check whether its tasks are already in the queue, from another `array-submit` or `sbatch`.
Before starting a loop, make sure that none of its tasks are queued or running.
