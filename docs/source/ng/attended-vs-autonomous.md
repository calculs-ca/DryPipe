# Attended or autonomous ?

Before writing a pipeline, decide what it is for:

+ producing some data as quickly as possible, to test a hypothesis
+ producing data for a research project, once
+ running for years, on new data
+ being shared or shipped as a product: a black box that never fails

## From napkin script to black box

Pipelines sit on an axis:

    quick & sloppy                                                  robust & autonomous
    napkin script ──────────────────────────────────────────────────────── black box
                        ├──────────────── DryPipe ────────────────────────┤

DryPipe covers most of the axis, but not its quick & sloppy end:
for a few tasks that run once, a bash loop is faster to write than a DryPipe pipeline.

## Attended pipelines

Every pipeline starts attended: submit a subset of tasks, read the failures, fix, resubmit.
DryPipe's tools for this are selections, `array-submit`, `analyze-logs`, `rewind` and masks.

Some pipelines stay attended, and don't need to become autonomous:

+ huge pipelines: over thousands of tasks, there is always a new failure mode
+ pipelines that run once: making them robust costs more than attending them

## Autonomous pipelines

Some pipelines become robust after debugging and tweaking, they then run unattended with `run`.

It's the same code: a pipeline moves along the axis as it becomes worth it, it is not rewritten.

## Practical over pure

Data is the first goal, code is a means to produce it.

DryPipe can be used at a low level or at a high level, the choice is made task by task:

+ high level: inputs and outputs are declared, DryPipe knows the DAG
+ low level: a bash step reads another task's output dir, a mask breaks the DAG to divide the work:
  sloppy but practical, the data gets produced

DryPipe doesn't forbid the low level way, the cost is that DryPipe knows less,
and you are responsible for what it doesn't know.

The closer a pipeline is to the autonomous end, the more of it should be high level.
