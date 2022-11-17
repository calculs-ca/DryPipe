# A Python DSL for pipelines 

Don't Repeat Yourself (D.R.Y) while writing pipelines, and stay away from leaky abstractions, use DryPipe !

## What is a pipeline ?

A pipeline could be described as _"a bunch of programs feeding data to one another"._

Programs within a pipeline tend to:

1. run for a long time
2. need large amounts of resources (cpu, memory, disk space, etc), requiring clusters to run (Slurm, Torque,etc)
3. be written in different languaged, have different CLI interfaces, file formats, etc.
4. long CODE->RUN->DEBUG->CODE cycles

The length of the debug cycle make pipelines difficult to debug.


[https://drypipe.readthedocs.io/](https://drypipe.readthedocs.io/)