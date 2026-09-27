# Well behaved DAG generators

Well behaved DAG generators do **NOT** perform any work other than to generate tasks.

DAG generators get called repeatedly, doing "real work" (code meant to run only once that produce output results) would
be wasteful, and should be done by tasks.

Generated output should depend **only** on:

1. the pipeline's [input dataset](pipeline_input_dataset)
2. the data produced by tasks that have completed
3. the execution status of tasks in the DAG(*)

The DryPipe DSL is meant to make the above particularly (2) and (3) as easy as possible.

