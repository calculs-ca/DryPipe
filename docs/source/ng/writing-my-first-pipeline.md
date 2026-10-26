


# Writing your first pipeline


## Develop the first task

DryPipe is a task orchestrator, tasks must be coded before they can be orchestrated!

It is sometimes easier to first develop a task without DryPipe at all.

### In DryPipe, env variables are the interface for tasks

A task that receives all its inputs as env variables is automatically "DryPipe compatible".
 
Example : `trim.sh` reads its inputs and outputs from env vars

```bash
#!/usr/bin/env bash

set -eu

fastp -i $r1 -I $r2 -o $trimmed_r1 -O $trimmed_r2 --html $report
```

It can be developed and tested without DryPipe, by setting the env vars by hand:

```bash
export r1=samples/S1_R1.fastq.gz
export r2=samples/S1_R2.fastq.gz
export trimmed_r1=test-out/S1_R1.trimmed.fastq.gz
export trimmed_r2=test-out/S1_R2.trimmed.fastq.gz
export report=test-out/S1.fastp.html

bash trim.sh
```

Because the script only relies on env vars, DryPipe can orchestrate it without changes: 

```python
def generate_my_tasks(dsl):

    with open("samplesheet.tsv") as f:
        for line in f:
            sample, r1, r2 = line.strip().split("\t")

            yield dsl.task(
                key=f"trim-{sample}"
            ).inputs(
                r1=r1,
                r2=r2
            ).outputs(
                trimmed_r1=dsl.file(f"{sample}_R1.trimmed.fastq.gz"),
                trimmed_r2=dsl.file(f"{sample}_R2.trimmed.fastq.gz"),
                report=dsl.file(f"{sample}.fastp.html")
            ).calls("trim.sh")()
```

DryPipe sets the env vars `r1`, `r2`, `trimmed_r1`, `trimmed_r2` and `report` before calling `trim.sh`,
the names of the env vars are the names of the arguments in `.inputs(...)` and `.outputs(...)`.

Why env vars: see {ref}`section_env_vars`.
