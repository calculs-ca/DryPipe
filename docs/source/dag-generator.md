# The DAG generator function

Pipelines can be represented by 
<a href="https://en.wikipedia.org/wiki/Directed_acyclic_graph" target=_blank target="_blank">Directed Acyclic Graphs (DAG)</a>, 
in which, nodes represent program executions (tasks in DryPipe jargon), 
and arrows represent producer/consumer relationships between tasks, as well as the flow of data.

To code of a DryPipe pipelines, consists of a python [generator functions](https://docs.python.org/3.10/tutorial/classes.html#generators)
that generate all the Tasks of the Pipeline DAG.

Here's the DAG of an example pipeline:      

```{mermaid}
flowchart LR
    c1(["crunch.1"])
    c2(["crunch.2"])        
    d1(["crunch-again.1"])
    d2(["crunch-again.2"])
    cN(["crunch.N"])    
    dN(["crunch-again.N"])       
    analyze(["analyze-all"])
    c1-->|"some-file.fasta"|d1
    c2-->|"some-file.fasta"|d2
    cN-->|"some-file.fasta"|dN
    d1-->|"another-file.tsv"|analyze
    d2-->|"another-file.tsv"|analyze
    dN-->|"another-file.tsv"|analyze
```

This DAG is produced by the following generator function

```python
import os
from pathlib import Path
from dry_pipe import DryPipe

def dag_generator_for_trivial_pipeline(dsl):
    with open(os.environ['file-list.tsv']) as f:
        for row in f.readlines():
            fasta_file, some_arg, other_arg = row.split("\t")
            file_name = Path(fasta_file).stem

            crunch_task = dsl.task(
                key=f"crunch.{file_name}"
            ).consumes(
                some_arg=dsl.val(some_arg),
                the_input_file=dsl.file(fasta_file)
            ).produces(
                a_tsv=dsl.file(f"some-file.fasta")
            ).calls("""
                #!/usr/bin/env bash
                $__pipeline_code_dir/crunch-it.sh --in=$the_input_file --out=$a_tsv --a=$some_arg
            """)()
            yield crunch_task

            yield dsl.task(
                key=f"crunch-again.{file_name}"
            ).consumes(
                f=crunch_task.out.a_tsv,
                b=dsl.val(other_arg)
            ).produces(
                another_file=dsl.file(f"another-file.tsv")
            ).calls("""
                #!/usr/bin/env bash
                $__pipeline_code_dir/digest-it.sh "--$b" $f > $crunched_result
            """)()

    for task_matcher in dsl.wait_for_matching_tasks("crunch-again.*"):
        yield dsl.task(
            key=f"analyze-all"
        ).consumes(
            glob_expression_for_all_results=task_matcher.all.another_file.as_glob_expression()
        ).produces(
            end_result=dsl.file(f"end-result.tsv")
        ).calls("""
            #!/usr/bin/env bash
            echo "will digest the following files:"
            ls $glob_expression_for_all_results
            $__pipeline_code_dir/analyze.sh $glob_expression_for_all_results
            echo "done"            
        """)()

def my_trivial_pipeline():
    return DryPipe.create_pipeline(dag_generator_for_trivial_pipeline)
```

Assuming the above code in a python module 'my_module', and DryPipe is in the shell's virtual env, the pipeline would be run with: 

```shell
drypipe run -p my_module:my_trivial_pipeline --instance-dir=<a-directory>
```
