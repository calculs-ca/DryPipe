# The DAG

Pipelines can be represented by [Directed Acyclic Graphs (DAG)](https://en.wikipedia.org/wiki/Directed_acyclic_graph)

The following diagram shows a DAG representing a simple pipeline:

  
```{mermaid}
flowchart LR    
    t1(["t1"])        
    t2(["t2"])
    t3(["t3"])
    t1-->|"x: int"|t2   
    t2-->|"f.tsv"|t3
    t1-->|"z.json"|t3
```

+ each oval shape represents a Task
+ each arrow represents data (files or variable) produced by a task, and consumed by another task

The function below `my_dag_generator` the generates the DAG above.

```python
def my_dag_generator(dsl):

    t1 = dsl.task(key="t1")\
        .outputs(x=dsl.var(int), z=dsl.file("z.json"))\
        .calls("""
            #!/usr/bin/env bash
            export x=123
            echo '{"i": "abc", "a": [56,57]}' > $z            
        """)()
    yield t1
    
    t2 = dsl.task(key="t2")\
        .inputs(t1.outputs.x)\
        .outputs(f=dsl.file("f.tsv"))\
        .calls(f2)()
    yield t2
        
    yield dsl.task(key="t3")\
        .inputs(t1.outputs.z, t2.outputs.f)\
        .outputs(t=dsl.file("pipeline-result.tsv"))\
        .calls(f3)()
```
The `my_dag_generator` is a [generator function](https://docs.python.org/3.10/glossary.html#term-generator), 
that yields every tasks of the DAG

Static vs Dynamic pipelines

So far, all example pipelines have a _static_ DAG, i.e. the set of Tasks is the same throughout the execution of the pipeline.

The DAG of most interesting pipelines are _dynamic_ , new tasks are created during the pipeline's execution.


```{admonition} Important
<b>DAG generators are invoked repeatedly by DryPipe</b> during the course of a pipeline execution

They generate the set of tasks _at the time they are invoked_

For a static DAG, every invocation yields the same set of tasks, but for _dynamic_ DAGs, new tasks get 
generated as execution progresses. 
```

The next series of examples will show static DAG generators, then we will show [dynamic DAG generators](section_dynamic_dags).

