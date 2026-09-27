# Running Tasks in parrallel

Example of a two independent task pipeline, (independent because no data is passed between them). DryPipe will run them in parallel.

```python

def my_pipeline_dag_generator(dsl):

  yield dsl.task(
        key="t1"
    ).outputs(
        a_file=dsl.file("a-file.txt")
    ).calls("""
        #!/usr/bin/env bash
        echo 'hello' > $a_file
    """)()

  yield dsl.task(
        key="t2"
    ).outputs(
        other_file=dsl.file("another-file.txt")
    ).calls("""
        #!/usr/bin/env bash
        echo 'world' > $other_file
    """)()
```

