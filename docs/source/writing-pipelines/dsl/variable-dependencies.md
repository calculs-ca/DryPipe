# Variable dependencies between Tasks

When a task only needs a variable (int, float, str) from an upstream task, it is more convenient to just pass the variable, 
as opposed to writing it in a file and then reading it by the consuming downstream task.  

Variable passing between tasks is expressed as follows with the DSL

```python
def my_pipeline_dag_generator(dsl):

  t1 = dsl.task(
        key="t1"
    ).inputs(
        x=123,
        y=3.14159265359,
        z='abc'
    ).outputs(
        result=int
    ).calls("""
        #!/usr/bin/env bash
        echo "all variables in the consumes(...) clause are in the env" $x, $y, $z
        # 'export' is used to assign output variable in the produces(...) clause 
        export result=$(( x * y ))
    """)()
  yield t1

  yield dsl.task(
        key="t2"
    ).inputs(
        r=t1.outputs.result
    ).outputs(
        f=dsl.file("final-result.txt")
    ).calls("""      
        #!/usr/bin/env bash
        echo "we got data from t1: $r" > $f 
    """)()
```

