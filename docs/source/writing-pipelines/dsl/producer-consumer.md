# Producer Consumer relationships (dependencies) between tasks

In this example, task t2 consumes a file produced by t1.

DryPipe will therefore launch task t2, when (and only when) task t1 has successfully completed. 

```python

def my_pipeline_dag_generator(dsl):

  t1 = dsl.task(
        key="t1"
    ).inputs(
        x=123
    ).outputs(
        result=dsl.file("f.txt")
    ).calls("""
        echo $(( x * x )) > $result
    """)()
  
  yield t1
  
  # we keep a reference to t1 so we can refer to t1.outputs.result
  
  yield dsl.task(
        key="t2"
    ).inputs(
        r=t1.outputs.result
    ).outputs(
        f=dsl.file("final-result.txt")
    ).calls("""
        result_from_t1="$(cat $r)"
        echo "we got data from t1: $result_from_t1" > $f 
    """)()
```

The calls(...) clause can also take the name of a bash script, ex: ```calls("my-script.sh")```, the path of the script
is resolved by ```$__pipeline_code_dir/my-script.sh``` (see [environment variables](section_environment_variables))

