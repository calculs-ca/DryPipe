# Calling Python functions from Tasks

The calls(...) clause of tasks, can take bash snippets (as in previous examples), or Python functions, provided that they
are annotated with ```@DryPipe.python_call()```

The following pipeline does the same things as the previous, but uses python functions instead of bash snippets.

(user_env_vars_python)=

Note1: functions have their var arguments correctly parsed, according to the types specified in the generator function. 

Note2: output vars from the produces(...) clause are also passed as arguments.

Note3: output variables (declared in the produces(...) clause) are returned by functions with a python ```dict()```

Note4: print("abc") will write to in $__pipeline_instance_dir/.drypipe/$__task_key/out.log (see [DryPipe assigned environment variables of tasks])

```python
import sys
from dry_pipe import DryPipe


@DryPipe.python_call()
def f1(x, y, z):
    print(f"all variables in the consumes(...) clause are in the env {x}, {y}, {z}")

    if isinstance(x, int):
        print("Wow, x has the correct type !")
    else:
        # will write to  $__pipeline_instance_dir/.drypipe/$__task_key/out.log
        print("Something is seriously wrong !")

    x_from_env = os.environ["x"]
    if isinstance(x_from_env, str):
        print("""one could grab vars from the environment, but
                 receiving pre parsed arguments is more convenient !""")

    print("")
    return {
        "result": x * y
    }


@DryPipe.python_call()
def f2(r, f):
    with open(f) as f_handle:
        f_handle.write(f"we got data from t1: {r}")


def my_pipeline_dag_generator(dsl):
    t1 = dsl.task(
        key="t1"
    ).inputs(
        x=123,
        y=3.14159265359,
        z='abc'
    ).outputs(
        result=int
    ).calls(
        f1
    )()

    yield t1

    yield dsl.task(
        key="t2"
    ).inputs(
        r=t1.outputs.result
    ).outputs(
        f=dsl.file("final-result.txt")
    ).calls(
        f2
    )()
```

