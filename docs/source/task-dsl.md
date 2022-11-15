# The Task definition DSL


```python

from dry_pipe import DryPipe, TaskConf

def pipeline_generator(dsl):    
    t1 = dsl.task(
            key="a-sring-unique-within-the-pipeline-instance",
            task_conf=TaskConf(
                executer_type="slurm",
                slurm_account="my-slurm-account",
                ssh_specs="max@cedar:~/.ssh/id_rsa",
                sbatch_options="--time=0:1:00",
                container="my-container.sif"                
            )
        ).consumes(
            input_a_string=dsl.val("a-string"),
            input_one_two_three=dsl.val(123),
            a_file=dsl.file("a-file.json") # path relative to $__pipeline_instance_dir 
        ).produces(
            a_file_output=dsl.file("an-output-file.tsv"),
            another_file_output=dsl.file("other-file.txt"),
            file_set=dsl.fileset("file-*.txt"),
            a_string_output=dsl.var(str),
            glorious_int_var=dsl.var(int),
            joyful_floating_number=dsl.var(float)
        ).calls("""
            #!/usr/bin/env bash
            # inline bash snippet 
            echo "input_a_string=$input_a_string"
            echo "input_one_two_three=$input_one_two_three"
            echo "a_file contains:"
            cat $a_file
            export a_string_output='xyz'        
            echo "serious data" > $a_file_output
        """).calls(
            a_python_function
        ).calls("a-script.sh")()
    
    yield t1
    
    yield dsl.task(
        key=f"the-task-{1234}" 
        # no task_conf specified, task will run as local process
    ).consumes(
        # note how variable names (f, j) are in a namespace local to the task     
        f=t1.out.a_file_output,
        j=t1.out.joyful_floating_number
    ).produces(
        grandiose_results=dsl.file("grandiose_results.txt")
    ).calls("""
        #!/usr/bin/env bash
        echo "we got joyful_floating_number=$j from an upstream task !"
        echo $j > $grandiose_results
    """)

@DryPipe.python_call()
def a_python_function( 
    input_a_string, a_file, a_file_output, joyful_floating_number,
    another_file_output
):
    with open(a_file) as _a_file:
        s = _a_file.read()
        print(f"read {s} from {a_file}")
        
    with open(another_file_output, "w") as _another_file_output:
        _another_file_output.write("something")
        
    # Python Task return variables in produces(...) with a dict
    return {
        "joyful_floating_number": 3.14159265359
    }
```
