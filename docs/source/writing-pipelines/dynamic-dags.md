# Dynamic DAGs
(section_dynamic_dags)=

Most interesting pipelines will have DAGs that change depending on the input dataset.

This section shows how dynamic DAGs can be expressed with the DSL.

DAG generators generate the tasks of a pipeline *at the instant it is invoked by DryPipe*     

DryPipe will track and orchestrate tasks, by invoking the generator repeatedly (a few times per minute) during the course of the pipeline execution.

At every invocation, DryPipe will analyze the generated DAG, and figure out what needs to be done 
(which tasks have all their dependencies satisfied and can be launched).

Below is an example of a dynamic/growing DAG. For simplicity, only two task status are shown (r=running, c=completed), there are many more in reality.

| time | DAG      [r=running], [c=completed]                              | pipeline status |
|------|------------------------------------------------------------------|-----------------|
| t0   | task-a[r]                                                        | running         |
| t1   | task-a[c]                                                        | running         |
| t2   | task-a[c], task-b[r]                                             | running         |
| t3   | task-a[c], task-b[c], task-c_1[r], task-c_2[r], ..., task-c_n[r] | running         |
| t4   | task-a[c], task-b[c], task-c_1[r], task-c_2[c], ..., task-c_n[r] | running         |
| t5   | task-a[c], task-b[c], task-c_1[c], task-c_2[c], ..., task-c_n[c] | completed       |

Each row of the table shows all tasks in the DAGs, that the generator would return at various points in time t0, t1, ...,t5.


## Semi hardcoded DAGs

Some pipeline are only meant to run very few inputs, in these cases, it might make sense to hard code every instance.

The code below uses created two pipelines (pipeline_1_2_3 and pipeline_7_8_9_543), that work over two hardcoded 
datasets (lists of ints).

Both pipelines use the same generator function, and can then be run with:

```shell
drypipe run -p my_modue:pipeline_1_2_3
drypipe run -p my_modue:pipeline_7_8_9_543
```

```python

def create_dag_generator(beautiful_numbers):
    def dag_gen(dsl):
        for i in beautiful_numbers:
            yield dsl.task(
                key=f"square-{i}"
            ).inputs(
                x=i
            ).outputs(
                f=dsl.file("file_with_squared_number.txt")
            ).calls("""
                #!/usr/bin/env bash
                echo $(( $x * $x)) > $f
             """)()
     
    return dag_gen

def pipeline_1_2_3():
    return DryPipe.create_pipeline(
        create_dag_generator([1,3,4]),        
    )  

def pipeline_7_8_9_543():
    return DryPipe.create_pipeline(
        create_dag_generator([7, 8, 9, 543]),        
    )

```

## Input file driven DAGs

Some Pipelines DAGs are driven by files from the pipeline dataset.

Since the DAG generator is just a normal python function, there are no other constraint on customization other 
than what can be coded in python code.

It's entirely up to the pipeline developer to decide on the input file format, how many files, how to parse them, which
library to use, etc. 

Next example shows a common pattern:

The pipeline's input consists of a TSV file, in which each line represents a unit or work. 

The generator implements this, by reading the TSV file, and yielding a task for every line, with the proper arguments. 

Note: the path of the file is taken from an environment variable set before running the pipeline, next example will 
show another approach.

```shell
export MY_INPUT_DATA_FILE_TSV=<a tsv file>
drypipe run -p my_modue:dag_gen
```


```python
def dag_gen(dsl):    
    with open(os.environ['MY_INPUT_DATA_FILE_TSV']) as f:
        for line in f.readlines():
            i, other_arg = line.split("\t")
            yield dsl.task(
                key=f"t-{i}"
            ).inputs(
                x=other_arg
            ).outputs(
                f=dsl.file("result.txt")
            ).calls("""
               #!/usr/bin/env bash
               echo "compute data for $x" > $f
            """)()
```

In a variation on this pattern, the TSV file lives the $__pipeline_instance_dir 

```python
def dag_gen(dsl):     
    with open(dsl.file_in_pipeline_instance_dir("dataset.tsv")) as f:
        for line in f.readlines():
            i, other_arg = line.split("\t")
            yield dsl.task(
                key=f"t-{i}"
            )...

def my_pipeline():
    return DryPipe.create_pipeline(my_pipeline)
```

With this approach, the $__pipeline_instance_dir is seeded with the input tsv (ex: dataset.tsv) file, before running the instance: 

```shell
cp dataset.tsv ./pipeline-instance-dir-123
drypipe run -p my_modue:my_pipeline --instance-dir=./pipeline-instance-dir-123
```

Storing the pipeline's input data in seed $__pipeline_instance_dir has the advantage of having the instance dir, 
contain the entirety of the pipeline instances data.

## DAG driven by task execution

The following example introduces three new DSL elements:

+ ```dsl.fileset('work-chunk.*.fasta')``` in a produces(...) clause
+ ```dsl.wait_for_tasks(task1, task2, ...)```
+ ```dsl.wait_for_matching_tasks(task1, task2, ...)```

The DAG is a bit more complicated, so we'll use diagram to show the "big picture":  

```{mermaid}
flowchart LR
    prepare_chunks(["prepare-chunks\n[input_fasta=chimp.fasta]"])
    w1(["task-for-chunk-1"])        
    w2(["task-for-chunk-2"])
    wN(["task-for-chunk-N"])
    analyze(["analyze-all"])
    prepare_chunks-->|"work-chunk.1.fasta"|w1
    prepare_chunks-->|"work-chunk.2.fasta"|w2
    prepare_chunks-->|"work-chunk.N.fasta"|wN
    w1-->|"results.json"|analyze    
    w2-->|"results.json"|analyze
    wN-->|"results.json"|analyze   
```

This pipeline has an initial task (prepare_chunks), that creates N files, for which a task is created (task-for-chunk-i).

prepare_task and task-for-chunk-0..n, have a producer/consumer relationship.  

The DSL expresses this (in the code below), by declaring

+ ```dsl.fileset('work-chunk.*.fasta')``` 

in the produces(...) clause of the producing task (prepare_task)

All downstream consuming tasks are yielded in the body of the for expression: 

+ `for _ in dsl.wait_for_tasks(prepare_chunks):`

The last task, depends on the completion of all previous, it's in a producer/consumer relationship with all 
upstream task-for-chunk-* 

The relationship is expressed with:

+ ```for matcher in dsl.wait_for_matching_tasks("task-for-chunk-*"):```


The evolution of the pipeline's DAG over time could look as follows 

| time | DAG      [r=running], [c=completed]                                                          | pipeline status |
|------|----------------------------------------------------------------------------------------------|-----------------|
| t0   | prepare_chunks[r]                                                                            | running         |
| t1   | prepare_chunks[c]                                                                            | running         |
| t2   | prepare_chunks[c], task-for-chunk.1[r], task-for-chunk.2[r], task-for-chunk.N[r]             | running         |
| t3   | prepare_chunks[c], task-for-chunk.1[c], task-for-chunk.2[r], task-for-chunk.N[r]             | running         |
| t4   | prepare_chunks[c], task-for-chunk.1[c], task-for-chunk.2[c], task-for-chunk.N[c]             | running         |
| t5   | prepare_chunks[c], task-for-chunk.1[c], task-for-chunk.2[c], task-for-chunk.N[c], analyze[r] | running         |
| t6   | prepare_chunks[c], task-for-chunk.1[c], task-for-chunk.2[c], task-for-chunk.N[c], analyze[c] | completed       |

Each lines in the above table shows the return of the generator function at various times during the execution.

```python

@DryPipe.python_call()
def create_n_chunks_of_work(input_fasta, __task_output_dir):
    with open(input_fasta) as f:
        c = 0
        for w in get_next_chunk_from(f.readlines()):
            with open(os.path.join(__task_output_dir, f"work-chunk-{c}.fasta")) as chunk_file:
                write_chunk_into(w, chunk_file)
            c += 1


def dag_gen(dsl):
    prepare_chunks = dsl.task(
        key=f"prepare-chunks"
    ).inputs(
        input_fasta=dsl.file('chimp.fasta')
    ).outputs(
        work_chunks=dsl.fileset('work-chunk.*.fasta')
    ).calls(
        create_n_chunks_of_work
    )()

    yield prepare_chunks
         
    for _ in dsl.wait_for_tasks(prepare_chunks):
     
        # dsl.wait_for_tasks ensures that we can only get here when prepare_chunks
        # has successfully completed
     
        for work_chunk_file_handle in prepare_chunks.outputs.work_chunks.fetch():
            # extract number from file name, i.e. 
            # work-chunk.3.fasta  -> 3
            chunk_number = work_chunk_file_handle.basename().split(".")[1]
            yield dsl.task(
                key=f"task-for-chunk.{chunk_number}"
            ).inputs(
                f=work_chunk_file_handle
            ).outputs(
                results_file=dsl.file("results.json")
            ).calls("""
                #!/usr/bin/env bash
                echo "work on $f" 
            """)()

        # wait_for_matching_tasks("task-for-chunk.*") ensures that we can only get here when AKK tasks
        # matching task-for-chunk-* have successfully completed
            
        for matcher in dsl.wait_for_matching_tasks("task-for-chunk.*"):
            yield dsl.task(
                key="analyze-all-work-pieces"
            ).inputs(
                # pattern_for_all_chunks is $__pipeline_instance_dir/output/task-for-chunk-*/results.json
                pattern_for_all_chunks=matcher.all.results_file.as_glob_expression()
            ).outputs(
                a_result_file=dsl.file("final-result-file")
            ).calls("""
                #!/usr/bin/env bash
                
                # the two following commands are equivalent:                
                  
                ls $__pipeline_instance_dir/output/task-for-chunk.*/results.json                
                ls $pattern_for_all_chunks
                
                # the second is way more DRY (Don't Repeat Yourself) !
                
                echo "work on $f" 
            """).calls(
                analyze_all
            )()


@DryPipe.python_call()
def analyze_all(pattern_for_all_chunks, a_result_file):
    with open(a_result_file, "w") as _a_result_file:
     
        # Because we are in a Python Call, we can iterate over pattern_for_all_chunks().  
        # It is equivalent to calling : 
        # glob.glob(os.path.expandvars("$__pipeline_instance_dir/output/task-for-chunk.*/results.json"))
        # but way more DRY !
   
        for chunk_file in pattern_for_all_chunks():
            some_results = read_from(chunk_file)
            a_result_file.write(f"got {some_results} from chunk {chunk_file}")
```


## When NOT to use dsl.wait_for

In some cases, a producer/consumer relationship can be expressed by simply passing a produced file or variable in the 
consumes(...) clause of the downstream task, ex:

```python
dsl.task("key=t2").inputs(z=t1.outputs.abc)
```

In such cases, dsl.wait_for isn't necessary. DryPipe will _know_ that the consuming task needs to wait. 

The above example shows cases where dsl.wait_for is needed.

The next example shows another case where dsl.wait_for is useful, where the generator function needs to access 
the actual result (variable x=dsl.var(int)) produced by a task, in order to parametrize a downstream task.

The task "highly-dependent-task" needs task_a.outputs.x to estimate a proper slurm execution time. Other uses cases
can easily be imagined.

In the example, "highly-dependent-task" also needs to wait after tasks: dsl.wait_for_matching_tasks("task-prefix-*", "other-task-prefix-*")
just for the sake of showing how waiting after many kinds of tasks is expressed.

The main things to remember:

1. dsl.wait_for_tasks and dsl.wait_for_matching_tasks are used in conjunction with `for`
2. the `for` loop will return an empty iterator when tasks waited upon are NOT completed, and a sigle item otherwise

see also [dsl.wait_for_tasks](api_dsl_wait_for_tasks) and [dsl.wait_for_matching_tasks](api_dsl_wait_for_matching_tasks)

```python

def my_dag_generator(dsl):
    
    task_a = dsl.task(
        key="task-a"
    ).outputs(
        x=dsl.var(int) 
    ).calls(
        some_func
    )()
        
    yield task_a
    
    for _ in dsl.wait_for_tasks(task_a, task_b):
     
        # because taskA has completed, we can fetch values from it's produces clause:
        
        actual_x_loaded_from_completed_task = task_a.outputs.x.fetch()
        
        assert isinstance(actual_x_loaded_from_completed_task, int)
     
        for matcher1, matcher2 in dsl.wait_for_matching_tasks("task-prefix-*", "other-task-prefix-*"):
            yield dsl.task(
                key="highly-dependent-task",
                task_conf=TaskConf(
                    executer_type="slurm",
                    slurm_account="me",
                    slurm_options=[
                        f"--time={estimate_time(actual_x_loaded_from_completed_task)}"
                    ]
                )
            ).inputs(
                x=dsl.val
            )...        
```

