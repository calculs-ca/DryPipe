# DryPipe

## A Python DSL for bioinformatics pipelines


## Getting Started

#### 1 Install dry-pipe in your virtualenv

```shell
    pyton3.8 -m venv your_venv 
    source your_venv/bin/activate
    pip install dry-pipe
```

#### 2 Write your  pipeline

```python

    from dry_pipe import DryPipe
    
    def my_pipeline_task_generator(dsl):
        yield dsl.task(key="task1") 
            .consumes(x=dsl.val(123)) 
            .produces(result=dsl.file("f.txt"))
            .calls("""
                #!/usr/bin/env bash                
                echo $x > $result
            """)
        
    def my_pipeline():
        return DryPipe.create_pipeline(my_pipeline_task_generator)
```

#### 3 Run it
(assuming the above code is in module my_module.py)
```shell
    drypipe run --pipeline='my_module:my_pipeline'
```
