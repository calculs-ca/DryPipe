# Definitions

+ DAG: directed acyclic graph
+ Orchestrating Host: the computer where ```drypipe run``` or ```drypipe watch``` is executed
+ Pipeline: refers to the code of a pipeline, and to the object returned by ```DryPipe.create_pipeline(my_dag_gen)``` 
+ Pipeline Instance: the execution of a pipeline over a dataset. A pipeline run over two datasets have two instances
+ Bash call: a bash snippet in the calls(...) clause of a task
+ PythonCall: a python function annotated with DryPipe.python_call() in the calls(...) clause of a task
(pipeline_input_dataset)=
+ Pipeline Input Dataset: pipeline instances execute over pre existing data, most often files (DryPipe assumes no particular format). We refer to it as the pipeline instance's input dataset.