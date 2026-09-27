# Directory structure

Every task in a pipeline instance has two dedicated directories:

1. an output directory: $__pipeline_instance_dir/output/$__task_key
2. a working directory: $__pipeline_instance_dir/.drypipe/$__task_key

Where $__task_key is the key assigned in the task declaration, ex: ```dsl.task(key="t123")``` 

To avoid name clashes, DryPipe enforces uniqueness of task keys. 

```{admonition} Warning
It's considered bad practice for a task to write in a directory other than it's dedicated $__task_output_dir (see [well behaved tasks]())
```

A two task pipeline with task keys "t1" and "t2", would have the directory structure below.

```
$__pipeline_instance_dir
│
└───.drypipe
│   │
│   └────t1
│      │   task
│      │   task-env.sh
│      │   task-conf.json
│      │   state.(completed|launched|step-started.0|failed.0|killed.0|...)
│      │   out.log
│      │   drypipe.log
│      └─t2
│      │   task
│      │   ...
... 
└───output
│   │
│   └───t1
│       │   results.txt
│   └───t2
│       │   f.tsv
```

