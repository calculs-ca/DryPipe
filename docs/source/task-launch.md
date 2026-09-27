# Task launch script and variables

Environment variables are the mechanism by which DryPipe orchestrator parametrizes the tasks in a pipeline instance.

Tasks run as separate processes, and user code (passed in calls(...) clause) transparently receive their inputs and outputs as env variables.

## Pipeline specific variables
(section_environment_variables)=

Pipeline specific variables are declared with the consumes(...) and produces(...) clause of a pipeline's tasks.

All these variables defined in these clauses end up as env variables in the task process.

[Bash Calls] simply refer to them as $my_var, while [python calls] have their vars injected in function args, ex: ```def f(my_var)``` ([see example](user_env_vars_python))


## Generated scripts

DryPipe generates the following scripts for every task in a pipeline instance:

1. $__pipeline_instance_dir/.drypipe/task: the script that runs the task
2. $__pipeline_instance_dir/.drypipe/task-env.sh: the script that loads the task env vars
3. $__pipeline_instance_dir/.drypipe/sbatch-launcher.sh: launches ./task as a slurm job 

Users normally don't have to deal with these scripts, but they can be useful for debugging.

A task can be manualy launched by executing the task script $__pipeline_instance_dir/.drypipe/task

Additionally, the task script has these other useful commands: 

+ ```task kill```: will kill the task 
+ ```task tail```: will do a multi tail -f of all three logs of the task (out.log, drypipe.log) 
+ ```task ps```: equivalent to calling "ps -p <pid of the task>" 


## DryPipe variables

The following table describing DryPipe assigned environment variables, pipeline coders will rarely need to access them.

They are documented here for the rare cases where they are needed and to help making the documentation more concise. 
Ex: we can refer to $__task_output_dir instead instead of "the task's output directory". 

| variable name            | description                                                 | equivalent                                                                                                                                               |
|--------------------------|-------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| $__pipeline_instance_dir | path of the pipeline instance directory                     |                                                                                                                                                          |
| $__task_key              | ```dsl.tasl(key="a-unique-str-123")```                      |                                                                                                                                                          |
| $__task_output_dir       | the task's dedicated output dir                             | $__pipeline_instance_dir/output/$__task_key                                                                                                              |
| $__out_log               | destination of the task's stdout                            | $__pipeline_instance_dir/.drypipe/$__task_key/out.log                                                                                                    |
| $__scratch_dir           | temp working directory                                      | $SLURM_TMPDIR if tasks running in Slurm, otherwise $__task_output_dir/scratch                                                                            |
| $__pipeline_code_dir     | path to the instance's code directory                       | defaults to the directory of the python file where the DAG generator is coded, can be overriden. Useful for refering to scripts in a task's bash snippet |
| $__containers_dir        | the directory where containers used by the pipeline reside  |                                                                                                                                                          |

Note: $__pipeline_code_dir, and $__containers_dir are defined at the pipeline level, and can be overriden (see ...) 

