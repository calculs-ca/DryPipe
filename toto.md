
+ Fix : log SLURM_JOB_ID, always 

+ rename slurm job for single sbatch launches

+ error message "key not found" when: 

   drypipe task -k non-existing-key --tail --regen

# Don't save for cli commands like list-keys

   [maxl@narval1 SC-g-a]$ drypipe list-keys --filter=*032618_G1vsG14NQO_B_6pct*
^CTraceback (most recent call last):
  File "<frozen runpy>", line 198, in _run_module_as_main
  File "<frozen runpy>", line 88, in _run_code
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/cli.py", line 1630, in <module>
    handle_script_lib_main()
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/cli.py", line 1607, in handle_script_lib_main
    cli.invoke()
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/cli.py", line 296, in invoke
    method()
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/cli.py", line 1427, in list_keys
    for key, _, _ in self.filter_key_state_step():
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/cli.py", line 1414, in filter_key_state_step
    for key, state, step in pipeline_instance.iterate_key_state_steps(key_universe):
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/pipeline_instance.py", line 290, in iterate_key_state_steps
    _, state_file = self.state_file_tracker.create_true_state_if_new_else_fetch_from_memory(task)
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/state_file_tracker.py", line 348, in create_true_state_if_new_else_fetch_from_memory
    state_file_in_memory = self.load_from_existing_file_on_disc_and_resave_if_required(task, state_file_path)
                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/state_file_tracker.py", line 232, in load_from_existing_file_on_disc_and_resave_if_required
    task.save(state_file, current_hash_code)
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/task.py", line 136, in save
    step_invocations = [
                       ^
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/task.py", line 137, in <listcomp>
    self.task_steps[step_number].get_invocation(control_dir, self, step_number)
  File "/scratch/maxl/openprot-v3/DryPipe/dry_pipe/task.py", line 202, in get_invocation
    with open(step_script, "w") as _step_script:
         ^^^^^^^^^^^^^^^^^^^^^^
  File "<frozen codecs>", line 186, in __init__
KeyboardInterrupt
