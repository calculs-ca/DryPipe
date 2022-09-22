import os
import shutil
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path

from dry_pipe import bash_shebang, script_lib
from dry_pipe.script_lib import task_script_header, touch
from test_utils import TestSandboxDir


def _run_script(script):

    with subprocess.Popen(
            [script, "--is-silent"],
            stdout=sys.stdout, #subprocess.PIPE,
            stderr=sys.stderr #subprocess.PIPE
    ) as p:
        p.wait()

        out = "" #p.stdout.read().strip().decode("utf8")
        err = "" #p.stderr.read().strip().decode("utf8")

        return p.returncode, out, err


__non_ending_script = """
#!/usr/bin/env bash

for i in $(seq 1000000); do
    echo "--:>$i"
    sleep 2
done
"""

class ScriptLibTests(unittest.TestCase):

    def test_bash_run_no_fail(self):
        d = TestSandboxDir(self)
        d.init_pid_for_tests()

        __ending_script = textwrap.dedent("""
            #!/usr/bin/env bash
            echo "the end"
            echo "err123" >&2
        """)

        ending_script_path = os.path.join(d.sandbox_dir, 'ending-script.sh')

        with open(ending_script_path, 'w') as f:
            f.write(__ending_script)

        pseudo_task_key = "task0"
        control_dir = os.path.join(d.sandbox_dir, '.drypipe', pseudo_task_key)
        Path(control_dir).mkdir()
        task_env_file = os.path.join(control_dir, 'task-env.sh')
        with open(task_env_file, 'w') as f:
            f.write(bash_shebang())
            f.write(textwrap.dedent(f"""
                __script_location=$( cd "$( dirname "${'{BASH_SOURCE[0]}'}" )" >/dev/null 2>&1 && pwd )
                export __pipeline_instance_dir=$( dirname $( dirname $__script_location))
                export export __pipeline_code_dir=$__pipeline_instance_dir                
                # . $__pipeline_instance_dir/.drypipe/pipeline-env.sh
                export __control_dir=$__pipeline_instance_dir/.drypipe/{pseudo_task_key}
                export __out=$__control_dir/out.log
                export __err=$__control_dir/err.log
                export __work_dir=$__pipeline_instance_dir/publish/{pseudo_task_key}   
            """))

        os.chmod(task_env_file, 0o764)
        os.chmod(ending_script_path, 0o764)

        task_script = os.path.join(control_dir, 'task')

        with open(task_script, 'w') as f:
            f.write(task_script_header())
            f.write(textwrap.dedent(
                f"""                        
                script_lib.touch(os.path.join(env['__control_dir'], 'output_vars'))
                
                step_number, control_dir, state_file = script_lib.read_task_state()                
                        
                state_file, step_number = script_lib.transition_to_step_started(state_file, step_number)                                        
                script_lib.run_script(os.path.join(env['__pipeline_code_dir'], 'ending-script.sh'))                    
                state_file, step_number = script_lib.transition_to_step_completed(state_file, step_number)                                                        
                """
            ))
        os.chmod(task_script, 0o764)

        state_file = os.path.join(control_dir, 'state.queued.0')
        touch(state_file)

        return_code, out, err = _run_script(task_script)

        if return_code != 0:
            raise Exception(f"task failed: {err}\n stdout:\n{out}")

        with open(os.path.join(control_dir, "out.log")) as out:
            with open(os.path.join(control_dir, "err.log")) as err:
                self.assertEqual(out.read(), "the end\n")
                self.assertEqual(err.read(), "err123\n")

        step_number, control_dir, state_file = script_lib.read_task_state(control_dir)

        self.assertEqual(step_number, 1)
        self.assertEqual(os.path.basename(state_file), "state.step-completed.1")



