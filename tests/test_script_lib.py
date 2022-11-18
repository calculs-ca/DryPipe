import os
import signal
import textwrap
import time
import unittest
from pathlib import Path
from threading import Thread

from dry_pipe import bash_shebang, script_lib
from dry_pipe.script_lib import task_script_header, touch, PortablePopen
from test_utils import TestSandboxDir


def _run_script(script, send_signal=None, env=None):

    with PortablePopen([script, "--is-silent", "--wait"], env=env) as p:

        if send_signal is not None:
            send_signal(p)

        p.popen.wait()
        out = p.stdout_as_string()
        err = p.stderr_as_string()
        return p.popen.returncode, out, err



class ScriptLibTests(unittest.TestCase):

    def _out_err_content(self, control_dir):
        with open(os.path.join(control_dir, "out.log")) as out:
            with open(os.path.join(control_dir, "err.log")) as err:
                return out.read(), err.read()

    def _write_task_env_file(self, control_dir, pseudo_task_key):
        task_env_file = os.path.join(control_dir, 'task-env.sh')
        with open(task_env_file, 'w') as f:
            f.write(bash_shebang())
            f.write(textwrap.dedent(f"""
                __script_location=$( cd "$( dirname "${'{BASH_SOURCE[0]}'}" )" >/dev/null 2>&1 && pwd )
                export __pipeline_instance_dir=$( dirname $( dirname $__script_location))
                export export __pipeline_code_dir=$__pipeline_instance_dir                
                # . $__pipeline_instance_dir/.drypipe/pipeline-env.sh
                export __control_dir=$__pipeline_instance_dir/.drypipe/{pseudo_task_key}
                export __out_log=$__control_dir/out.log
                export __err_log=$__control_dir/err.log
                export __task_output_dir=$__pipeline_instance_dir/output/{pseudo_task_key}   
                export __file_list_to_sign=$__task_output_dir/a.txt,$__task_output_dir/b.txt
            """))
        os.chmod(task_env_file, 0o764)

    def _single_step_bash_template(self, d, script_code):
        d.init_pid_for_tests()

        script_path = os.path.join(d.sandbox_dir, 'script.sh')

        with open(script_path, 'w') as f:
            f.write(script_code)

        pseudo_task_key = "task0"
        control_dir = os.path.join(d.sandbox_dir, '.drypipe', pseudo_task_key)
        Path(control_dir).mkdir()
        work_dir = os.path.join(d.sandbox_dir, 'output', pseudo_task_key)
        Path(work_dir).mkdir()
        self._write_task_env_file(control_dir, pseudo_task_key)
        os.chmod(script_path, 0o764)

        task_script = os.path.join(control_dir, 'task')

        with open(task_script, 'w') as f:
            f.write(task_script_header())
            f.write(textwrap.dedent(
                f"""
                env = script_lib.source_task_env(os.path.join(__script_location, 'task-env.sh'))                                        
                script_lib.touch(os.path.join(env['__control_dir'], 'output_vars'))                                                
                step_number, control_dir, state_file, read_task_state = script_lib.read_task_state()                                
                        
                state_file, step_number = script_lib.transition_to_step_started(state_file, step_number)                                        
                script_lib.run_script(os.path.join(env['__pipeline_code_dir'], 'script.sh'))                    
                state_file, step_number = script_lib.transition_to_step_completed(state_file, step_number)
                script_lib.sign_files()                                                        
                """
            ))
        os.chmod(task_script, 0o764)

        state_file = os.path.join(control_dir, 'state.queued.0')
        touch(state_file)

        return control_dir, task_script

    def test_bash_fail(self):
        d = TestSandboxDir(self)
        script_code = textwrap.dedent("""
            #!/usr/bin/env bash            
            echo "the end"
            echo "err99999" >&2
            exit 1            
        """).strip()

        control_dir, task_script = self._single_step_bash_template(d, script_code)

        return_code = _run_script(task_script)

        self.assertNotEqual(return_code, 0)

        out, err = self._out_err_content(control_dir)

        self.assertEqual(out, "the end\n")
        self.assertEqual(err, "err99999\n")

        step_number, control_dir, state_file, state_name = script_lib.read_task_state(control_dir)

        self.assertEqual(step_number, 0)
        self.assertEqual(os.path.basename(state_file), "state.failed.0")

    def _test_bash_timeout(self):
        d = TestSandboxDir(self)
        script_code = textwrap.dedent("""
            #!/usr/bin/env bash            
            echo "the end"
            echo "err99999" >&2
            for i in $(seq 1000000); do
                echo "--:>$i"
                sleep 2
            done                        
        """).strip()

        control_dir, task_script = self._single_step_bash_template(d, script_code)

        def send_signal(p):
            def f():
                time.sleep(1)
                p.send_signal(signal.SIGUSR1)
                time.sleep(1)
                p.kill()
            t = Thread(target=f)
            t.start()

        return_code = _run_script(task_script, send_signal)

        self.assertNotEqual(return_code, 0)

        out, err = self._out_err_content(control_dir)

        self.assertEqual(err, "err99999\n")

        step_number, control_dir, state_file, state_name = script_lib.read_task_state(control_dir)

        self.assertEqual(step_number, 0)
        self.assertEqual(os.path.basename(state_file), "state.timed-out.0")
