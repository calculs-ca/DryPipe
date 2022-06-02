import glob
import json
import pathlib
import subprocess
import sys
import time
import unittest
import os
import shutil
import textwrap
import signal

from dry_pipe.bash import BASH_TASK_FUNCS_AND_TRAPS, bash_shebang
from dry_pipe.task_state import TaskState


class BaseFuncTests(unittest.TestCase):

    def _task_mockup(self):

        class TaskMockup:
            def launch(self, wait=False, fail_silently=False):
                return None

        return TaskMockup()

    def _test_script_dir(self):
        return os.path.join(os.path.dirname(__file__), "test-scripts")

    setup_done = False

    def setUp(self):

        history = os.path.join(self._test_script_dir(), "history.tsv")

        if os.path.exists(history):
            os.remove(history)

        if BaseFuncTests.setup_done:
            return

        BaseFuncTests.setup_done = True
        shutil.rmtree(self._test_script_dir(), ignore_errors=True)
        pathlib.Path(self._test_script_dir()).mkdir(parents=True)

    def _prepare_script(self, test_name, script_text, environment, add_echo_traces=False):

        script_file = os.path.join(
            self._test_script_dir(),
            f"{test_name}.sh"
        )

        env = {
            ** {"__control_dir": self._test_script_dir()},
            ** environment
        }

        with open(script_file, "w") as f:
            f.write(f"{bash_shebang()}\n\n")

            for k, v in env.items():
                f.write(f"export {k}={v}\n")

            if add_echo_traces:
                c = 0
                for line in BASH_TASK_FUNCS_AND_TRAPS.split("\n"):
                    if line.strip() != "" and not line.strip().startswith("#"):
                        f.write(f"echo '--->{c}' >&2\n")
                    f.write(f"{line}\n")
                    c += 1
            else:
                f.write(BASH_TASK_FUNCS_AND_TRAPS)

            f.write("\n\n# ================== TESTS BEGIN ==================\n\n")
            f.write(textwrap.dedent(script_text))

        os.chmod(script_file, 0o764)

        return script_file

    def _launch_script_and_dump_env(self, test_name, script_text, environment={}, assert_func=None,
                                    add_echo_traces=False, dont_wait=False, capture_env=False):

        script_file = self._prepare_script(test_name, script_text, environment, add_echo_traces)

        if capture_env:
            current_python_executable = os.path.abspath(sys.executable)
            dump_with_python_script = \
                f'{current_python_executable} -c "import os, json; print(json.dumps(dict(os.environ)))"'
            cmd = ['/bin/bash', '-c', f". {script_file} && {dump_with_python_script}"]
        else:
            cmd = [script_file]

        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        ) as p:

            if dont_wait:
                return p

            p.wait()
            err = p.stderr.read()
            out = p.stdout.read()

            if p.returncode != 0:
                if assert_func is not None:
                    assert_func(out, err, p.returncode, None)
                return None
            else:
                out_env = json.loads(out) if capture_env else {}
                if assert_func is not None:
                    assert_func(out, err, p.returncode, out_env)

                return out_env


    def _write_state_file(self, file_name):

        state_file_abs_path = os.path.abspath(os.path.join(self._test_script_dir(), file_name))

        with open(state_file_abs_path, "w") as f:
            f.write(" ")

        return state_file_abs_path

    def _delete_state_files(self):
        for f in glob.glob(os.path.join(self._test_script_dir(), "state.*")):
            os.remove(f)

    def test_ensure_detection_of_duplicate_state_file(self):

        self._delete_state_files()
        self._write_state_file("state.zaz1")
        self._write_state_file("state.zaz2")

        def assertions(out, err, return_code, out_env):
            if not "CORRUPTED_CONTROL_DIR" in err:
                raise Exception(f"expected CORRUPTED_CONTROL_DIR in stderr, got:\n{err}")

        self._launch_script_and_dump_env(
                "test_ensure_detection_of_duplicate_state_file",
                "__read_task_state",
                assert_func=assertions
        )

    def test_ensure_detection_of_missing_state_file(self):

        self._delete_state_files()

        def assertions(out, err, return_code, out_env):
            if not "CORRUPTED_CONTROL_DIR" in err:
                raise Exception(f"expected CORRUPTED_CONTROL_DIR in stderr, got:\n{err}")

        self._launch_script_and_dump_env(
                "test_ensure_detection_of_missing_state_file",
                "__read_task_state",
                assert_func=assertions
        )

    def _ensure_proper_var_values_in_dict(self, out_env, expected_vars):

        for k, v in expected_vars.items():
            if k not in out_env:
                self.fail(f"var {k} was not defined in script")
            else:
                v_in_env = out_env[k]
                if v_in_env != v:
                    self.fail(f"expected var {k} to be {v}, but got '{v_in_env}'. " +
                              f"out_env:\n{json.dumps(out_env, indent=4)}")

    def test_parsing_of_correct_state_file_1(self):

        self._delete_state_files()

        state_file_abs_path = self._write_state_file("state.launched.3.9.25")

        def assertions(out, err, return_code, out_env):
            self._ensure_proper_var_values_in_dict(
                out_env,
                {
                    "__state_name": "launched",
                    "__next_step_number": "3",
                    "__state_file_abs_path": state_file_abs_path
                }
            )

        self._launch_script_and_dump_env(
                "test_parsing_of_correct_state_file_1",
                "__read_task_state",
                assert_func=assertions,
                capture_env=True
        )

    def load_task_state(self):

        state_file = [f for f in glob.glob(os.path.join(self._test_script_dir(), "state.*"))]

        if len(state_file) != 1:
            raise Exception(f"expected 1 state file, got {len(state_file)}")

        s = TaskState(os.path.abspath(state_file[0]))
        s.task_key = "fake_task"
        return s

    def test_clean_failure(self):

        self._delete_state_files()

        self._write_state_file("state.launched.3.3.1")

        def assertions(out, err, return_code, out_env):
            if return_code == 0:
                self.fail(f"expected non zero termination")
            else:

                task_state = self.load_task_state()

                if not task_state.is_failed():
                    self.fail(f"expected task state to be failed, got {task_state}")

        self._launch_script_and_dump_env(
            "test_clean_failure",
            """
                __read_task_state
                __transition_to_failed
            """,
            assert_func=assertions
        )

        self._launch_script_and_dump_env(
            "test_clean_failure",
            """
                __read_task_state
                $(( 1 / 0))
            """,
            assert_func=assertions
        )

    def test_clean_timeout(self):

        self._delete_state_files()

        self._write_state_file("state.scheduled.1.2.1")

        script_file = self._prepare_script(
            "test_clean_timeout",
            """
                __read_task_state
                __check_bash_version                
                trap  "__transition_to_timed_out"  USR1                
                trap '__transition_to_failed ${LINENO}' ERR                
                while true; do 
                    echo "z"; 
                    sleep 1; 
                done            
            """,
            {
                "__control_dir": self._test_script_dir()
            }
        )

        def launch_it():
            return subprocess.Popen(
                [script_file],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True
            )

        p = launch_it()

        try:
            p.communicate(timeout=1)
        except subprocess.TimeoutExpired as ex:
            pass

        os.kill(p.pid, signal.SIGUSR1)

        time.sleep(1)

        task_state = self.load_task_state()

        if not task_state.is_timed_out():
            self.fail(f"expected task state to be 'timed-out', got {task_state}")

        p.kill()

        p.wait()

    def test_state_history_tracking(self):

        self._delete_state_files()
        self._write_state_file("state.launched.1")

        self._launch_script_and_dump_env(
            "test_state_history_tracking1",
            """
            __read_task_state
            __transition_to_step_started            
            __transition_to_failed
            """
        )
        self.assertEqual("state.failed.1", str(self.load_task_state()))

        self.load_task_state().transition_to_queued()
        self.assertEqual("state.queued.1", str(self.load_task_state()))

        self.load_task_state().transition_to_launched(self._task_mockup())
        self.assertEqual("state.launched.1", str(self.load_task_state()))

        self._launch_script_and_dump_env(
            "test_state_history_tracking1",
            """
            __read_task_state
            __transition_to_step_started            
            __transition_to_timed_out
            """
        )
        self.assertEqual("state.timed-out.1", str(self.load_task_state()))

        self.load_task_state().transition_to_queued()
        self.assertEqual("state.queued.1", str(self.load_task_state()))

        self.load_task_state().transition_to_launched(self._task_mockup())

        input_hash = "712053c23ced29fec4621b6002f3d28fea4df7a8"

        self._launch_script_and_dump_env(
            "test_state_history_tracking1",
            """
            
            __read_task_state                        
            __transition_to_step_started;                                    
            __transition_to_step_completed;
            __transition_to_completed;            
            """,
            environment={
                "__input_hash": input_hash
            },
            capture_env=False
        )

        self.assertEqual(f"state.completed-unsigned", str(self.load_task_state()))

        invalid_transitions = list(self.load_task_state().validate_history())

        if len(invalid_transitions) > 0:

            hr = list(self.load_task_state().load_history_rows())

            raise Exception(f"history has invalid transitions: {','.join(invalid_transitions)}\n{hr}")


