import unittest

import tempfile
from pathlib import Path

from dry_pipe.task_classifier import TaskClassifier, RuntimeMetrics


class TaskClassifierTests(unittest.TestCase):

    def setUp(self):
        self.classifier = TaskClassifier().compile()

    def signature(self, out_log, key="t1"):
        return self.classifier.signature(key, out_log, None, "failed", 1, None, None, None)

    def assertSameSignature(self, log1, log2):
        self.assertEqual(self.signature(log1, "t1"), self.signature(log2, "t2"))

    def test_python_traceback(self):
        log = """
Traceback (most recent call last):
  File "/home/u/p/split.py", line 330, in split_results
    df = pd.read_csv(rescored_peptides, sep='\\t')
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: '/scratch/out/t1/ms2out.psms.tsv'
"""
        self.assertEqual(self.signature(log), "FileNotFoundError: [Errno <n>] No such file or directory: <str>")

    def test_variable_parts_are_masked(self):
        self.assertSameSignature(
            "ValueError: sample t1 has 42 peaks in /data/t1/a.mgf, expected 0x1f3a\n",
            "ValueError: sample t2 has 7 peaks in /data/t2/b.mgf, expected 0x2b4c\n"
        )

    def test_java_exception_message_above_its_frames(self):
        log = """
java.lang.OutOfMemoryError: Required array length 2147483639 + 28 is too large
	at java.base/jdk.internal.util.ArraysSupport.hugeLength(ArraysSupport.java:649)
	at java.base/java.io.ByteArrayOutputStream.write(ByteArrayOutputStream.java:130)
"""
        self.assertEqual(self.signature(log), "java.lang.OutOfMemoryError: Required array length <n> + <n> is too large")

    def test_truncated_java_stack_falls_back_on_top_frame(self):
        log = """
	at java.base/java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1181)
	at com.compomics.util.db.object.ObjectsCache.saveObjects(ObjectsCache.java:343)
"""
        self.assertEqual(
            self.signature(log),
            "<no error line> frame: at <path>(ObjectOutputStream.java:<n>)"
        )

    def test_bash_killed_with_progress_glued_at_line_start(self):
        self.assertSameSignature(
            "10% 20% 30%/opt/run.sh: line 12: 4242 Killed java -jar tool.jar\n",
            "10%/opt/run.sh: line 12: 777 Killed java -jar tool.jar\n"
        )

    def test_rust_panic_and_backtrace(self):
        log = """
thread 'main' panicked at src/main.rs:12:5:
index out of bounds: the len is 3 but the index is 7
stack backtrace:
   0: rust_begin_unwind
   1: core::panicking::panic_fmt
"""
        self.assertEqual(self.signature(log), "thread <str> panicked at <path>:<n>:…")

    def test_zero_error_summaries_are_not_errors(self):
        log = """
FATAL: disk full on /scratch
cleanup: 0 errors, failures: 0
"""
        self.assertEqual(self.signature(log), "FATAL: disk full on <path>")

    def test_no_error_line_uses_last_meaningful_line(self):
        log = """
Fri Jul 03 00:22:24 EDT 2026 Saving probabilities.
Search progress: 0%\b\b\b  1%\b\b\b  2%\b\b\b
10% 20% 30%
"""
        self.assertEqual(self.signature(log), "<no error line> last: Search progress: <n>%…")

    def test_dates_are_masked(self):
        self.assertSameSignature(
            "Fri Jul 03 00:22:24 EDT 2026 Saving results.\n",
            "Mon Aug 17 11:02:59 EDT 2026 Saving results.\n"
        )

    def test_empty_log(self):
        self.assertEqual(self.signature(""), "<no error line>")

    def test_augmented_lists(self):
        classifier = TaskClassifier()
        classifier.error_words.append(r"stopped unexpectedly")
        classifier.masks.insert(0, (r"PXD\d+", "<dataset>"))
        classifier.compile()

        self.assertEqual(
            classifier.signature("t1", "worker for PXD000123 stopped unexpectedly\n", None, "failed", 1, None, None, None),
            "worker for <dataset> stopped unexpectedly"
        )

    def test_overridden_method(self):

        class ToolAwareClassifier(TaskClassifier):
            def no_error_line_signature(self, lines, key):
                if any("[progress:" in l for l in lines):
                    return "<no error line> while running MSFragger"
                return super().no_error_line_signature(lines, key)

        classifier = ToolAwareClassifier().compile()

        self.assertEqual(
            classifier.signature("t1", "[progress: 12/100 (12%) - 87 spectra/s]\n", None, "failed", 1, None, None, None),
            "<no error line> while running MSFragger"
        )


class RuntimeMetricsTests(unittest.TestCase):

    def test_elapsed_times_of_the_last_run(self):
        with tempfile.TemporaryDirectory() as d:
            drypipe_log = Path(d, "drypipe.log")
            drypipe_log.write_text(
                "2026-10-03 17:39:49-0400 - INFO - TIME_ELAPSED_FOR:STEP-0: 00:00:05, 5.0\n"
                "2026-10-03 17:39:49-0400 - INFO - TIME_ELAPSED_FOR:TASK: 00:00:05, 5.0\n"
                "2026-10-03 17:45:00-0400 - INFO - restarted\n"
                "2026-10-03 17:45:02-0400 - INFO - TIME_ELAPSED_FOR:STEP-0: 00:00:02, 2.0\n"
                "2026-10-03 17:45:09-0400 - INFO - TIME_ELAPSED_FOR:STEP-1: 00:00:07, 7.25\n"
                "2026-10-03 17:45:09-0400 - INFO - TIME_ELAPSED_FOR:TASK: 00:00:09, 9.25\n"
            )
            metrics = RuntimeMetrics(drypipe_log)

            self.assertEqual(metrics.elapsed, 9.25)
            self.assertEqual(metrics.elapsed_by_step, {0: 2.0, 1: 7.25})
            self.assertIsNone(metrics.max_rss)

    def test_no_drypipe_log(self):
        metrics = RuntimeMetrics(Path("/nonexistent/drypipe.log"))
        self.assertIsNone(metrics.elapsed)
        self.assertIsNone(metrics.elapsed_by_step)
