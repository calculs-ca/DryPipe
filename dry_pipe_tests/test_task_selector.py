import argparse
import tempfile
import unittest
from pathlib import Path

from dry_pipe.task_selector import TaskSelector, InvalidSelection, KeyLists, Mask, add_selection_options

TASKS = [
    ("trim-S1", "completed", None),
    ("trim-S2", "failed", 3),
    ("trim-S3", "timed-out", 2),
    ("quant-S1", "failed", 1),
    ("quant-S2", "ready", None),
    ("quant-S7", "failed", 3),
]

REPORT = """## Error signatures

3 tasks, 2 signatures

| # | tasks | signature |
|---:|---:|---|
| 1 | 2 | `ValueError: <n>` |
| 2 | 1 | `KeyError: <str>` |

## Tails

### 1. trim-S2 (signature 1)

### 2. quant-S1 (signature 2)

### 3. quant-S7 (signature 1)
"""


def make_filter(threshold=0):
    def f(key, state, step):
        return step is not None and step > threshold
    return f


def keys_of(selector):
    return [key for key, _, _ in selector.select(TASKS)]


class TaskSelectorTests(unittest.TestCase):

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())

    def selector(self, *conditions, limit=None, mask=None):
        return TaskSelector(conditions, limit, mask, KeyLists(self.dir, "the-array", base_dir=self.dir))

    def file(self, name, text):
        f = self.dir / name
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(text)
        return f

    def test_no_condition_selects_all(self):
        self.assertEqual(keys_of(self.selector()), [k for k, _, _ in TASKS])

    def test_shortcuts_are_rewritten_as_where(self):
        self.file("to-retry.tsv", "trim-S1\n")
        for option, value, expected in [
            ("--where-state", "failed", "state == 'failed'"),
            ("--where-state", "failed,timed-out", "state in ('failed', 'timed-out')"),
            ("--where-state-not", "completed,killed", "state not in ('completed', 'killed')"),
            ("--where-state-not", "completed", "state != 'completed'"),
            ("--where-state", "unhealthy", "state in ('failed', 'timed-out', 'killed', 'crashed')"),
            ("--where-step", "3", "step == 3"),
            ("--where-step", "2,3", "step in (2, 3)"),
            ("--where-step-not", "0", "step != 0"),
            ("--where-key", "quant-*", "key.like('quant-*')"),
            ("--where-key-not", "quant-S7*", "not key.like('quant-S7*')"),
            ("--where-key-not", "@to-retry.tsv", "key not in @to-retry.tsv"),
            ("--where-key-not", "a*,b*", "not (key.like('a*') or key.like('b*'))"),
        ]:
            with self.subTest(option=option, value=value):
                condition = self.selector((option, value)).conditions[0]
                self.assertEqual(condition.where_expression, expected)

    def test_conditions_are_anded(self):
        selector = self.selector(("--where-key", "trim-*"), ("--where-state", "failed,timed-out"))
        self.assertEqual(keys_of(selector), ["trim-S2", "trim-S3"])

    def test_where(self):
        self.assertEqual(keys_of(self.selector(("--where", "key.like('quant-S?') and state == 'failed'"))), ["quant-S1", "quant-S7"])
        self.assertEqual(keys_of(self.selector(("--where", "state == 'failed' and step >= 3"))), ["trim-S2", "quant-S7"])
        self.assertEqual(keys_of(self.selector(("--where", "key.endswith('S1') or step == 2"))), ["trim-S1", "trim-S3", "quant-S1"])

    def test_where_on_tasks_without_step(self):
        self.assertEqual(keys_of(self.selector(("--where", "step < 2"))), ["quant-S1"])
        self.assertEqual(keys_of(self.selector(("--where", "step == None"))), ["trim-S1", "quant-S2"])

    def test_unknown_state_is_an_error(self):
        for condition in [("--where-state", "faild"), ("--where", "state == 'faild'"), ("--where", "state in ('failed', 'x')")]:
            with self.subTest(condition=condition):
                with self.assertRaisesRegex(InvalidSelection, "unknown state"):
                    self.selector(condition)

    def test_invalid_step_is_an_error(self):
        with self.assertRaisesRegex(InvalidSelection, "step numbers"):
            self.selector(("--where-step", "3+"))

    def test_invalid_where_is_an_error(self):
        with self.assertRaisesRegex(InvalidSelection, "not a valid python expression"):
            self.selector(("--where", "state =="))

    def test_key_list_tsv_with_slices(self):
        self.file("to-retry.tsv", "trim-S1\tx\ntrim-S2\nquant-S1\nquant-S2\n")
        for token, expected in [
            ("@to-retry.tsv", ["trim-S1", "trim-S2", "quant-S1", "quant-S2"]),
            ("@to-retry.tsv[:2]", ["trim-S1", "trim-S2"]),
            ("@to-retry.tsv[2:]", ["quant-S1", "quant-S2"]),
            ("@to-retry.tsv[:-1]", ["trim-S1", "trim-S2", "quant-S1"]),
        ]:
            with self.subTest(token=token):
                selector = self.selector(("--where", f"key in {token.replace('@', '@' + str(self.dir) + '/')}"))
                self.assertEqual(keys_of(selector), expected)

    def test_key_list_report(self):
        report = self.file("failed.3-3.md", REPORT)
        for suffix, expected in [
            ("", ["trim-S2", "quant-S1", "quant-S7"]),
            (":1", ["trim-S2", "quant-S7"]),
            (":1[:1]", ["trim-S2"]),
            (":1,2[1:]", ["quant-S1", "quant-S7"]),
        ]:
            with self.subTest(suffix=suffix):
                self.assertEqual(KeyLists().keys(f"{report}{suffix}"), frozenset(expected))

    def test_where_key_with_report_and_pattern(self):
        report = self.file("failed.3-3.md", REPORT)
        selector = self.selector(("--where-key", f"@{report}:1,2,trim-S3"))
        self.assertEqual(
            selector.conditions[0].where_expression,
            f"key in @{report}:1,2 or key.like('trim-S3')"
        )
        self.assertEqual(keys_of(selector), ["trim-S2", "trim-S3", "quant-S1", "quant-S7"])

    def test_array_submissions(self):
        self.file(".drypipe/the-array/array.0.tsv", "trim-S1\ntrim-S2\n")
        self.file(".drypipe/the-array/array.1.tsv", "quant-S1\nquant-S2\nquant-S7\n")
        self.assertEqual(keys_of(self.selector(("--where-key", "@array.0"))), ["trim-S1", "trim-S2"])
        self.assertEqual(keys_of(self.selector(("--where-key", "@array.last[:2]"))), ["quant-S1", "quant-S2"])

    def test_translate(self):
        self.file("a.tsv", "trim-S1\ntrim-S2\ntrim-S3\n")
        self.file(".drypipe/the-array/array.0.tsv", "quant-S1\n")
        key_lists = KeyLists(self.dir, "the-array", base_dir=self.dir)
        self.assertEqual(
            key_lists.translate("(key in @a.tsv[:2]) or key in @array.last"),
            ("(key in sets[0]) or key in sets[1]", [frozenset(["trim-S1", "trim-S2"]), frozenset(["quant-S1"])])
        )

    def test_invalid_slice_is_an_error(self):
        for key_list in ["a.tsv[3]", "a.tsv[1:2:3]", "a.tsv[x:]"]:
            with self.subTest(key_list=key_list):
                with self.assertRaisesRegex(InvalidSelection, "slice|number"):
                    KeyLists().keys(key_list)

    def test_missing_key_list_is_an_error(self):
        with self.assertRaisesRegex(InvalidSelection, "does not exist"):
            self.selector(("--where-key", "@nope.tsv"))

    def test_where_func(self):
        selector = self.selector(("--where-func", "test_task_selector:make_filter(2)"))
        self.assertEqual(keys_of(selector), ["trim-S2", "quant-S7"])

    def test_where_func_bare_name_uses_generator_module(self):
        selector = TaskSelector([("--where-func", "make_filter(2)")], generator_module="test_task_selector")
        self.assertEqual(keys_of(selector), ["trim-S2", "quant-S7"])

    def test_limit_is_applied_last(self):
        selector = self.selector(("--where-state", "failed"), limit=2)
        self.assertEqual(keys_of(selector), ["trim-S2", "quant-S1"])

    def test_mask(self):
        self.file("high-ram.tsv", "quant-S1\nquant-S2\nquant-S7\n")
        mask_file = self.file(Mask.file_name, "- @high-ram.tsv\n+ quant-S2  # keep this one\n")
        self.assertEqual(keys_of(self.selector(mask=Mask(mask_file))), ["trim-S1", "trim-S2", "trim-S3", "quant-S2"])
        self.assertEqual(keys_of(self.selector(mask=Mask(mask_file, disabled=True))), [k for k, _, _ in TASKS])

    def test_explain(self):
        mask_file = self.file(Mask.file_name, "- quant-S7\n")
        selector = self.selector(
            ("--where-key", "trim-*,quant-*"), ("--where-state", "failed"), ("--where", "step >= 3"),
            limit=10, mask=Mask(mask_file)
        )
        selector.select(TASKS)
        self.assertEqual(selector.explain(), [
            "generator                                                 6",
            "mask           drypipe-task-universe.mask                 5  (-1)",
            "--where-key    key.like('trim-*') or key.like('quant-*')  5   (0)",
            "--where-state  state == 'failed'                          2  (-3)",
            "--where        step >= 3                                  1  (-1)",
            "--limit        10                                         1   (0)",
        ])

    def test_left_out_keys_saved_in_proposed_file(self):
        selector = self.selector(("--where-state", "failed"), limit=1)
        selector.select(TASKS)
        self.file("remain-2_1.tsv", "")
        prompts = []

        def accept_default(prompt, default):
            prompts.append((prompt, default))
            return default

        saved = selector.offer_to_save_left_out_keys("drypipe list --where-state failed --limit 1", self.dir, accept_default)

        self.assertEqual(prompts, [(
            "--limit 1 left out 2 tasks, save their keys in (clear the name to cancel): ",
            str(self.dir / "remain-2_2.tsv")
        )])
        self.assertEqual(saved, self.dir / "remain-2_2.tsv")
        lines = saved.read_text().splitlines()
        self.assertEqual(lines[:2], ["# drypipe list --where-state failed --limit 1", "#"])
        self.assertEqual(lines[2:-2], [f"# {line}" for line in selector.explain()])
        self.assertEqual(lines[-2:], ["quant-S1", "quant-S7"])
        self.assertEqual(KeyLists(base_dir=self.dir).keys("remain-2_2.tsv"), frozenset(["quant-S1", "quant-S7"]))

    def test_left_out_keys_saved_in_renamed_file(self):
        selector = self.selector(("--where-state", "failed"), limit=1)
        selector.select(TASKS)
        renamed = self.dir / "my-retries.tsv"
        self.assertEqual(selector.offer_to_save_left_out_keys("cmd", self.dir, lambda prompt, default: str(renamed)), renamed)
        self.assertTrue(renamed.exists())

    def test_cleared_name_cancels(self):
        selector = self.selector(("--where-state", "failed"), limit=1)
        selector.select(TASKS)
        self.assertIsNone(selector.offer_to_save_left_out_keys("cmd", self.dir, lambda prompt, default: ""))
        self.assertEqual(list(self.dir.glob("remain-*")), [])

    def test_nothing_offered_when_limit_leaves_nothing_out(self):
        selector = self.selector(("--where-state", "failed"), limit=10)
        selector.select(TASKS)

        def never_asked(prompt, default):
            raise AssertionError("asked")

        self.assertIsNone(selector.offer_to_save_left_out_keys("cmd", self.dir, never_asked))


class SbatchOptionsOfKeyListTests(unittest.TestCase):

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())

    def sbatch_options(self, text, key_list="keys.tsv"):
        (self.dir / "keys.tsv").write_text(text)
        return KeyLists(base_dir=self.dir).option_lines(key_list, "--sbatch-options")

    def test_directive_with_spaces_and_quotes(self):
        self.assertEqual(
            self.sbatch_options("#   --sbatch-options='--mem=40G --time=1:00:00'\nt1\n"),
            ["--mem=40G --time=1:00:00"]
        )

    def test_directive_of_a_sliced_key_list(self):
        self.assertEqual(self.sbatch_options("# --sbatch-options=--mem=40G\nt1\n", "keys.tsv[:1]"), ["--mem=40G"])

    def test_other_comments_are_ignored(self):
        self.assertEqual(self.sbatch_options("# drypipe list --where-state failed\n# --where-key x\nt1\n"), [])

    def test_directive_does_not_change_the_keys(self):
        (self.dir / "keys.tsv").write_text("# --sbatch-options=--mem=40G\nt1\n")
        self.assertEqual(KeyLists(base_dir=self.dir).keys("keys.tsv"), frozenset(["t1"]))

    def test_malformed_directive_is_an_error(self):
        for text in ["# --sbatch-options '--mem=40G'\n", "# --sbatch-options='--mem=40G\n", "# --sbatch-options=a b\n"]:
            with self.subTest(text=text), self.assertRaises(InvalidSelection):
                self.sbatch_options(text)

    def test_env_lines_apart_from_sbatch_options_lines(self):
        (self.dir / "keys.tsv").write_text("# --sbatch-options=--mem=40G\n# --env='JVM_MEM=12g JVM_STACK=8m'\nt1\n")
        key_lists = KeyLists(base_dir=self.dir)
        self.assertEqual(key_lists.option_lines("keys.tsv", "--env"), ["JVM_MEM=12g JVM_STACK=8m"])
        self.assertEqual(key_lists.option_lines("keys.tsv", "--sbatch-options"), ["--mem=40G"])

    def test_reports_and_arrays_have_none(self):
        self.assertEqual(KeyLists(base_dir=self.dir).option_lines("failed.3-1.md:1", "--sbatch-options"), [])
        self.assertEqual(KeyLists(base_dir=self.dir).option_lines("array.last", "--sbatch-options"), [])


class ArgparseTests(unittest.TestCase):

    def test_conditions_keep_command_line_order(self):
        parser = argparse.ArgumentParser()
        add_selection_options(parser)
        args = parser.parse_args([
            "--where-state", "failed", "--where-key", "t*", "--limit", "3", "--where", "step > 1", "--where-state", "ready"
        ])
        self.assertEqual(args.where_conditions, [
            ("--where-state", "failed"), ("--where-key", "t*"), ("--where", "step > 1"), ("--where-state", "ready")
        ])
        self.assertEqual(args.limit, 3)
        self.assertEqual(parser.parse_args([]).where_conditions, [])


if __name__ == '__main__':
    unittest.main()
