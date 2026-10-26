import argparse
import ast
import fnmatch
import inspect
import itertools
import re
import sys
from pathlib import Path

from dry_pipe.core_lib import CliUserError, func_from_mod_func

STATES = [
    "waiting", "ready", "_step-started", "step-started", "step-completed",
    "completed", "failed", "timed-out", "killed", "crashed"
]

STATE_GROUPS = {
    "unhealthy": ["failed", "timed-out", "killed", "crashed"],
    "incomplete": [s for s in STATES if s != "completed"],
}


class InvalidSelection(CliUserError):
    explainer = "see the --where options in --help"


class NotAnAnalyzeLogsFile(CliUserError):
    explainer = "Expected a markdown file written by 'drypipe analyze-logs'"


def _signatures_of_analysis_file(path, lines):
    """
    (task count, signature cell) of each row of the error signatures table of an analyze-logs file,
    in signature number order, raises when the table is malformed
    """
    separator_of_header = {
        "| # | tasks | signature |": "|---:|---:|---|",
        "| # | tasks | signature | keys |": "|---:|---:|---|---|",
    }

    header_index = next((i for i, line in enumerate(lines) if line in separator_of_header), None)
    if header_index is None:
        raise NotAnAnalyzeLogsFile(f"{path}: not an analyze-logs file, it has no error signatures table")

    separator = separator_of_header[lines[header_index]]
    if lines[header_index + 1:header_index + 2] != [separator]:
        raise NotAnAnalyzeLogsFile(f"{path}:{header_index + 2}: not an analyze-logs file, expected {separator}")

    rows = itertools.takewhile(lambda l: l.startswith("|"), lines[header_index + 2:])
    for number, row in enumerate(rows, start=1):
        cells = re.fullmatch(rf"\| {number} \| (\d+) \| (`.*`) \|(?: [^|]* \|)?", row)
        if cells is None:
            raise NotAnAnalyzeLogsFile(
                f"{path}:{header_index + 2 + number}: not an analyze-logs file, "
                f"expected | {number} | <task count> | `<signature>` |"
            )
        yield int(cells.group(1)), cells.group(2)


def _keys_and_signature_numbers_of_analysis_file(path, lines):
    """
    from the "### i. <key> (signature <n>)" headings of an analyze-logs file, skipping the fenced tails,
    raises when a heading is malformed
    """
    fence = None
    for line_number, line in enumerate(lines, start=1):
        if fence is not None:
            if line == fence:
                fence = None
        elif line.startswith("```"):
            fence = re.match(r"`+", line).group()
        elif line.startswith("### "):
            heading = re.fullmatch(r"### \d+\. (\S+) \(signature (\d+)\)", line)
            if heading is None:
                raise NotAnAnalyzeLogsFile(
                    f"{path}:{line_number}: not an analyze-logs file, expected ### <i>. <task-key> (signature <n>)"
                )
            yield heading.group(1), int(heading.group(2))


def _signature_numbers(text, signature_count):
    """parses "1,3" into {1, 3}, the numbers must be in 1..signature_count"""
    try:
        numbers = {int(n) for n in text.split(",")}
    except ValueError:
        raise CliUserError(f"expected comma separated signature numbers, got '{text.strip()}'")
    invalid = sorted(n for n in numbers if not 1 <= n <= signature_count)
    if invalid:
        raise CliUserError(f"no signature numbered {invalid}, valid numbers are 1 to {signature_count}")
    return numbers


def _parse_func_filter_spec(spec):
    """
    Parses a --where-func value into (module_func_reference, args, kwargs).

    Accepts a bare reference ("a.b.c:f" or "f") or a call form ("a.b.c:f('z', threshold=3)").
    The reference part is returned verbatim (the ':' and '.' make it invalid python, so it is not
    parsed); the call arguments are parsed as python literals (ast.literal_eval, no code executed).
    """
    paren = spec.find("(")
    if paren == -1:
        return spec.strip(), [], {}

    if not spec.rstrip().endswith(")"):
        raise InvalidSelection(
            f"--where-func '{spec}' is malformed, expected 'module:func' or \"module:func(args...)\""
        )

    reference = spec[:paren].strip()
    call_source = spec[paren:].strip()

    try:
        call_node = ast.parse(f"__f__{call_source}", mode="eval").body
    except SyntaxError:
        raise InvalidSelection(f"--where-func '{spec}' has an invalid argument list")

    if not isinstance(call_node, ast.Call):
        raise InvalidSelection(f"--where-func '{spec}' has an invalid argument list")

    try:
        args = [ast.literal_eval(a) for a in call_node.args]
        kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in call_node.keywords}
    except ValueError:
        raise InvalidSelection(
            f"--where-func '{spec}' arguments must be python literals (str, int, ...), not expressions"
        )

    return reference, args, kwargs


class _AppendCondition(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        # a new list, not .append(): argparse shares the default list between parses
        setattr(namespace, self.dest, [*getattr(namespace, self.dest), (option_string, value)])


def add_selection_options(parser):
    group = parser.add_argument_group(
        "task selection",
        "each option adds one 'and' condition, commas inside a value mean 'or', "
        "@LIST is a key list: a tsv file, an analyze-logs report (failed.3-42.md:1,2), "
        "or an array submission (array.3, array.last), optionally sliced: @keys.tsv[:100]"
    )
    for option, metavar, help in [
        ("--where", "EXPR", "a python expression of key, state and step, ex: \"state == 'failed' and step >= 2\""),
        ("--where-key", "GLOB,@LIST,...", "tasks whose key matches a glob, or is in a key list"),
        ("--where-key-not", "GLOB,@LIST,...", "tasks whose key matches none of the globs and key lists"),
        ("--where-state", "STATE,...", f"tasks in one of the states: {', '.join(STATES)}, or groups: {', '.join(STATE_GROUPS)}"),
        ("--where-state-not", "STATE,...", "tasks in none of the states"),
        ("--where-step", "N,...", "tasks at one of the steps"),
        ("--where-step-not", "N,...", "tasks at none of the steps"),
        ("--where-func", "module:f(args)", "tasks for which the filter function(key, state, step) returned by f(args) is true"),
    ]:
        group.add_argument(
            option, dest="where_conditions", action=_AppendCondition, default=[], metavar=metavar, help=help
        )
    group.add_argument("--limit", type=int, metavar="N", help="at most N tasks, applied after all conditions")
    group.add_argument("--no-mask", action="store_true", help=f"ignore the mask file {Mask.file_name}")
    group.add_argument("--explain-off", action="store_true", help="don't explain the selection on stderr")


class _NoStep:
    """the step of a task that has none (ex: completed): every order comparison is false"""

    def __lt__(self, other):
        return False

    __le__ = __gt__ = __ge__ = __lt__

    def __eq__(self, other):
        return other is None or isinstance(other, _NoStep)

    def __hash__(self):
        return hash(None)

    def __repr__(self):
        return "None"


_NO_STEP = _NoStep()


class _Key(str):
    """the key of a task in a --where expression"""

    def like(self, pattern):
        """matches a pattern with shell wildcards: key.like('trim-*')"""
        return fnmatch.fnmatch(self, pattern)


class KeyLists:
    """
    the @ key lists of a --where expression, without their '@':

        to-retry.tsv[:100]           the first 100 keys of a key file, the first column of a tsv
        failed.3-434.md:2,4[:10]     the first 10 tasks of signatures 2 and 4 of an analyze-logs report
        array.9, array.last          the keys of a submission of the array task

    relative paths are relative to base_dir
    """

    def __init__(self, pipeline_instance_dir=None, array_task_key=None, base_dir="."):
        self.pipeline_instance_dir = pipeline_instance_dir
        self.array_task_key = array_task_key
        self.base_dir = Path(base_dir)

    def translate(self, expression):
        """
        (python expression, sets): the expression, with the i-th @ key list replaced by sets[i],
        the keys are loaded here, so a missing key list is an error before any task is selected
        """
        head, *texts = expression.split("@")

        def split_key_list(text):
            """a key list ends at a space, or at a ')' outside of its slice"""
            in_slice = False
            for i, c in enumerate(text):
                if c == "[":
                    in_slice = True
                elif c == "]":
                    in_slice = False
                elif not in_slice and (c.isspace() or c == ")"):
                    return text[:i], text[i:]
            return text, ""

        key_lists_and_rests = [split_key_list(text) for text in texts]
        python_expression = head + "".join(f"sets[{i}]{rest}" for i, (_, rest) in enumerate(key_lists_and_rests))
        return python_expression, [self.keys(key_list) for key_list, _ in key_lists_and_rests]

    def keys(self, key_list):
        """the set of keys of a key list, ex: 'failed.3-434.md:2[:10]'"""

        def number(text):
            try:
                return int(text)
            except ValueError:
                raise InvalidSelection(f"@{key_list}: expected a number, got '{text}'")

        def slice_of(slice_text):
            if slice_text == "":
                return slice(None)
            if not slice_text.endswith("]") or slice_text.count(":") != 1:
                raise InvalidSelection(f"@{key_list}: expected a slice like [10:20], [:20] or [10:]")
            start, end = slice_text[:-1].split(":")
            return slice(None if start == "" else number(start), None if end == "" else number(end))

        source, _, slice_text = key_list.partition("[")
        keys_slice = slice_of(slice_text)

        if source.startswith("array."):
            submission = source[len("array."):]
            keys = self.array(-1 if submission == "last" else number(submission))
        elif source.endswith(".md") or ".md:" in source:
            report, _, signatures = source.rpartition(".md")
            keys = self.log_report(report + ".md", signatures[1:] if signatures != "" else None)
        else:
            keys = self.key_file(source)

        return frozenset(keys[keys_slice])

    def key_file(self, path):
        """the first column of each line, # starts a comment line"""
        file = self.base_dir / path
        if not file.exists():
            raise InvalidSelection(f"key list {file} does not exist")

        def key(line_number, line):
            k = line.split("\t")[0].strip()
            if len(k.split()) > 1:
                raise InvalidSelection(f"{file}:{line_number}: task keys can't contain spaces, got '{k}'")
            return k

        with open(file) as f:
            return [
                key(line_number, line) for line_number, line in enumerate(f, start=1)
                if line.strip() != "" and not line.lstrip().startswith("#")
            ]

    def log_report(self, path, signatures=None):
        """the keys of an analyze-logs report, of the signature numbers given as '2,4', or of all"""
        file = self.base_dir / path
        if not file.exists():
            raise InvalidSelection(f"analyze-logs report {file} does not exist")
        lines = file.read_text().splitlines()
        # parsing the signatures table validates the report, even when all its keys are selected
        signature_count = len(list(_signatures_of_analysis_file(file, lines)))
        keys_and_numbers = _keys_and_signature_numbers_of_analysis_file(file, lines)
        if signatures is None:
            return [key for key, _ in keys_and_numbers]
        numbers = _signature_numbers(signatures, signature_count)
        return [key for key, number in keys_and_numbers if number in numbers]

    def array(self, submission):
        """the keys of an array submission, a negative submission counts from the last one: -1 is the last"""
        if self.array_task_key is None:
            raise InvalidSelection(f"@array.{submission} needs the array task, given by -k")

        array_dir = Path(self.pipeline_instance_dir, ".drypipe", self.array_task_key)

        if submission < 0:
            submissions = sorted(
                int(f.name.split(".")[1]) for f in array_dir.glob("array.*.tsv") if f.name.split(".")[1].isdigit()
            )
            if len(submissions) < -submission:
                raise InvalidSelection(f"array task {self.array_task_key} has {len(submissions)} submissions")
            submission = submissions[submission]

        return KeyLists(base_dir=array_dir).key_file(f"array.{submission}.tsv")


class Condition:
    """one stage of a selection, ANDed with the others"""

    def __init__(self, option, value, where_expression, predicate):
        self.option = option
        self.value = value
        self.where_expression = where_expression
        self.predicate = predicate

    def filter(self, rows):
        return [row for row in rows if self.predicate(*row[:3])]

    def explain(self):
        return [self.option, self.where_expression or self.value]

    @staticmethod
    def parse(option, value, key_lists, generator_module=None):
        if option == "--where":
            return Condition(option, value, None, _compile_where(value, key_lists))
        if option == "--where-func":
            return Condition(option, value, None, _where_func(value, generator_module))

        if value.strip() == "":
            raise InvalidSelection(f"{option} has an empty value")

        rewrite = {
            "--where-key": _rewrite_where_key,
            "--where-state": _rewrite_where_state,
            "--where-step": _rewrite_where_step,
        }[option.removesuffix("-not")]
        expression = rewrite(option, value, negated=option.endswith("-not"))
        return Condition(option, value, expression, _compile_where(expression, key_lists, option, value))


def _in_or_equals(variable, values, negated):
    if len(values) == 1:
        return f"{variable} {'!=' if negated else '=='} {values[0]!r}"
    listed = ", ".join(repr(v) for v in values)
    return f"{variable} {'not in' if negated else 'in'} ({listed})"


def _rewrite_where_key(option, value, negated):

    def items_of(value):
        """the comma separated items, an analyze-logs report's signature numbers stay with it: @failed.3-423.md:1,3"""
        items = []
        for part in value.split(","):
            if len(items) > 0 and part[:1].isdigit() and ".md:" in items[-1]:
                items[-1] += "," + part
            else:
                items.append(part)
        return items

    items = items_of(value)

    def condition_of(item, negated=False):
        if item.startswith("@"):
            return f"key {'not in' if negated else 'in'} {item}"
        return f"{'not ' if negated else ''}key.like({item!r})"

    items = [item.strip() for item in items]
    if len(items) == 1:
        return condition_of(items[0], negated)
    conditions = " or ".join(condition_of(item) for item in items)
    return f"not ({conditions})" if negated else conditions


def _rewrite_where_state(option, value, negated):

    def states_of(item):
        if item in STATE_GROUPS:
            return STATE_GROUPS[item]
        _check_state(option, item)
        return [item]

    states = [s for item in value.split(",") for s in states_of(item.strip())]
    return _in_or_equals("state", list(dict.fromkeys(states)), negated)


def _rewrite_where_step(option, value, negated):

    def step_of(item):
        if not item.strip().isdigit():
            raise InvalidSelection(f"{option} expects step numbers, got '{item.strip()}'")
        return int(item)

    return _in_or_equals("step", [step_of(item) for item in value.split(",")], negated)


def _check_state(source, state):
    if state not in STATES:
        groups = ", ".join(STATE_GROUPS)
        raise InvalidSelection(
            f"{source}: unknown state '{state}', valid states are: {', '.join(STATES)}, and groups: {groups}"
        )


def _compile_where(expression, key_lists, option="--where", typed_value=None):
    """
    compiles a --where expression into a predicate(key, state, step),
    the i-th @ key list is replaced by sets[i], see KeyLists
    """
    source_of_error = f"{option} '{typed_value if typed_value is not None else expression}'"
    python_expression, sets = key_lists.translate(expression)

    try:
        tree = ast.parse(python_expression, mode="eval")
    except SyntaxError as e:
        raise InvalidSelection(f"{source_of_error} is not a valid python expression: {e.msg}")

    def check_compared_states():
        """state == 'faild' is an error, not an empty selection"""
        for node in ast.walk(tree):
            if not isinstance(node, ast.Compare):
                continue
            operands = [node.left, *node.comparators]
            if not any(isinstance(o, ast.Name) and o.id == "state" for o in operands):
                continue
            for o in operands:
                for constant in ast.walk(o):
                    if isinstance(constant, ast.Constant) and isinstance(constant.value, str):
                        _check_state(source_of_error, constant.value)

    check_compared_states()

    code = compile(tree, f"<{option}>", "eval")
    names = {"sets": sets}

    def predicate(key, state, step):
        try:
            return eval(code, names, {"key": _Key(key), "state": state, "step": _NO_STEP if step is None else step})
        except NameError as e:
            raise InvalidSelection(f"{source_of_error}: {e}, the variables are key, state and step")

    return predicate


def _where_func(spec, generator_module):
    reference, call_args, call_kwargs = _parse_func_filter_spec(spec)

    if ":" not in reference:
        if generator_module is None:
            raise InvalidSelection(
                f"--where-func '{spec}' has no module, and there is no generator module to look it up in"
            )
        reference = f"{generator_module}:{reference}"

    factory = func_from_mod_func(reference)

    # the referenced function is a factory: it receives the --where-func args and returns the filter
    try:
        inspect.signature(factory).bind(*call_args, **call_kwargs)
    except TypeError as e:
        raise InvalidSelection(f"--where-func '{spec}': arguments don't match {reference}: {e}")

    filter_func = factory(*call_args, **call_kwargs)
    if not callable(filter_func):
        raise InvalidSelection(
            f"--where-func '{spec}': {reference} must return a filter function(key, state, step), "
            f"got a {type(filter_func).__name__}"
        )
    return filter_func


class Mask:
    """
    the rules of a mask file, one per line: '- <glob>', '+ <glob>', '- @<list>', '+ @<list>',
    the last matching rule wins, keys matched by no rule are not masked
    """

    file_name = "drypipe-task-universe.mask"

    def __init__(self, mask_file, disabled=False):
        self.mask_file = Path(mask_file)
        self.disabled = disabled
        if disabled:
            return

        key_lists = KeyLists(base_dir=self.mask_file.resolve().parent)

        def parse_rule(line_number, line):
            sign, _, pattern = line.partition(" ")
            pattern = pattern.strip()
            if sign not in ("+", "-") or pattern == "" or len(pattern.split()) > 1:
                raise InvalidSelection(
                    f"{mask_file}:{line_number}: expected '+ <glob>', '- <glob>', '+ @<list>' or '- @<list>', got '{line}'"
                )
            if not pattern.startswith("@"):
                return sign == "-", lambda key: fnmatch.fnmatch(key, pattern)
            return sign == "-", key_lists.keys(pattern[1:]).__contains__

        with open(mask_file) as f:
            lines = [(line_number, line.split("#")[0].strip()) for line_number, line in enumerate(f, start=1)]

        self.rules = [parse_rule(line_number, line) for line_number, line in lines if line != ""]

    def is_masked(self, key):
        return next((masks for masks, matches in reversed(self.rules) if matches(key)), False)

    def filter(self, rows):
        if self.disabled:
            return rows
        return [row for row in rows if not self.is_masked(row[0])]

    def explain(self):
        if self.disabled:
            return ["mask", "disabled (--no-mask)"]
        return ["mask", self.mask_file.name]


class Limit:

    def __init__(self, n):
        self.n = n
        self.left_out = []

    def filter(self, rows):
        self.left_out = rows[self.n:]
        return rows[:self.n]

    def explain(self):
        return ["--limit", str(self.n)]


class TaskSelector:

    def __init__(self, conditions=(), limit=None, mask=None, key_lists=None, generator_module=None):
        key_lists = key_lists or KeyLists()
        self.mask = mask
        self.conditions = [
            Condition.parse(option, value, key_lists, generator_module) for option, value in conditions
        ]
        self.limit = None if limit is None else Limit(limit)
        self.counts = []

    @staticmethod
    def from_args(parsed_args, pipeline_instance_dir, array_task_key=None, generator_module=None):

        def mask():
            mask_file = Path(pipeline_instance_dir, Mask.file_name)
            if not mask_file.exists():
                return None
            return Mask(mask_file, disabled=parsed_args.no_mask)

        return TaskSelector(
            parsed_args.where_conditions,
            parsed_args.limit,
            mask(),
            KeyLists(pipeline_instance_dir, array_task_key),
            generator_module
        )

    def stages(self):
        if self.mask is not None:
            yield self.mask
        yield from self.conditions
        if self.limit is not None:
            yield self.limit

    def has_stages(self):
        return any(True for _ in self.stages())

    def select(self, items, key_state_step=lambda item: item):
        """the selected items, key_state_step(item) returns (key, state, step)"""
        remaining = [(*key_state_step(item), item) for item in items]
        self.counts = [(None, len(remaining))]
        for stage in self.stages():
            remaining = stage.filter(remaining)
            self.counts.append((stage, len(remaining)))
        return [item for _, _, _, item in remaining]

    def explain(self):
        """the lines of the explanation table, available after select()"""

        def rows():
            previous = None
            for stage, count in self.counts:
                columns = ["generator", ""] if stage is None else stage.explain()
                removed = "" if previous is None else f"({count - previous})"
                previous = count
                yield [*columns, str(count), removed]

        table = list(rows())
        widths = [max(len(row[i]) for row in table) for i in range(4)]

        def line(row):
            option, condition, count, removed = row
            return (
                f"{option:<{widths[0]}}  {condition:<{widths[1]}}  {count:>{widths[2]}}  {removed:>{widths[3]}}"
            ).rstrip()

        return [line(row) for row in table]

    def left_out_keys(self):
        """the keys of the tasks selected by all conditions, but left out by --limit, available after select()"""
        if self.limit is None:
            return []
        return [key for key, _, _, _ in self.limit.left_out]

    def offer_to_save_left_out_keys(self, command_line, directory=".", ask=None):
        """
        when --limit left out tasks, asks to save their keys in a file, ENTER saves in the proposed file,
        returns the path of the saved file, or None
        """
        keys = self.left_out_keys()
        if len(keys) == 0:
            return None

        if ask is None:
            if not (sys.stdin.isatty() and sys.stdout.isatty()):
                return None
            ask = _ask_with_editable_default

        def proposed_file():
            names = (f"remain-{len(keys)}_{idx}.tsv" for idx in itertools.count(1))
            return next(Path(directory, name) for name in names if not Path(directory, name).exists())

        answer = ask(
            f"--limit {self.limit.n} left out {len(keys)} tasks, save their keys in (clear the name to cancel): ",
            str(proposed_file())
        )
        if answer.strip() == "":
            return None

        path = Path(answer.strip())
        comments = [f"# {command_line}", "#", *(f"# {line}" for line in self.explain())]
        path.write_text("\n".join([*comments, *keys]) + "\n")
        return path


def _ask_with_editable_default(prompt, default):
    """input() with the default already typed, so ENTER accepts it, ctrl-c or ctrl-d cancels"""
    # imported here: importing readline changes how every input() of the process behaves
    import readline

    readline.set_startup_hook(lambda: readline.insert_text(default))
    try:
        return input(prompt)
    except (EOFError, KeyboardInterrupt):
        print(file=sys.stderr)
        return ""
    finally:
        readline.set_startup_hook()
