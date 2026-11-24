"""bash completion of the drypipe command line, see 'drypipe completion bash'"""
import argparse
import os
import re
from pathlib import Path

from dry_pipe.core_lib import CliUserError
from dry_pipe.task_selector import STATES, STATE_GROUPS, _items_of_where_key, _signatures_of_analysis_file

BASH_SCRIPT = """\
# to enable in the current shell (add to ~/.bashrc to make it permanent):
#   eval "$(drypipe completion bash)"
_drypipe() {
    mapfile -t COMPREPLY < <(COMP_WORDBREAKS="$COMP_WORDBREAKS" drypipe __complete "${COMP_LINE:0:COMP_POINT}" 2>/dev/null)
}
complete -o nospace -F _drypipe drypipe
"""


def completions(parser, line, wordbreaks):
    """
    the completions of the last word of line, as bash expects them in COMPREPLY: they replace only the part
    of the word that readline completes, i.e. after its last word break (ex: '=', ':') or open quote
    """
    *previous, (start, current) = _words(line)
    words = [value for _, value in previous[1:]]
    skip = _completed_prefix_length(line[start:], wordbreaks)
    return (c[skip:] for c in _candidates(parser, words, current) if c.startswith(current))


def _words(line):
    """(start, unquoted value) of the shell words of line, the last one is the word being completed, possibly empty"""
    words = []
    start, value, quote = None, "", None
    chars = iter(enumerate(line))
    for i, c in chars:
        if quote is None and c.isspace():
            if start is not None:
                words.append((start, value))
                start, value = None, ""
            continue
        if start is None:
            start = i
        if c == quote:
            quote = None
        elif quote is None and c in "'\"":
            quote = c
        elif c == "\\" and quote != "'":
            value += next(chars, (i, ""))[1]
        else:
            value += c
    words.append((len(line) if start is None else start, value))
    return words


def _completed_prefix_length(raw_word, wordbreaks):
    """the length of the unquoted part of raw_word that precedes the part readline completes"""
    length, length_at_break, quote, escaped = 0, 0, None, False
    for c in raw_word:
        if escaped:
            length += 1
            escaped = False
        elif c == quote:
            quote = None
            length_at_break = length
        elif quote is None and c in "'\"":
            quote = c
            length_at_break = length
        elif c == "\\" and quote != "'":
            escaped = True
        elif quote is None and c in wordbreaks and c in "@$":
            # readline's special prefixes: they break words, but the word starts with them
            length_at_break = length
            length += 1
        else:
            length += 1
            if quote is None and c in wordbreaks:
                length_at_break = length
    return length_at_break


def _candidates(parser, words, current):
    commands = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction)).choices
    command = next((w for w in words if w in commands), None)

    if command is None:
        if current.startswith("-"):
            return _option_names(parser)
        return (f"{c} " for c in commands)

    actions = commands[command]._actions

    if current.startswith("-") and "=" in current:
        option, _, value = current.partition("=")
        action = _action(actions, option)
        return () if action is None else (f"{option}={c}" for c in _values(action, value, words))

    if current.startswith("-"):
        return _option_names(commands[command])

    previous_action = _action(actions, words[-1])
    if previous_action is not None and previous_action.nargs != 0:
        return _values(previous_action, current, words)

    positionals = [a for a in actions if not a.option_strings]
    given_count = _positional_count(actions, words[words.index(command) + 1:])
    if given_count < len(positionals):
        return _values(positionals[given_count], current, words)
    return ()


def _option_names(parser):
    return (f"{o} " for a in parser._actions for o in a.option_strings)


def _action(actions, option):
    return next((a for a in actions if option in a.option_strings), None)


def _positional_count(actions, words):
    count, is_option_value = 0, False
    for word in words:
        if is_option_value:
            is_option_value = False
        elif word.startswith("-"):
            action = _action(actions, word)
            is_option_value = action is not None and action.nargs != 0
        else:
            count += 1
    return count


def _values(action, value, words):
    name = next((o for o in action.option_strings if o.startswith("--")), action.dest)
    completer = _VALUE_COMPLETERS.get(name)
    if completer is not None:
        return completer(value, words)
    if action.choices is not None:
        return (f"{c} " for c in action.choices)
    return ()


def _paths(prefix, is_selected_file=lambda name: True):
    """the dirs (ending with '/') and selected files (ending with a space) starting with prefix"""
    head = prefix[:prefix.rfind("/") + 1]
    name_prefix = prefix[len(head):]
    try:
        entries = list(os.scandir(os.path.expanduser(head) or "."))
    except OSError:
        return
    for entry in entries:
        if not entry.name.startswith(name_prefix) or entry.name.startswith(".") and not name_prefix.startswith("."):
            continue
        if entry.is_dir():
            yield f"{head}{entry.name}/"
        elif is_selected_file(entry.name):
            yield f"{head}{entry.name} "


def _dirs(value, words):
    return _paths(value, is_selected_file=lambda name: False)


def _files(value, words):
    return _paths(value)


def _states(value, words):
    head = value[:value.rfind(",") + 1]
    return (f"{head}{s} " for s in [*STATES, *STATE_GROUPS])


def _key_list(source, words):
    """completions of a key list, without its '@'"""
    if ".md:" in source:
        report, _, numbers = source.rpartition(".md:")
        head = f"{report}.md:{numbers[:numbers.rfind(',') + 1]}"
        return (f"{head}{n} " for n in range(1, _signature_count(f"{report}.md") + 1))
    return [*_paths(source), *_array_submissions(words)]


def _signature_count(report):
    try:
        return len(list(_signatures_of_analysis_file(report, Path(report).read_text().splitlines())))
    except (OSError, CliUserError):
        return 0


def _array_submissions(words):
    """array.N of the array task given by -k, and array.last"""
    pid = _option_value(words, "--pipeline-instance-dir", "-pid") or os.environ.get("DRYPIPE_PIPELINE_INSTANCE_DIR")
    key = _option_value(words, "--task-key", "-k") or os.environ.get("DRYPIPE_TASK_KEY")
    if pid is None or key is None:
        return []
    submissions = [
        f.name[:-len(".tsv")]
        for f in Path(os.path.expanduser(pid), ".drypipe", key).glob("array.*.tsv")
        if f.name.split(".")[1].isdigit()
    ]
    if not submissions:
        return []
    return [f"{s} " for s in ["array.last", *submissions]]


def _option_value(words, *names):
    """the value of the last occurrence of the option, given as '--name value' or '--name=value'"""
    def value_at(i):
        option, equals, value = words[i].partition("=")
        if words[i] in names:
            return words[i + 1] if i + 1 < len(words) else None
        if equals and option in names:
            return value
        return None

    return next(filter(None, map(value_at, reversed(range(len(words))))), None)


def _key_lists(value, words):
    """completions of the last item of a --where-key value, when it is a key list"""
    items = _items_of_where_key(value)
    last = items[-1]
    if last == "" and len(items) > 1 and ".md:" in items[-2]:
        last = f"{items[-2]},"
    head = value[:len(value) - len(last)]
    if not last.startswith("@"):
        return ()
    return (f"{head}@{c}" for c in _key_list(last[1:], words))


def _where_expression(value, words):
    """completions of the key list being typed at the end of a --where expression"""
    fragment = re.split(r"[\s(]", value)[-1]
    head = value[:len(value) - len(fragment)]
    if not fragment.startswith("@"):
        return ()
    return (f"{head}@{c}" for c in _key_list(fragment[1:], words))


_VALUE_COMPLETERS = {
    "--where-state": _states,
    "--where-state-not": _states,
    "--state": lambda value, words: STATES,
    "--where-key": _key_lists,
    "--where-key-not": _key_lists,
    "--where": _where_expression,
    "--task-set": lambda value, words: ["@ALL ", *_paths(value)],
    "--pipeline-instance-dir": _dirs,
    "--pipeline-instances-dir": _dirs,
    "--into": _dirs,
    "--dir": _dirs,
    "--log-conf": _files,
    "--empty-db": _files,
    "--dest": _files,
    "--name": _files,
    "analysis_file": lambda value, words: _paths(value, is_selected_file=lambda name: name.endswith(".md")),
}
