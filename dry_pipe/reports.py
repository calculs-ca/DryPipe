from datetime import datetime
from pathlib import Path

from dry_pipe.core_lib import format_seconds_to_hhmmss

# drypipe.log lines are formatted with "%(asctime)s - %(levelname)s - %(message)s" and
# datefmt='%Y-%m-%d %H:%M:%S%z', ex: "2026-08-20 15:00:22-0400 - INFO - START_TIMER_FOR:TASK"
LOG_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S%z"
LOG_TIMESTAMP_LENGTH = 24

# label suffix for a timer that was started but never ended, see parse_timers_in_log
PARTIAL_SUFFIX = "-PARTIAL"


def timers_for_tasks(pipeline_instance_dir, task_keys, include_steps=False):
    """
    task_keys is the already filtered selection of tasks (see Cli.filter_key_state_step), tasks
    that have no drypipe.log yet (never launched) are silently skipped
    """
    for task_key in task_keys:
        task_log_file = Path(pipeline_instance_dir, ".drypipe", task_key, "drypipe.log")
        if not task_log_file.exists():
            continue
        for label, h_m_s, s in parse_timers_in_log(task_log_file):
            if not include_steps and label.startswith("STEP-"):
                continue
            yield task_key, label, h_m_s, s


def _log_line_timestamp(line):
    try:
        return datetime.strptime(line[:LOG_TIMESTAMP_LENGTH], LOG_TIMESTAMP_FORMAT)
    except ValueError:
        # continuation lines of multi line messages (stack traces, etc) carry no timestamp
        return None


def parse_timers_in_log(drypipe_log):
    """
    yields (label, hh:mm:ss, seconds) for each timer in the log.

    A task that doesn't end on its own terms (timed-out, killed, or crashed) never logs its
    TIME_ELAPSED_FOR: the signal handlers exit with os._exit(), which skips the TimeLogger
    context manager's __exit__, and a hard crash gets no chance to log anything at all. The
    time such a task ran is still worth reporting (it's what tells you the timeout to ask for
    next), so it is derived from the log's own timestamps, from the timer's start to the last
    line logged, and the label is suffixed with "-PARTIAL": the task ran AT LEAST that long.
    A task still running gets the same treatment, for the same reason.
    """

    started_at_per_label = {}
    last_timestamp = None

    def partial(label, started_at, ended_at):
        seconds = (ended_at - started_at).total_seconds()
        return (
            f"{label}{PARTIAL_SUFFIX}",
            format_seconds_to_hhmmss(round(seconds)),
            str(round(seconds, 2))
        )

    with open(drypipe_log, "r") as log:
        c = 0
        for line in log:
            c += 1

            timestamp = _log_line_timestamp(line)
            if timestamp is not None:
                last_timestamp = timestamp

            parts = line.split("START_TIMER_FOR:")
            if len(parts) >= 2:
                label = parts[1].strip()
                # the log is appended to across restarts, so a second start for a label means
                # the previous run of it never ended, report it before it is forgotten
                previously_started_at = started_at_per_label.get(label)
                if previously_started_at is not None and timestamp is not None:
                    yield partial(label, previously_started_at, timestamp)
                started_at_per_label[label] = timestamp
                continue

            try:
                parts = line.split("TIME_ELAPSED_FOR:")
                if len(parts) < 2:
                    continue
                label, p2 = parts[1].split(":", 1)
                time_h_m_s, time_s = p2.split(",")
                started_at_per_label.pop(label.strip(), None)
                yield label, time_h_m_s.strip(), time_s.strip()
            except Exception:
                raise Exception(f"Error parsing line : {c} in log {drypipe_log}")

    for label, started_at in started_at_per_label.items():
        if started_at is not None and last_timestamp is not None:
            yield partial(label, started_at, last_timestamp)


if __name__ == "__main__":

    for t in parse_timers_in_log(Path("/home/maxl/dev/OpenProt/drypipe.log")):
        print(f"{t[0]}\t{t[1]}\t{t[2]}")
