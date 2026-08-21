from pathlib import Path


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


def parse_timers_in_log(drypipe_log):
    """
    yields (label, hh:mm:ss, seconds) for every timer in the log.

    Tasks that are timed out or killed log their TIME_ELAPSED_FOR like any other, from the
    signal handlers, see TaskProcess._log_elapsed_for_active_time_loggers. A task that dies
    without its handlers running (SIGKILL, node failure) logs none, and is absent from the
    report: there is no elapsed time to read anywhere.
    """

    with open(drypipe_log, "r") as log:
        c = 0
        for line in log:
            c += 1
            try:
                parts = line.split("TIME_ELAPSED_FOR:")
                if len(parts) < 2:
                    continue
                label, p2 = parts[1].split(":", 1)
                time_h_m_s, time_s = p2.split(",")
                yield label, time_h_m_s.strip(), time_s.strip()
            except Exception:
                raise Exception(f"Error parsing line : {c} in log {drypipe_log}")


if __name__ == "__main__":

    for t in parse_timers_in_log(Path("/home/maxl/dev/OpenProt/drypipe.log")):
        print(f"{t[0]}\t{t[1]}\t{t[2]}")
