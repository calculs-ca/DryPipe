import glob
from pathlib import Path


def timers_for_tasks(pipeline_instance_dir, glob_filter, include_steps=False):
    for task_log_file in glob.glob(str(Path(pipeline_instance_dir, ".drypipe", glob_filter, "drypipe.log"))):
        task_key = Path(task_log_file).parent.name
        for label, h_m_s, s in parse_timers_in_log(task_log_file):
            if not include_steps and label.startswith("STEP-"):
                continue
            yield task_key, label, h_m_s, s


def parse_timers_in_log(drypipe_log):

    with open(drypipe_log, "r") as log:
        for line in log:
            parts = line.split("TIME_ELAPSED_FOR:")
            if len(parts) < 2:
                continue
            label, p2 = parts[1].split(":", 1)
            time_h_m_s, time_s = p2.split(",")
            yield label, time_h_m_s.strip(), time_s.strip()
