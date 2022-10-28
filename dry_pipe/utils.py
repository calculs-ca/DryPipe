import logging
import os
import traceback
from contextlib import contextmanager
from itertools import groupby
from timeit import default_timer
import requests

from dry_pipe.script_lib import PortablePopen


def send_email_error_report_if_configured(subject_line, exception=None, details=None):

    logger = logging.getLogger()

    error_email_config = os.environ.get("DRYPIPE_ERROR_EMAIL")

    if error_email_config is None:
        logger.info("DRYPIPE_ERROR_EMAIL not set")
        return

    mailgun_adresseses, mailgun_domain, mailgun_private_api_key, instance_label = error_email_config.split(":")

    logger.info("will send error report to %s with domain %s", mailgun_adresseses, mailgun_domain)

    if exception is not None:
        text = exception_to_string(exception)
    elif details is not None:
        text = details
    else:
        text = subject_line

    data = {
        "from": f"drypipe@{mailgun_domain}",
        "to": mailgun_adresseses.split(","),
        "subject": f"{subject_line}, {instance_label}",
        "text": f"""
            {text}
            INSTANCE: {instance_label}        
        """
    }

    requests.post(
        'https://api.mailgun.net/v2/{}/messages'.format(mailgun_domain),
        data=data,
        auth=('api', mailgun_private_api_key)
    )


def exception_to_string(exception):
    return "".join(traceback.TracebackException.from_exception(exception).format())


@contextmanager
def elapsed_timer():
    start = default_timer()
    elapser = lambda: default_timer() - start
    yield lambda: elapser()


logger_perf = logging.getLogger("dry_pipe.perf")

#   logger_perf.setLevel(logging.DEBUG)
#   logger_perf.addHandler(logging.StreamHandler(sys.stdout))

@contextmanager
def perf_logger_timer(group_label, details="", rounding_decimals=3, logger=logger_perf):

    if logger.level != logging.DEBUG:
        yield ""
        return

    start = default_timer()
    try:
        yield ""
    finally:
        logger.debug(
            "%s\t%s\t%s",
            group_label,
            round(default_timer() - start, rounding_decimals),
            details
        )

def analyze_perf_log(log_file, rounding_decimals=2):

    def enum_rows():
        with open(log_file) as f:
            for line in f.readlines():
                yield line.split("\t")

    def call_label(row):
        return row[0]

    def time_taken(row):
        return float(row[1])

    def gen_stats():
        for label, rows in groupby(sorted(enum_rows(), key=call_label), key=call_label):
            durations = list(map(time_taken, rows))
            s = sum(durations)
            avg = s / len(durations)
            yield round(avg, rounding_decimals), round(s, rounding_decimals), label

    for stat in gen_stats():
        stat = [
            str(s) for s in stat
        ]
        print("\t".join(stat))


def bash_shebang():
    return "#!/usr/bin/env bash"


def count_cpus():
    with PortablePopen(["grep", "-c", "^processor", "/proc/cpuinfo"]) as p:
        p.wait_and_raise_if_non_zero()
        return int(p.stdout_as_string())
