import logging
import os
import traceback
import requests

from dry_pipe.core_lib import PortablePopen


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

def bash_shebang():
    return "#!/usr/bin/env bash"


def count_cpus():
    with PortablePopen(["grep", "-c", "^processor", "/proc/cpuinfo"]) as p:
        p.wait_and_raise_if_non_zero()
        return int(p.stdout_as_string())
