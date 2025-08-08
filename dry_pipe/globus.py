import json
import os
import time
from pathlib import Path
import logging

import requests

from dry_pipe.core_lib import exec_remote
from dry_pipe import DryPipe

module_logger = logging.getLogger(__name__)


class GlobusToken:

    def __init__(self, env_var_name=None, access_token=None, client_id=None):

        self.client_id = client_id
        self.file = None

        if access_token is not None:
            self.token = {
                "access_token": access_token
            }
        else:

            if env_var_name is None:
                raise Exception('env_var_name must be provided')

            f = os.environ.get(env_var_name)

            if f is None:
                raise Exception(f'env var {env_var_name} not set')

            if f.endswith(".json"):
                self.file = f
                with open(f) as globus_token_file:
                    self.token = json.load(globus_token_file)
            else:
                self.token = {
                    "access_token": f,
                }

    def bearer_token(self):
        return self.token['access_token']

    def create_auth_headers(self, add_app_json=True):
        h = {
            f"Authorization": f"Bearer {self.bearer_token()}"
        }
        if add_app_json:
            h["Content-Type"] = "application/json"

        return h

    def refresh(self):

        if self.client_id is None:
            raise Exception('client_id must be provided for token refresh')

        form_data = {
            "client_id": self.client_id,
            "refresh_token": self.token["refresh_token"],
            "grant_type": "refresh_token"
        }

        res = requests.post("https://auth.globus.org/v2/oauth2/token", data=form_data)

        if res.status_code != 200:
            raise RuntimeError(f"Globus Token Refresh Error: {res.status_code}")

        self.token = res.json()

    def save_to_file(self):
        if self.file is None:
            raise Exception('token was not created from file')

        with open(self.file, 'w') as globus_token_file:
            globus_token_file.write(json.dumps(self.token, indent=4))

class GlobusFileTransfer:

    def __init__(self, auth_token, source_endpoint, destination_endpoint):
        self.auth_token = auth_token
        self.source_endpoint = source_endpoint
        self.destination_endpoint = destination_endpoint


    def submit_file_transfer(self, file_tuples, log_file=None):

        def get_submission_id():
            return requests.get(
                'https://transfer.api.globusonline.org/v0.10/submission_id',
                headers=self.auth_token.create_auth_headers()
            )

        res = get_submission_id()

        if res.status_code != 200:

            if res.status_code == 401:
                self.auth_token.refresh()
                res = get_submission_id()

                if res.status_code == 200:
                    self.auth_token.save_to_file()
                else:
                    raise Exception(f"unexpected status code {res.status_code}")

        j = res.json()

        submission_id = j["value"]

        transfer_items = [
            {"DATA_TYPE": "transfer_item", "source_path": src, "destination_path": dst}
            for src, dst in file_tuples
        ]

        if log_file is not None:
            with open(log_file, "w") as u:
                json.dump(transfer_items, u, indent=4)

        transfer_res = requests.post(
            "https://transfer.api.globusonline.org/v0.10/transfer",
            data=json.dumps({
                "DATA_TYPE": "transfer",
                "submission_id": submission_id,
                "source_endpoint": self.source_endpoint,
                "destination_endpoint": self.destination_endpoint,
                "DATA": transfer_items
            }),
            headers=self.auth_token.create_auth_headers()
        )

        return GlobusFileTransferResponse(self, transfer_res.json())

class GlobusFileTransferResponse:

    def __init__(self, globus_file_transfer, transfer_response):
        self.globus_file_transfer = globus_file_transfer
        self.transfer_response = transfer_response

    def fetch_status(self):

        task_id = self.transfer_response["task_id"]

        res3 = requests.get(
            f'https://transfer.api.globusonline.org/v0.10/task/{task_id}',
            headers=self.globus_file_transfer.auth_token.create_auth_headers()
        )

        if res3.status_code != 200:
            raise Exception(f"unexpected status code {res3.status_code}")

        return res3.json()

    def spin_until_complete(self, logger=module_logger, sleep_schedule=[3, 5, 8, 10, 20, 30]):

        last_sleep = 30

        def f():
            res = self.fetch_status()
            if res["status"] == "SUCCEEDED":
                logger.info("globus upload completed")
                return True
            return False

        for s in sleep_schedule:
            last_sleep = s
            if f():
                return True

            logger.debug("globus transfer in progress will sleep for %s seconds", last_sleep)
            time.sleep(last_sleep)


        while True:
            if f():
                return True
            logger.debug("globus transfer in progress will sleep for %s seconds", last_sleep)
            time.sleep(last_sleep)




@DryPipe.python_call()
def upload_task_inputs_globus(
    __task_key,
    __task_control_dir,
    __remote_pipeline_specs,
    __task_logger,
    __task_process,
    __pipeline_instance_dir,
    __task_conf
):

    __task_logger.info("will generate file list for upload")

    tok = GlobusToken(env_var_name="DRYPIPE_GLOBUS_TOKEN")

    src_endpoint, dst_endpoint = __task_conf.globus_transfer.split(":")

    transfer = GlobusFileTransfer(tok, src_endpoint, dst_endpoint)

    def g():
        external_file_deps = []
        for f in __task_process.gen_internal_file_deps(external_file_deps):
            yield str(Path(__pipeline_instance_dir, f)), str(Path(__remote_pipeline_specs.remote_pid, f))

        for f in external_file_deps:
            yield f, str(Path(__remote_pipeline_specs.remote_pid, "external-file-deps", f))

        o_src = __remote_pipeline_specs.gen_override_file()

        yield o_src, str(Path(__remote_pipeline_specs.remote_control_dir, "task-conf-overrides.json"))

    transfer_response = transfer.submit_file_transfer(
        g(),
        log_file=
        Path(__task_control_dir, "globus-uploads.json")
        if __task_logger.isEnabledFor(logging.DEBUG)
        else None
    )

    transfer_response.spin_until_complete(__task_logger)




@DryPipe.python_call()
def download_task_outputs_globus(
    __task_key,
    __task_control_dir,
    __task_logger,
    __task_process,
    __pipeline_work_dir,
    __pipeline_instance_dir,
    __remote_pipeline_specs
):

    #fetch states, and generate rsync list
    remote_cli = os.path.join(__remote_pipeline_specs.remote_instance_work_dir, "cli")

    remote_exec_result = exec_remote(__remote_pipeline_specs.user_at_host, [
        "python3",
        remote_cli,
        "list-states",
        f"--task-key={__task_key}"
    ])

    __task_logger.debug("remote states:\n %s", remote_exec_result)

    if __task_process.is_slurm_array_parent():
        __remote_pipeline_specs.reconcile_local_array_states_with_remote_state(remote_exec_result, __pipeline_work_dir)


    __task_logger.info("will generate file list for upload")

    tok = GlobusToken(env_var_name="DRYPIPE_GLOBUS_TOKEN")

    src_endpoint, dst_endpoint = __task_process.task_conf.globus_transfer.split(":")

    transfer = GlobusFileTransfer(tok, dst_endpoint, src_endpoint)

    def g():
        for f in __remote_pipeline_specs.gen_result_files():
            yield str(Path(__remote_pipeline_specs.remote_pid, f)), str(Path(__pipeline_instance_dir, f))

    transfer_response = transfer.submit_file_transfer(
        g(),
        log_file=
            Path(__task_control_dir, "globus-downloads-1.json")
            if __task_logger.isEnabledFor(logging.DEBUG)
            else None
    )

    transfer_response.spin_until_complete(__task_logger)

    file_set_list = __task_process.file_sets_rsync_list_file()

    if os.path.exists(file_set_list) and os.stat(file_set_list).st_size > 0:

        def g2():
            with open(file_set_list) as _file_set_list:
                for f in _file_set_list.readlines():
                    f = f.strip()
                    yield str(Path(__remote_pipeline_specs.remote_pid, f)), str(Path(__pipeline_instance_dir, f))

        transfer_response = transfer.submit_file_transfer(
            g2(),
            log_file=
                Path(__task_control_dir, "globus-downloads-2.json")
                if __task_logger.isEnabledFor(logging.DEBUG)
                else None
        )

        transfer_response.spin_until_complete(__task_logger)