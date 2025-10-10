import json
import os
import time
from pathlib import Path
import logging

from dry_pipe.thttp import request

from dry_pipe.core_lib import exec_remote, SleepySpinner
from dry_pipe import DryPipe

module_logger = logging.getLogger(__name__)


class GlobusToken:

    def __init__(self, env_var_name=None, access_token=None, client_id=None, access_token_file=None):

        self.client_id = client_id
        self.file = access_token_file

        if access_token is not None:
            self.token = {
                "access_token": access_token
            }
        else:
            if self.file is None:
                if env_var_name is None:
                    raise Exception('env_var_name must be provided')

                self.file = os.environ.get(env_var_name)

                if self.file is None:
                    raise Exception(f'env var {env_var_name} not set')

            if not Path(self.file).exists():
                raise Exception(f"file {self.file} does not  exists")

            with open(self.file) as globus_token_file:
                self.token = json.load(globus_token_file)


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

        res = request("https://auth.globus.org/v2/oauth2/token", data=form_data, method="POST")

        if res.status != 200:
            raise RuntimeError(f"Globus Token Refresh Error: {res.status}")

        self.token = res.json

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
            return request(
                'https://transfer.api.globusonline.org/v0.10/submission_id',
                headers=self.auth_token.create_auth_headers()
            )

        res = get_submission_id()

        if res.status != 200:

            if res.status == 401:
                self.auth_token.refresh()
                res = get_submission_id()

                if res.status == 200:
                    self.auth_token.save_to_file()
                else:
                    raise Exception(f"unexpected status code {res.status}")

        j = res.json

        submission_id = j["value"]

        transfer_items = [
            {"DATA_TYPE": "transfer_item", "source_path": src, "destination_path": dst}
            for src, dst in file_tuples
        ]

        if log_file is not None:
            with open(log_file, "w") as u:
                json.dump(transfer_items, u, indent=4)

        transfer_res = request(
            "https://transfer.api.globusonline.org/v0.10/transfer",
            method="POST",
            data=json.dumps({
                "DATA_TYPE": "transfer",
                "submission_id": submission_id,
                "source_endpoint": self.source_endpoint,
                "destination_endpoint": self.destination_endpoint,
                "DATA": transfer_items
            }),
            headers=self.auth_token.create_auth_headers()
        )

        if transfer_res.status != 200:
            raise Exception(f"unexpected status code {transfer_res.status}")

        return GlobusFileTransferResponse(self, transfer_res.json)

class GlobusFileTransferResponse:

    def __init__(self, globus_file_transfer, transfer_response):
        self.globus_file_transfer = globus_file_transfer
        self.transfer_response = transfer_response

    def fetch_status(self):

        task_id = self.transfer_response["task_id"]

        res3 = request(
            f'https://transfer.api.globusonline.org/v0.10/task/{task_id}',
            headers=self.globus_file_transfer.auth_token.create_auth_headers()
        )

        if res3.status != 200:
            raise Exception(f"unexpected status code {res3.status}")

        return res3.json

    def spin_until_complete(self, logger=module_logger, sleep_schedule=[3, 5, 8, 10, 20, 30]):
        with SleepySpinner(sleep_schedule) as ss:
            while True:
                res = self.fetch_status()
                if res["status"] == "SUCCEEDED":
                    logger.info("globus upload completed")
                    return True
                next_sleep = ss.next_sleep()
                logger.debug("globus transfer in progress will sleep for %s seconds", next_sleep)
                ss.sleep()

