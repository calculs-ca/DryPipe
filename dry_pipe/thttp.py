import gzip
import json as json_lib
import mimetypes
import secrets
import ssl
from base64 import b64encode
from collections import namedtuple
from http.cookiejar import CookieJar
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import (
    HTTPCookieProcessor,
    HTTPRedirectHandler,
    HTTPSHandler,
    Request,
    build_opener,
)

Response = namedtuple("Response", "request content json status url headers cookiejar")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def request(
    url,
    params={},
    json=None,
    data=None,
    headers={},
    method="GET",
    verify=True,
    redirect=True,
    cookiejar=None,
    basic_auth=None,
    timeout=None,
    files={},  # note: experimental
):
    """
    Returns a (named)tuple with the following properties:
        - request
        - content
        - json (dict; or None)
        - headers (dict; all lowercase keys)
            - https://stackoverflow.com/questions/5258977/are-http-headers-case-sensitive
        - status
        - url (final url, after any redirects)
        - cookiejar
    """
    method = method.upper()
    headers = {k.lower(): v for k, v in headers.items()}  # lowercase headers

    if params:
        url += "?" + urlencode(params)  # build URL from query parameters

    if json and data:
        raise Exception("Cannot provide both json and data parameters")

    if method not in ["POST", "PATCH", "PUT"] and (json or data):
        raise Exception("Request method must POST, PATCH or PUT if json or data is provided")

    if files and method != "POST":
        raise Exception("Request method must be POST when uploading files")

    if not timeout:
        timeout = 60

    if json:  # if we have json, dump it to a string and put it in our data variable
        headers["content-type"] = "application/json"
        data = json_lib.dumps(json).encode("utf-8")
    elif data and not isinstance(data, (str, bytes)):
        data = urlencode(data).encode()
    elif isinstance(data, str):
        data = data.encode()
    elif files:
        boundary = secrets.token_hex()

        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        data = b""

        for key, file in files.items():
            file_data = file.read()  # okay, we want this to stay as a byte-string
            if isinstance(file_data, str):
                file_data = file_data.encode("utf-8")
            fn = file.name

            mime, _ = mimetypes.guess_type(fn)
            if not mime:
                print("Using default mimetype")
                mime = "application/octet-stream"

            data += b"--" + boundary.encode() + b"\r\n"
            data += b'Content-Disposition: form-data; name="' + key.encode() + b'"; filename="' + fn.encode() + b'"\r\n'
            data += b"Content-Type: " + mime.encode() + b"\r\n\r\n"
            data += file_data + b"\r\n"
            data += b"--" + boundary.encode() + b"--\r\n"

        data = data
        headers["Content-Length"] = len(data)

    if basic_auth and len(basic_auth) == 2 and "authorization" not in headers:
        username, password = basic_auth
        headers["authorization"] = f'Basic {b64encode(f"{username}:{password}".encode()).decode("ascii")}'

    if not cookiejar:
        cookiejar = CookieJar()

    ctx = ssl.create_default_context()
    if not verify:  # ignore ssl errors
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    handlers = []
    handlers.append(HTTPSHandler(context=ctx))
    handlers.append(HTTPCookieProcessor(cookiejar=cookiejar))

    if not redirect:
        no_redirect = NoRedirect()
        handlers.append(no_redirect)

    opener = build_opener(*handlers)
    req = Request(url, data=data, headers=headers, method=method)

    try:
        with opener.open(req, timeout=timeout) as resp:
            status, content, resp_url = (resp.getcode(), resp.read(), resp.geturl())
            headers = {k.lower(): v for k, v in list(resp.info().items())}

            if "gzip" in headers.get("content-encoding", ""):
                content = gzip.decompress(content)

            json = (
                json_lib.loads(content)
                if "application/json" in headers.get("content-type", "").lower() and content
                else None
            )
    except HTTPError as e:
        status, content, resp_url = (e.code, e.read(), e.geturl())
        headers = {k.lower(): v for k, v in list(e.headers.items())}

        if "gzip" in headers.get("content-encoding", ""):
            content = gzip.decompress(content)

        json = (
            json_lib.loads(content)
            if "application/json" in headers.get("content-type", "").lower() and content
            else None
        )

    return Response(req, content, json, status, resp_url, headers, cookiejar)
