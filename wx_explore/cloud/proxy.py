"""
This is a small shim wrapping the actual functions to make them (in theory) cloud/FaaS provider
agnostic.
"""
from typing import Mapping, Optional, Any

import base64
import os
import collections

class HttpRequest(object):
    args: Mapping[str, str]
    method: Optional[str]
    headers: Optional[Mapping[str, str]]
    body: Optional[bytes]

    def __init__(self, args, method=None, headers=None, body=None):
        self.args = args
        self.method = method
        self.headers = headers
        self.body = body


class HttpResponse(object):
    code: int
    headers: Mapping[str, str]
    body: bytes

    def __init__(self, body, code=200, headers=None):
        if type(body) is not bytes:
            body = bytes(str(body), 'utf-8')

        self.body = body
        self.code = code
        self.headers = headers if headers is not None else {}


def proxy(cb, *args):
    if 'WEBSITE_SITE_NAME' in os.environ:
        # Azure
        import azure.functions
        az_req: azure.functions.HttpRequest = args[0]
        req = HttpRequest(
            method=az_req.method,
            headers=az_req.headers,
            args=az_req.params,
            body=req.get_body()
        )

        resp = cb(req)

        az_resp = azure.functions.HttpResponse(
            status_code=resp.code,
            headers=resp.headers,
            body=resp.body,
        )

    elif 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
        # AWS Lambda (proxy function)
        event: Mapping[str, Any] = args[0]
        evInput = event['input']
        body = evInput['body']
        if evInput['isBase64Encoded']:
            body = base64.b64decode(body)

        req = HttpRequest(
            method=evInput['httpMethod'],
            headers=evInput['headers'],
            args=evInput['queryStringParameters'],
            body=body,
        )

        resp = cb(req)

        return {
            'statusCode': resp.code,
            'headers': resp.headers,
            'body': resp.body,
            # TODO: return isBase64Encoded if binary
        }

    elif '__OW_ACTION_NAME' in os.environ:
        # OpenWhisk (IBM cloud functions)
        params = args[0]

        req = HttpRequest(
            method=params.__ow_method,
            headers=params.__ow_headers,
            args=params,
        )

        resp = cb(req)

        return {
            'statusCode': resp.code,
            'headers': resp.headers,
            'body': resp.body,
            # TODO: base64 if necessary based on content type
        }

    elif 'FUNCTION_NAME' in os.environ:
        # Google Cloud Functions
        import flask
        gcf_req: flask.Request = args[0]

        req = HttpRequest(
            method=gcf_req.method,
            headers=gcf_req.headers,
            args=gcf_req.args,
            body=gcf_req.data,
        )

        resp = cb(req)

        return (resp.body, resp.code, resp.headers)
