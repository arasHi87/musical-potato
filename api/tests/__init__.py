from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional

from app import APP
from fastapi.testclient import TestClient
from starlette.datastructures import URLPath


@dataclass
class RequestBody:
    url: URLPath
    body: Dict[str, Any]


@dataclass
class ResponseBody:
    status_code: int
    body: Dict[str, Any]


class AssertRequest:
    def __call__(
        self,
        tc: TestClient,
        method: str,
        req_body: RequestBody,
        resp_body: ResponseBody,
        assert_func: Callable = None,
        *args,
        **kwargs,
    ):
        url = APP.url_path_for(req_body.url)
        resp = tc.request(method, url, json=req_body.body)

        # If assert_func is not None, use assert_func to assert
        if assert_func is not None:
            assert_func(resp, resp_body, *args, **kwargs)
        else:
            assert resp.status_code == resp_body.status_code
            assert resp.json() == resp_body.body


assert_request = AssertRequest()
