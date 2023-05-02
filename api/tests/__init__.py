from dataclasses import dataclass
from typing import Any, BinaryIO, Callable, Dict, Tuple, Union

import schemas
from app import APP
from fastapi.testclient import TestClient
from starlette.datastructures import URLPath

DEFAULT_FILE = schemas.File(
    name="m3ow87.txt",
    size=26,
    checksum="d44d11c472f88a15737ae8eee2247231",
    content="Do U Want To Meow With Me?",
    content_type="text/plain",
)


@dataclass
class RequestBody:
    url: URLPath
    body: Dict[str, Any]
    files: Union[Dict[str, Tuple[str, BinaryIO]], None] = None


@dataclass
class ResponseBody:
    status_code: int
    body: Dict[str, Any]


class AssertRequest:
    async def __call__(
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
        resp = tc.request(method, url, json=req_body.body, files=req_body.files)

        # If assert_func is not None, use assert_func to assert
        if assert_func is not None:
            assert_func(resp, resp_body, *args, **kwargs)
        else:
            assert resp.status_code == resp_body.status_code
            assert resp.json() == resp_body.body


assert_request = AssertRequest()
