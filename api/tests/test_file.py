from typing import BinaryIO

import pytest
from fastapi import UploadFile
from storage import storage
from tests import DEFAULT_FILE, RequestBody, ResponseBody, assert_request

"""
Test cases for create file endpoints
@name file:create_file
@router post /file/
@status_code 201
@response_model schemas.File
"""


async def test_create_file_success(file: BinaryIO) -> None:
    req = RequestBody(
        url="file:create_file",
        body=None,
        files={"file": ("m3ow87.txt", file, "text/plain")},
    )
    resp = ResponseBody(status_code=201, body=DEFAULT_FILE.dict())
    await assert_request("post", req, resp)


async def test_create_file_duplicate(file: BinaryIO) -> None:
    # create a file to be used for testing duplicate
    upload_file = UploadFile(
        filename="m3ow87.txt", file=file, content_type="text/plain"
    )
    await storage.delete_file(upload_file.filename)
    await storage.save_file(upload_file)

    req = RequestBody(
        url="file:create_file",
        body=None,
        files={"file": ("m3ow87.txt", file, "text/plain")},
    )
    resp = ResponseBody(status_code=409, body={"detail": "File already exists"})
    await assert_request("post", req, resp)


"""
Test case for read file endpoint
@name file:read_file
@router get /file/
@status_code 200
@response_model str
"""


@pytest.mark.usefixtures("init_file")
async def test_read_file_success() -> None:
    req = RequestBody(
        url="file:read_file", body=None, params={"filename": DEFAULT_FILE.name}
    )
    resp = ResponseBody(status_code=200, body=DEFAULT_FILE.content)
    await assert_request("get", req, resp)


async def test_read_file_none_exists() -> None:
    req = RequestBody(
        url="file:read_file", body=None, params={"filename": "non-exists.txt"}
    )
    resp = ResponseBody(status_code=404, body={"detail": "File not found"})
    await assert_request("get", req, resp)
