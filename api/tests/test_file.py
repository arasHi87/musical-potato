from typing import BinaryIO

from fastapi import UploadFile
from fastapi.testclient import TestClient
from storage import storage
from tests import DEFAULT_FILE, RequestBody, ResponseBody, assert_request

"""
Test cases for create file endpoints
@name file:create_file
@router post /file/
@status_code 201
@response_model schemas.File
"""


async def test_create_file_success(client: TestClient, file: BinaryIO) -> None:
    req = RequestBody(
        url="file:create_file",
        body=None,
        files={"file": ("m3ow87.txt", file, "text/plain")},
    )
    resp = ResponseBody(status_code=201, body=DEFAULT_FILE.dict())
    await assert_request(client, "post", req, resp)


async def test_create_file_duplicate(client: TestClient, file: BinaryIO) -> None:
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
    await assert_request(client, "post", req, resp)
