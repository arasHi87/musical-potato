from pathlib import Path
from typing import BinaryIO, Generator

import pytest
import schemas
from app import APP
from fastapi import UploadFile
from fastapi.testclient import TestClient
from storage import storage
from tests import DEFAULT_FILE


@pytest.fixture(scope="module")
def client() -> Generator:
    with TestClient(APP) as tc:
        yield tc


@pytest.fixture(scope="function")
def file(request) -> Generator:
    # get file from marker if exists
    file: schemas.File = DEFAULT_FILE
    marker = request.node.get_closest_marker("file_data")
    if marker:
        file = marker.args[0]

    # create a file in tmp use to upload
    path = Path("/tmp") / file.name
    path.unlink(missing_ok=True)
    path.write_text(file.content)

    # yield file
    with open(path, "rb") as fp:
        yield fp


@pytest.fixture()
async def init_file(file: BinaryIO) -> None:
    # create a file to be used for testing duplicate
    upload_file = UploadFile(
        filename="m3ow87.txt", file=file, content_type="text/plain"
    )

    # delete file if exists to make sure we have a clean state
    if await storage.file_integrity(upload_file.filename):
        await storage.delete_file(upload_file.filename)
    await storage.create_file(upload_file)
