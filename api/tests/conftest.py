from pathlib import Path
from typing import BinaryIO, Generator

import pytest
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
def file() -> Generator:
    path = Path("/tmp") / DEFAULT_FILE.name

    # delete file if exists
    path.unlink(missing_ok=True)

    # create file and write data
    path.write_text(DEFAULT_FILE.content)

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
    if storage.exists(upload_file.filename):
        await storage.delete_file(upload_file.filename)
    await storage.save_file(upload_file)
