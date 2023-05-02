from pathlib import Path
from typing import Generator

import pytest
from app import APP
from fastapi.testclient import TestClient
from tests import DEFAULT_FILE


@pytest.fixture(scope="module")
def client() -> Generator:
    with TestClient(APP) as tc:
        yield tc


@pytest.fixture(scope="module")
def file() -> Generator:
    path = Path("/tmp") / DEFAULT_FILE.name

    # delete file if exists
    path.unlink(missing_ok=True)

    # create file and write data
    path.write_text(DEFAULT_FILE.content)

    # yield file
    with open(path, "rb") as fp:
        yield fp
