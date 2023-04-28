from fastapi.testclient import TestClient
from tests import RequestBody, ResponseBody, assert_request


def test_health(client: TestClient) -> None:
    req = RequestBody(url="health:get_health", body=None)
    resp = ResponseBody(status_code=200, body={"detail": "Service healthy"})
    assert_request(client, "get", req, resp)
