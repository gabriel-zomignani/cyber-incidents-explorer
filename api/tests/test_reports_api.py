import pytest

from api import server


@pytest.fixture
def client():
    with server.app.test_client() as c:
        yield c


def test_reports_list_returns_expected_shape(client):
    resp = client.get("/api/reports")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, dict)
    assert "reports" in data
    assert isinstance(data["reports"], list)
    if data["reports"]:
        first = data["reports"][0]
        for key in ["id", "fileName", "displayName", "description", "status"]:
            assert key in first
