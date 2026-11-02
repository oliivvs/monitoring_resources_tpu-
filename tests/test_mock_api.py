"""Тесты Mock API для приёма событий."""

import pytest
from fastapi.testclient import TestClient

from app.config import AppSettings
from mock_api.main import create_mock_app


EVENT = {
    "event_id": "7d51a70e-6bea-4d1c-9805-d0157ddc0d90",
    "resource_id": "server-001",
    "resource_name": "Test Server",
    "status": "UP",
    "check_type": "HTTP",
    "checked_at": "2026-11-02T08:00:00Z",
    "response_time_ms": 12.5,
    "error": None,
    "event_type": "STATUS_CHANGED",
}

HEADERS = {"Authorization": "Bearer test-secret-token"}


@pytest.fixture
def client():
    settings = AppSettings(
        _env_file=None,
        event_token="test-secret-token",
    )

    with TestClient(create_mock_app(settings)) as test_client:
        yield test_client


def test_mock_api_accepts_event(client):
    response = client.post(
        "/api/v1/status/events",
        json=EVENT,
        headers=HEADERS,
    )

    assert response.status_code == 202
    assert response.json() == {
        "event_id": EVENT["event_id"],
        "result": "accepted",
    }


def test_duplicate_event_is_idempotent(client):
    first = client.post(
        "/api/v1/status/events", json=EVENT, headers=HEADERS
    )
    second = client.post(
        "/api/v1/status/events", json=EVENT, headers=HEADERS
    )

    assert first.status_code == 202
    assert second.status_code == 200
    assert second.json()["result"] == "duplicate"


def test_same_id_with_different_data_rejected(client):
    client.post(
        "/api/v1/status/events", json=EVENT, headers=HEADERS
    )

    changed = {**EVENT, "status": "DOWN"}

    response = client.post(
        "/api/v1/status/events", json=changed, headers=HEADERS
    )

    assert response.status_code == 409


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer wrong-token"},
    ],
)
def test_unauthorized_request_rejected(client, headers):
    response = client.post(
        "/api/v1/status/events",
        json=EVENT,
        headers=headers,
    )

    assert response.status_code == 401


def test_invalid_json_event_rejected(client):
    invalid = {
        key: value for key, value in EVENT.items()
        if key != "resource_id"
    }

    response = client.post(
        "/api/v1/status/events",
        json=invalid,
        headers=HEADERS,
    )

    assert response.status_code == 422


def test_missing_server_token_rejected():
    settings = AppSettings(
        _env_file=None,
        event_token=None,
    )

    with TestClient(create_mock_app(settings)) as client:
        response = client.post(
            "/api/v1/status/events",
            json=EVENT,
            headers=HEADERS,
        )

    assert response.status_code == 503


def test_mock_api_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
