"""Проверки учебного Mock Entity API."""

import pytest

from fastapi.testclient import TestClient

from mock_api.oracle_snapshot import create_snapshot_app


PATH = "/mock/tpu/entity/v1/entities/servers/records"
RECORD_ID = "docker-lab-snapshot-001"
TOKEN = "lab-test-token"


def create_client():
    return TestClient(
        create_snapshot_app(
            token=TOKEN,
            record_id=RECORD_ID,
        )
    )


@pytest.mark.parametrize(
    "authorization",
    [None, "Bearer invalid-token"],
)
def test_requires_valid_token(authorization):
    with create_client() as client:
        headers = (
            {"Authorization": authorization}
            if authorization is not None
            else {}
        )

        response = client.get(
            PATH,
            headers=headers,
            params={"source_record_id": RECORD_ID},
        )

        assert response.status_code == 401


def test_returns_full_snapshot():
    with create_client() as client:
        response = client.get(
            PATH,
            headers={"Authorization": f"Bearer {TOKEN}"},
            params={
                "source_record_id": RECORD_ID,
                "limit": 2,
            },
        )

    assert response.status_code == 200

    body = response.json()

    assert body["next_cursor"] is None
    assert len(body["items"]) == 1

    record = body["items"][0]

    assert record["source_record_id"] == RECORD_ID
    assert record["source_sequence"] == 2
    assert record["interval_seconds"] == 120
    assert record["name"] == "Обновлённый сервер Docker"


def test_filters_records_and_entities():
    with create_client() as client:
        headers = {
            "Authorization": f"Bearer {TOKEN}"
        }

        missing = client.get(
            PATH,
            headers=headers,
            params={"source_record_id": "unknown"},
        )

        unsupported = client.get(
            "/mock/tpu/entity/v1/entities/other/records",
            headers=headers,
        )

    assert missing.status_code == 200
    assert missing.json()["items"] == []
    assert unsupported.status_code == 404


def test_disabled_without_token():
    app = create_snapshot_app(
        token="",
        record_id=RECORD_ID,
    )

    with TestClient(app) as client:
        response = client.get(
            PATH,
            params={"source_record_id": RECORD_ID},
        )

    assert response.status_code == 503
