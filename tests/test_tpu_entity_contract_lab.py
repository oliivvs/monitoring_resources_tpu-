"""A25.4: лабораторный Entity API с другим маршрутом."""

from pathlib import Path

from fastapi.testclient import TestClient

from integrations.tpu.entity_contract import load_contract
from integrations.tpu.entity_mapper import load_mapping
from mock_api.oracle_snapshot import create_snapshot_app
from scripts.run_inbox_worker import (
    OracleWorkerSettings,
    build_snapshot_client,
)


ROOT = Path(__file__).resolve().parents[1]

CUSTOM_ROUTE = (
    "/lab/tpu/entity/v2/entities/servers/records"
)

DEFAULT_ROUTE = (
    "/mock/tpu/entity/v1/entities/servers/records"
)


def make_app():
    return create_snapshot_app(
        token="lab-test-token",
        record_id="lab-server-001",
        source_sequence=7,
        interval_seconds=90,
    )


def fetch(client, path, token="lab-test-token"):
    headers = {}

    if token is not None:
        headers["Authorization"] = f"Bearer {token}"

    return client.get(
        path,
        params={
            "source_record_id": "lab-server-001",
            "limit": 2,
        },
        headers=headers,
    )


def test_custom_lab_route_returns_snapshot():
    with TestClient(make_app()) as client:
        response = fetch(client, CUSTOM_ROUTE)

    assert response.status_code == 200

    record = response.json()["items"][0]

    assert record["source_record_id"] == "lab-server-001"
    assert record["source_sequence"] == 7
    assert record["interval_seconds"] == 90


def test_original_lab_route_still_works():
    with TestClient(make_app()) as client:
        response = fetch(client, DEFAULT_ROUTE)

    assert response.status_code == 200
    assert len(response.json()["items"]) == 1


def test_custom_route_requires_authorization():
    with TestClient(make_app()) as client:
        response = fetch(
            client,
            CUSTOM_ROUTE,
            token=None,
        )

    assert response.status_code == 401


def test_worker_reads_lab_contract():
    contract_path = (
        ROOT / "config/tpu_entity_contract_lab.yaml"
    )

    contract = load_contract(contract_path)
    mapping = load_mapping(
        ROOT / "config/tpu_oracle_mapping.yaml"
    )

    assert contract["source_entity"] == mapping.source_entity

    settings = OracleWorkerSettings(
        _env_file=None,
        snapshot_enabled=True,
        snapshot_base_url="https://oracle-snapshot",
        snapshot_allowed_hosts="oracle-snapshot",
        snapshot_allowed_cidrs="172.16.0.0/12",
        snapshot_token="lab-test-token",
        snapshot_contract_path=contract_path,
    )

    client = build_snapshot_client(
        settings,
        mapping=mapping,
    )

    assert client._routes.records == (
        contract["records_path"]
    )
