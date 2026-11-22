"""Тесты получения полного снимка Oracle Sync."""

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from integrations.tpu.entity_client import (
    EntityApiHttpError,
    EntityApiTransportError,
    EntityRoutes,
    TpuEntityClient,
)
from integrations.tpu.entity_mapper import load_mapping
from integrations.tpu.oracle_models import OracleSyncEvent
from integrations.tpu.oracle_snapshot import (
    OracleSnapshotContractError,
    OracleSnapshotNotReady,
    fetch_full_oracle_snapshot,
)


MAPPING = load_mapping(
    Path(__file__).resolve().parents[1]
    / "config"
    / "tpu_oracle_mapping.yaml"
)


def make_event(**changes):
    values = {
        "event_id": uuid4(),
        "source": "tpu-oracle-sync",
        "entity": "servers",
        "record_id": "server-17",
        "operation_code": 3,
        "source_sequence": 11,
        "occurred_at": datetime.now(timezone.utc),
        "data": {
            "name": "Обновлённый сервер",
        },
    }

    values.update(changes)

    return OracleSyncEvent(**values)


def full_record(**changes):
    record = {
        "source_record_id": "server-17",
        "source_sequence": 11,
        "name": "Обновлённый сервер",
        "target": "http://127.0.0.1:18080/health",
        "check_type": "HTTP",
        "active": True,
        "port": None,
        "interval_seconds": 120,
        "timeout_seconds": 5.0,
        "expected_http_codes": [
            {"start": 200, "end": 299}
        ],
        "up_after": 2,
        "down_after": 3,
        "heartbeat_interval_seconds": 75,
    }

    record.update(changes)
    return record


async def fetch_mock(
    payload,
    *,
    event=None,
    status_code=200,
):
    requests = []

    def handler(request):
        requests.append(request)

        return httpx.Response(
            status_code,
            json=payload,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
    ) as http_client:

        client = TpuEntityClient(
            base_url="http://127.0.0.1:8001",
            routes=EntityRoutes.local_mock(),
            http_client=http_client,
        )

        result = await fetch_full_oracle_snapshot(
            client,
            event or make_event(),
            MAPPING,
        )

    return result, requests


@pytest.mark.asyncio
async def test_full_snapshot_success():
    record, requests = await fetch_mock({
        "items": [full_record()],
        "next_cursor": None,
    })

    assert record["source_record_id"] == "server-17"
    assert record["source_sequence"] == 11
    assert record["name"] == "Обновлённый сервер"
    assert record["interval_seconds"] == 120

    assert len(requests) == 1

    request = requests[0]

    assert request.method == "GET"
    assert request.url.params["source_record_id"] == (
        "server-17"
    )
    assert request.url.params["limit"] == "2"


@pytest.mark.asyncio
async def test_snapshot_not_found():
    with pytest.raises(OracleSnapshotNotReady):
        await fetch_mock({
            "items": [],
            "next_cursor": None,
        })


@pytest.mark.asyncio
async def test_duplicate_records_rejected():
    with pytest.raises(OracleSnapshotContractError):
        await fetch_mock({
            "items": [
                full_record(),
                full_record(),
            ],
            "next_cursor": None,
        })


@pytest.mark.asyncio
async def test_paginated_result_not_accepted():
    with pytest.raises(OracleSnapshotNotReady):
        await fetch_mock({
            "items": [full_record()],
            "next_cursor": "another-page",
        })


@pytest.mark.asyncio
async def test_wrong_record_id_rejected():
    with pytest.raises(OracleSnapshotContractError):
        await fetch_mock({
            "items": [
                full_record(
                    source_record_id="another-server"
                )
            ],
            "next_cursor": None,
        })


@pytest.mark.asyncio
async def test_incomplete_snapshot_rejected():
    record = full_record()
    del record["timeout_seconds"]

    with pytest.raises(OracleSnapshotContractError):
        await fetch_mock({
            "items": [record],
            "next_cursor": None,
        })


@pytest.mark.asyncio
@pytest.mark.parametrize("sequence", [10, 12])
async def test_different_sequence_not_accepted(sequence):
    with pytest.raises(OracleSnapshotNotReady):
        await fetch_mock({
            "items": [
                full_record(source_sequence=sequence)
            ],
            "next_cursor": None,
        })


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sequence",
    [None, "11", True],
)
async def test_invalid_snapshot_sequence_rejected(sequence):
    with pytest.raises(OracleSnapshotContractError):
        await fetch_mock({
            "items": [
                full_record(source_sequence=sequence)
            ],
            "next_cursor": None,
        })


@pytest.mark.asyncio
async def test_wrong_operation_rejected():
    with pytest.raises(OracleSnapshotContractError):
        await fetch_mock(
            {"items": []},
            event=make_event(operation_code=1),
        )


@pytest.mark.asyncio
async def test_wrong_source_rejected():
    with pytest.raises(OracleSnapshotContractError):
        await fetch_mock(
            {"items": []},
            event=make_event(source="another-source"),
        )


@pytest.mark.asyncio
async def test_entity_api_http_error_propagated():
    with pytest.raises(EntityApiHttpError) as exc:
        await fetch_mock(
            {"error": "unavailable"},
            status_code=503,
        )

    assert exc.value.retryable is True


@pytest.mark.asyncio
async def test_entity_api_transport_error_propagated():
    def handler(request):
        raise httpx.ConnectError(
            "Connection failed",
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http_client:

        client = TpuEntityClient(
            base_url="http://127.0.0.1:8001",
            routes=EntityRoutes.local_mock(),
            http_client=http_client,
        )

        with pytest.raises(EntityApiTransportError):
            await fetch_full_oracle_snapshot(
                client,
                make_event(),
                MAPPING,
            )
