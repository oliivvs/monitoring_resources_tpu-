
"""Тесты подготовки Oracle Sync через EntityMapper."""

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from checks.security import NetworkPolicy
from integrations.tpu.entity_mapper import (
    EntityMapper,
    EntityMappingError,
    load_mapping,
)
from integrations.tpu.oracle_models import OracleSyncEvent
from integrations.tpu.oracle_prepare import (
    FullSnapshotRequired,
    OraclePreparationError,
    prepare_oracle_resource,
)
from services.source_sequence import MissingSourceSequence


MAPPING_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "tpu_oracle_mapping.yaml"
)


@pytest.fixture
def mapper():
    policy = NetworkPolicy(
        allowed_hosts=frozenset({"127.0.0.1"}),
        allowed_ports=frozenset({18080}),
        permit_loopback_for_tests=True,
    )

    return EntityMapper(
        load_mapping(MAPPING_PATH),
        policy,
    )


def complete_data():
    return {
        "name": "Тестовый сервер",
        "target": "http://127.0.0.1:18080/health",
        "check_type": "HTTP",
        "active": True,
        "port": None,
        "interval_seconds": 60,
        "timeout_seconds": 5.0,
        "expected_http_codes": [
            {"start": 200, "end": 299}
        ],
        "up_after": 2,
        "down_after": 3,
        "heartbeat_interval_seconds": 75,
    }


def make_event(
    *,
    operation_code=1,
    source="tpu-oracle-sync",
    entity="servers",
    record_id="server-17",
    source_sequence=100,
    data=None,
):
    return OracleSyncEvent(
        event_id=uuid4(),
        source=source,
        entity=entity,
        record_id=record_id,
        operation_code=operation_code,
        source_sequence=source_sequence,
        occurred_at=datetime.now(timezone.utc),
        data=complete_data() if data is None else data,
    )


@pytest.mark.asyncio
async def test_complete_insert(mapper):
    item = await prepare_oracle_resource(
        make_event(),
        mapper,
    )

    assert item.config.id == "tpu-oracle-server-17"
    assert item.config.name == "Тестовый сервер"
    assert item.config.active is True


@pytest.mark.asyncio
async def test_complete_update(mapper):
    item = await prepare_oracle_resource(
        make_event(
            operation_code=3,
            source_sequence=101,
        ),
        mapper,
    )

    assert item.config.interval_seconds == 60
    assert item.config.heartbeat_interval_seconds == 75


@pytest.mark.asyncio
async def test_tcp_update(mapper):
    data = complete_data()
    data["target"] = "127.0.0.1"
    data["check_type"] = "TCP"
    data["port"] = 18080
    data["expected_http_codes"] = []

    item = await prepare_oracle_resource(
        make_event(operation_code=3, data=data),
        mapper,
    )

    assert item.config.check_type == "TCP"
    assert item.config.port == 18080


@pytest.mark.asyncio
async def test_partial_update_rejected(mapper):
    with pytest.raises(FullSnapshotRequired):
        await prepare_oracle_resource(
            make_event(
                operation_code=3,
                data={
                    "name": "Новое название",
                    "active": True,
                },
            ),
            mapper,
        )


@pytest.mark.asyncio
async def test_mismatched_record_id(mapper):
    data = complete_data()
    data["source_record_id"] = "another-server"

    with pytest.raises(OraclePreparationError):
        await prepare_oracle_resource(
            make_event(data=data),
            mapper,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source,entity",
    [
        ("another-source", "servers"),
        ("tpu-oracle-sync", "other-entity"),
    ],
)
async def test_wrong_source_or_entity(
    mapper,
    source,
    entity,
):
    with pytest.raises(OraclePreparationError):
        await prepare_oracle_resource(
            make_event(
                source=source,
                entity=entity,
            ),
            mapper,
        )


@pytest.mark.asyncio
async def test_delete_uses_separate_handler(mapper):
    with pytest.raises(OraclePreparationError):
        await prepare_oracle_resource(
            make_event(operation_code=2, data={}),
            mapper,
        )


@pytest.mark.asyncio
async def test_disallowed_target(mapper):
    data = complete_data()
    data["target"] = "http://127.0.0.1:18082/health"

    with pytest.raises(EntityMappingError):
        await prepare_oracle_resource(
            make_event(data=data),
            mapper,
        )


@pytest.mark.asyncio
async def test_missing_sequence(mapper):
    with pytest.raises(MissingSourceSequence):
        await prepare_oracle_resource(
            make_event(source_sequence=None),
            mapper,
        )
