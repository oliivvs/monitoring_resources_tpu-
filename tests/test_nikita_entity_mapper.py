
"""N20: тестирование EntityMapper без реальных систем ТПУ."""

from pathlib import Path

import httpx
import pytest

from checks.security import NetworkPolicy
from integrations.tpu.entity_client import (
    EntityRoutes,
    TpuEntityClient,
)
from integrations.tpu.entity_mapper import (
    EntityMapper,
    EntityMappingError,
    MappingConfig,
    load_mapping,
)
from mock_entity_api.main import app


MAPPING_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "tpu_entity_mapping.yaml"
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


def record(**changes):
    data = {
        "source_record_id": "tpu-demo-001",
        "name": "Тестовый сервис",
        "target": "http://127.0.0.1:18080/health",
        "check_type": "HTTP",
        "active": True,
    }

    data.update(changes)
    return data


def test_mapping_http_and_defaults(mapper):
    item = mapper.map_record(record())

    assert item.source_record_id == "tpu-demo-001"
    assert item.config.id == "tpu-servers-tpu-demo-001"
    assert item.config.name == "Тестовый сервис"
    assert item.config.interval_seconds == 60
    assert item.config.heartbeat_interval_seconds == 75
    assert item.config.expected_http_codes[0].start == 200


def test_mapping_tcp_with_port():
    cfg = MappingConfig(
        source_system="mock",
        source_entity="servers",
        resource_id_prefix="lab-",
        fields={
            "source_record_id": "id",
            "name": "title",
            "target": "host",
            "check_type": "type",
            "active": "enabled",
            "port": "tcp_port",
        },
    )

    mapper = EntityMapper(cfg, NetworkPolicy())

    item = mapper.map_record({
        "id": "tcp-1",
        "title": "TCP service",
        "host": "127.0.0.1",
        "type": "TCP",
        "enabled": False,
        "tcp_port": 18081,
    })

    assert item.config.check_type == "TCP"
    assert item.config.port == 18081
    assert item.config.active is False
    assert item.config.expected_http_codes == []


def test_mapping_source_field_names_are_configurable():
    cfg = MappingConfig(
        source_system="mock",
        source_entity="asset",
        resource_id_prefix="asset-",
        fields={
            "source_record_id": "ext_id",
            "name": "label",
            "target": "endpoint",
            "check_type": "probe",
            "active": "enabled",
        },
    )

    mapper = EntityMapper(cfg, NetworkPolicy())

    item = mapper.map_record({
        "ext_id": "11",
        "label": "Site",
        "endpoint": "https://example.org",
        "probe": "HTTP",
        "enabled": True,
        "external_secret": "private-value",
    })

    assert item.config.id == "asset-11"
    assert item.ignored_fields == ("external_secret",)
    assert "private-value" not in repr(item.ignored_fields)


@pytest.mark.parametrize("invalid", [
    {"source_record_id": "../escape"},
    {"source_record_id": ""},
    {"active": "yes"},
    {"check_type": "ICMP"},
    {"target": "file:///etc/passwd"},
    {
        "target": "http://user:pass@127.0.0.1:18080/a",
        "port": None,
    },
    {"name": ""},
])
def test_invalid_record_rejected(mapper, invalid):
    with pytest.raises(EntityMappingError):
        mapper.map_record(record(**invalid))


def test_missing_required_rejected(mapper):
    bad = record()
    bad.pop("target")

    with pytest.raises(EntityMappingError):
        mapper.map_record(bad)


def test_duplicate_id_rejected(mapper):
    with pytest.raises(
        EntityMappingError,
        match="Повтор source_record_id",
    ):
        mapper.map_records([
            record(),
            record(name="Другая запись"),
        ])


def test_empty_snapshot_does_not_delete_anything(mapper):
    assert mapper.map_records([]) == []


def test_invalid_mapping_fields_rejected():
    with pytest.raises(ValueError):
        MappingConfig(
            source_system="mock",
            source_entity="server",
            resource_id_prefix="good-",
            fields={
                "source_record_id": "id",
                "name": "name",
            },
        )


def test_yaml_duplicate_keys_rejected(tmp_path):
    path = tmp_path / "mapping.yaml"

    path.write_text(
        "source_system: first\nsource_system: second\n"
    )

    with pytest.raises(EntityMappingError):
        load_mapping(path)


@pytest.mark.asyncio
async def test_allowlist_accepts_local_explicit_target(mapper):
    await mapper.validate_destination(
        mapper.map_record(record())
    )


@pytest.mark.asyncio
async def test_allowlist_rejects_unknown_port(mapper):
    bad = mapper.map_record(
        record(
            target="http://127.0.0.1:18082/health"
        )
    )

    with pytest.raises(
        EntityMappingError,
        match="сетевой политикой",
    ):
        await mapper.validate_destination(bad)


@pytest.mark.asyncio
async def test_default_policy_is_fail_closed():
    config = load_mapping(MAPPING_PATH)

    mapper = EntityMapper(
        config,
        NetworkPolicy(),
    )

    with pytest.raises(
        EntityMappingError,
        match="сетевой политикой",
    ):
        await mapper.validate_destination(
            mapper.map_record(record())
        )


@pytest.mark.asyncio
async def test_client_to_mapper_without_sqlite(mapper):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app)
    ) as transport:

        client = TpuEntityClient(
            base_url="http://127.0.0.1:18082",
            routes=EntityRoutes.local_mock(),
            http_client=transport,
        )

        page = await client.get_entity_records(
            "servers",
            limit=100,
        )

    mapped = mapper.map_records(page.items)

    assert len(mapped) == 3

    assert {
        item.source_record_id
        for item in mapped
    } == {
        "tpu-demo-001",
        "tpu-demo-002",
        "tpu-demo-003",
    }

    assert sum(
        item.config.active
        for item in mapped
    ) == 2

    for item in mapped:
        await mapper.validate_destination(item)
