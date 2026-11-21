"""N21: тесты полного импорта Entity API в SQLite без реальной сети ТПУ."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text

from checks.security import NetworkPolicy
from db.session import initialize, make_engine, read_transaction
from integrations.tpu.entity_client import (
    EntityApiHttpError, EntityRoutes, TpuEntityClient,
)
from integrations.tpu.entity_mapper import (
    EntityMapper, EntityMappingError, load_mapping,
)
from integrations.tpu.entity_sync import EntitySyncError, sync_entity_resources
from mock_entity_api.main import app


MAPPING = Path(__file__).resolve().parents[1] / "config/tpu_entity_mapping.yaml"


@pytest.fixture
def engine(tmp_path):
    db = make_engine(tmp_path / "n21.sqlite")
    initialize(db)
    yield db
    db.dispose()


@pytest.fixture
def mapper():
    return EntityMapper(
        load_mapping(MAPPING),
        NetworkPolicy(
            allowed_hosts=frozenset({"127.0.0.1"}),
            allowed_ports=frozenset({18080}),
            permit_loopback_for_tests=True,
        ),
    )


def count(engine, table: str) -> int:
    assert table in {"resources", "resource_config_versions", "check_results"}
    with read_transaction(engine) as conn:
        return conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def record(resource_id: str, **changes):
    data = {
        "source_record_id": resource_id,
        "name": "Сервис " + resource_id,
        "target": "http://127.0.0.1:18080/health",
        "check_type": "HTTP",
        "active": True,
    }
    data.update(changes)
    return data


@asynccontextmanager
async def mock_client(handler=None):
    transport = (
        httpx.MockTransport(handler)
        if handler is not None
        else httpx.ASGITransport(app=app)
    )
    async with httpx.AsyncClient(transport=transport) as http_client:
        yield TpuEntityClient(
            base_url="http://127.0.0.1:18082",
            routes=EntityRoutes.local_mock(),
            http_client=http_client,
        )


def one_page(records, *, next_cursor=None):
    def handler(request):
        return httpx.Response(
            200,
            json={"items": records, "next_cursor": next_cursor},
        )
    return handler


@pytest.mark.asyncio
async def test_first_import_and_repeat_are_idempotent(engine, mapper):
    async with mock_client() as client:
        first = await sync_entity_resources(engine, client, mapper)
        assert first.total == 3
        assert first.count("CREATED") == 3
        assert count(engine, "resources") == 3
        assert count(engine, "resource_config_versions") == 3
        assert count(engine, "check_results") == 0