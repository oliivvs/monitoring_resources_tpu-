
"""A22.6: POST -> Inbox -> Worker -> ресурс SQLite."""

from __future__ import annotations

import asyncio

from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from uuid import uuid4

import pytest

from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.config import AppSettings
from app.database import get_engine
from app.main import app
from app.security_tokens import get_settings

from checks.security import NetworkPolicy
from db.session import (
    initialize,
    make_engine,
    read_transaction,
)

from integrations.tpu.entity_mapper import (
    EntityMapper,
    load_mapping,
)
from integrations.tpu.oracle_inbox_handler import (
    OracleInboxHandler,
)

from services.inbox_loop import InboxLoop
from services.inbox_worker import (
    process_one_inbox_event_async,
)


SYNC_TOKEN = "test-oracle-sync-token"
API_PATH = "/api/v1/integrations/tpu/resource-events"

MAPPING_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "tpu_oracle_mapping.yaml"
)


@pytest.fixture
def environment(tmp_path):
    engine = make_engine(tmp_path / "pipeline.sqlite")
    initialize(engine)

    for name in (
        "003_tpu_inbox",
        "004_inbox_worker",
        "005_source_sequence",
    ):
        migration = import_module(
            f"migrations.versions.{name}"
        )

        with engine.begin() as conn:
            context = MigrationContext.configure(conn)

            with Operations.context(context):
                migration.upgrade()

    mapper = EntityMapper(
        load_mapping(MAPPING_PATH),
        NetworkPolicy(
            allowed_hosts=frozenset({"127.0.0.1"}),
            allowed_ports=frozenset({18080}),
            permit_loopback_for_tests=True,
        ),
    )

    bridge = OracleInboxHandler(mapper)

    app.dependency_overrides[get_engine] = lambda: engine
    app.dependency_overrides[get_settings] = (
        lambda: AppSettings(
            _env_file=None,
            tpu_sync_token=SYNC_TOKEN,
        )
    )

    try:
        with TestClient(app) as client:
            yield engine, client, bridge
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


def full_data(**changes):
    record = {
        "name": "Сервер ТПУ",
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

    record.update(changes)
    return record


def send(
    client,
    sequence,
    *,
    operation=1,
    data=None,
):
    payload = {
        "event_id": str(uuid4()),
        "source": "tpu-oracle-sync",
        "entity": "servers",
        "record_id": "server-17",
        "operation_code": operation,
        "source_sequence": sequence,
        "occurred_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "data": (
            full_data()
            if data is None
            else data
        ),
    }

    response = client.post(
        API_PATH,
        json=payload,
        headers={
            "Authorization": f"Bearer {SYNC_TOKEN}"
        },
    )

    assert response.status_code == 202, response.text
    return payload["event_id"]


def resource_state(engine):
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT
                    r.name,
                    r.active,
                    c.version_number,
                    c.interval_seconds
                FROM resources AS r
                JOIN resource_states AS s
                  ON s.resource_id = r.id
                JOIN resource_config_versions AS c
                  ON c.id = s.config_version_id
                WHERE r.id = 'tpu-oracle-server-17'
            """)
        ).mappings().one_or_none()


def event_status(engine, event_id):
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT
                    processing_status,
                    attempts,
                    claim_token,
                    last_error
                FROM inbox_events
                WHERE event_id=:id
            """),
            {"id": event_id},
        ).mappings().one()


def last_sequence(engine):
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT last_sequence
                FROM resource_source_sequences
                WHERE source_system='tpu-oracle-sync'
                  AND source_entity='servers'
                  AND source_record_id='server-17'
            """)
        ).scalar_one_or_none()


async def process(engine, bridge, **kwargs):
    return await process_one_inbox_event_async(
        engine,
        bridge.prepare,
        bridge.apply,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_api_insert_creates_resource(environment):
    engine, client, bridge = environment

    event_id = send(client, 10)

    assert event_status(
        engine, event_id
    )["processing_status"] == "PENDING"

    result = await process(engine, bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["name"] == "Сервер ТПУ"
    assert resource_state(engine)["active"] == 1
    assert last_sequence(engine) == 10
    assert event_status(
        engine, event_id
    )["processing_status"] == "PROCESSED"


@pytest.mark.asyncio
async def test_api_update_changes_resource(environment):
    engine, client, bridge = environment

    send(client, 10)
    await process(engine, bridge)

    event_id = send(
        client,
        11,
        operation=3,
        data=full_data(
            name="Обновлённый сервер",
            interval_seconds=120,
        ),
    )

    result = await process(engine, bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["name"] == (
        "Обновлённый сервер"
    )
    assert resource_state(engine)["interval_seconds"] == 120
    assert resource_state(engine)["version_number"] == 2
    assert last_sequence(engine) == 11
    assert event_status(
        engine, event_id
    )["processing_status"] == "PROCESSED"


@pytest.mark.asyncio
async def test_api_delete_and_old_event(environment):
    engine, client, bridge = environment

    send(client, 10)
    await process(engine, bridge)

    delete_id = send(
        client,
        12,
        operation=2,
        data={},
    )

    result = await process(engine, bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["active"] == 0
    assert last_sequence(engine) == 12
    assert event_status(
        engine, delete_id
    )["processing_status"] == "PROCESSED"

    # Старое событие не должно включить ресурс.
    send(client, 11, operation=1)
    result = await process(engine, bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["active"] == 0
    assert last_sequence(engine) == 12

    # Только более новый INSERT может его включить.
    send(client, 13, operation=1)
    result = await process(engine, bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["active"] == 1
    assert last_sequence(engine) == 13


@pytest.mark.asyncio
async def test_partial_update_retries_without_changes(
    environment
):
    engine, client, bridge = environment

    send(client, 10)
    await process(engine, bridge)

    event_id = send(
        client,
        11,
        operation=3,
        data={"name": "Неполное обновление"},
    )

    result = await process(engine, bridge)

    assert result.disposition == "RETRY"
    assert resource_state(engine)["name"] == "Сервер ТПУ"
    assert last_sequence(engine) == 10

    status = event_status(engine, event_id)

    assert status["processing_status"] == "PENDING"
    assert status["claim_token"] is None
    assert status["last_error"] == "FullSnapshotRequired"


@pytest.mark.asyncio
async def test_expired_lease_cannot_apply_resource(
    environment
):
    engine, client, bridge = environment

    event_id = send(client, 10)

    async def slow_prepare(claim):
        # Дольше заданного срока захвата.
        await asyncio.sleep(1.2)
        return await bridge.prepare(claim)

    result = await process_one_inbox_event_async(
        engine,
        slow_prepare,
        bridge.apply,
        lease_seconds=1,
    )

    assert result.disposition == "STALE"
    assert resource_state(engine) is None
    assert last_sequence(engine) is None

    # После истечения lease другой Worker
    # забирает то же событие.
    result = await process(engine, bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["active"] == 1
    assert event_status(
        engine, event_id
    )["processing_status"] == "PROCESSED"
    assert event_status(engine, event_id)["attempts"] == 2


@pytest.mark.asyncio
async def test_loop_processes_post_automatically(
    environment
):
    engine, client, bridge = environment

    event_id = send(client, 10)

    worker = InboxLoop(
        engine,
        prepare=bridge.prepare,
        apply=bridge.apply,
        poll_seconds=0.02,
    )

    task = asyncio.create_task(worker.run())

    try:
        for _ in range(200):
            status = event_status(
                engine,
                event_id,
            )["processing_status"]

            if status == "PROCESSED":
                break

            await asyncio.sleep(0.01)

        else:
            pytest.fail(
                "Worker не обработал событие через POST"
            )

    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=3)

    assert resource_state(engine)["active"] == 1
    assert last_sequence(engine) == 10


@pytest.mark.asyncio
async def test_unsupported_source_failed_immediately(
    environment,
):
    engine, client, bridge = environment

    event_id = str(uuid4())

    payload = {
        "event_id": event_id,
        "source": "unsupported-source",
        "entity": "servers",
        "record_id": "server-17",
        "operation_code": 1,
        "source_sequence": 10,
        "occurred_at": "2026-11-21T12:00:00Z",
        "data": full_data(),
    }

    response = client.post(
        API_PATH,
        json=payload,
        headers={
            "Authorization": f"Bearer {SYNC_TOKEN}"
        },
    )

    assert response.status_code == 202

    result = await process(engine, bridge)

    assert result.disposition == "FAILED"

    saved = event_status(engine, event_id)

    assert saved["processing_status"] == "FAILED"
    assert saved["attempts"] == 1
    assert saved["last_error"] == "OracleInboxError"
    assert last_sequence(engine) is None


@pytest.mark.asyncio
async def test_invalid_resource_fields_failed_immediately(
    environment,
):
    engine, client, bridge = environment

    event_id = send(
        client,
        10,
        data=full_data(active="not-a-boolean"),
    )

    result = await process(engine, bridge)

    assert result.disposition == "FAILED"
    assert event_status(engine, event_id)["attempts"] == 1
    assert event_status(engine, event_id)[
        "last_error"
    ] == "OracleInboxError"
    assert resource_state(engine) is None
    assert last_sequence(engine) is None


@pytest.mark.asyncio
async def test_sequence_conflict_failed_immediately(
    environment,
):
    engine, client, bridge = environment

    send(client, 10)

    first = await process(engine, bridge)
    assert first.disposition == "PROCESSED"

    event_id = send(
        client,
        10,
        operation=3,
        data=full_data(name="Конфликтующая версия"),
    )

    result = await process(engine, bridge)

    assert result.disposition == "FAILED"
    assert event_status(engine, event_id)["attempts"] == 1
    assert event_status(engine, event_id)[
        "last_error"
    ] == "OracleInboxError"

    assert last_sequence(engine) == 10
    assert resource_state(engine)["name"] == "Сервер ТПУ"
    assert resource_state(engine)["version_number"] == 1


@pytest.mark.asyncio
async def test_unknown_update_still_retries(
    environment,
):
    engine, client, bridge = environment

    event_id = send(
        client,
        10,
        operation=3,
        data=full_data(),
    )

    result = await process(engine, bridge)

    assert result.disposition == "RETRY"

    saved = event_status(engine, event_id)

    assert saved["processing_status"] == "PENDING"
    assert saved["attempts"] == 1
    assert saved["last_error"] == "UnknownSourceResource"

    assert resource_state(engine) is None
    assert last_sequence(engine) is None


@pytest.mark.asyncio
async def test_delete_without_sequence_failed_immediately(
    environment,
):
    engine, client, bridge = environment

    event_id = send(
        client,
        None,
        operation=2,
        data={},
    )

    result = await process(engine, bridge)

    assert result.disposition == "FAILED"

    saved = event_status(engine, event_id)

    assert saved["processing_status"] == "FAILED"
    assert saved["attempts"] == 1
    assert saved["last_error"] == "OracleInboxError"

    assert last_sequence(engine) is None
    assert resource_state(engine) is None


# A23.2.2: частичные UPDATE с Entity API.

def make_snapshot_handler(
    original_bridge,
    record=None,
    *,
    items=None,
    status_code=200,
):
    import httpx

    from integrations.tpu.entity_client import (
        EntityRoutes,
        TpuEntityClient,
    )

    requests = []

    def mock_response(request):
        requests.append(request)

        if status_code != 200:
            return httpx.Response(
                status_code,
                json={"error": "test"},
            )

        records = (
            items
            if items is not None
            else [record]
        )

        return httpx.Response(
            200,
            json={
                "items": records,
                "next_cursor": None,
            },
        )

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(mock_response)
    )

    entity_client = TpuEntityClient(
        base_url="http://127.0.0.1:8001",
        routes=EntityRoutes.local_mock(),
        http_client=http_client,
    )

    bridge = OracleInboxHandler(
        original_bridge._mapper,
        snapshot_client=entity_client,
    )

    return bridge, http_client, requests


def make_snapshot_record(sequence=11, **changes):
    return {
        **full_data(**changes),
        "source_record_id": "server-17",
        "source_sequence": sequence,
    }


@pytest.mark.asyncio
async def test_partial_update_fetches_full_snapshot(
    environment,
):
    import json

    engine, client, original_bridge = environment

    send(client, 10)
    await process(engine, original_bridge)

    event_id = send(
        client,
        11,
        operation=3,
        data={"name": "Новое название"},
    )

    bridge, http_client, requests = make_snapshot_handler(
        original_bridge,
        make_snapshot_record(
            name="Новое название",
            interval_seconds=120,
        ),
    )

    async with http_client:
        result = await process(engine, bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["name"] == "Новое название"
    assert resource_state(engine)["interval_seconds"] == 120
    assert resource_state(engine)["version_number"] == 2
    assert last_sequence(engine) == 11

    assert event_status(engine, event_id)[
        "processing_status"
    ] == "PROCESSED"

    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert requests[0].url.params[
        "source_record_id"
    ] == "server-17"

    # Оригинальный JSON события не переписывается.
    with read_transaction(engine) as conn:
        raw_payload = conn.execute(
            text("""
                SELECT payload_json
                FROM inbox_events
                WHERE event_id=:id
            """),
            {"id": event_id},
        ).scalar_one()

    assert json.loads(raw_payload)["data"] == {
        "name": "Новое название"
    }


@pytest.mark.asyncio
async def test_snapshot_not_ready_retries(environment):
    engine, client, original_bridge = environment

    send(client, 10)
    await process(engine, original_bridge)

    event_id = send(
        client, 11, operation=3,
        data={"name": "Новое название"},
    )

    bridge, http_client, _ = make_snapshot_handler(
        original_bridge,
        items=[],
    )

    async with http_client:
        result = await process(engine, bridge)

    assert result.disposition == "RETRY"
    assert last_sequence(engine) == 10
    assert resource_state(engine)["version_number"] == 1
    assert event_status(engine, event_id)[
        "last_error"
    ] == "OracleSnapshotNotReady"


@pytest.mark.asyncio
async def test_snapshot_wrong_sequence_retries(environment):
    engine, client, original_bridge = environment

    send(client, 10)
    await process(engine, original_bridge)

    event_id = send(
        client, 11, operation=3,
        data={"name": "Новое название"},
    )

    bridge, http_client, _ = make_snapshot_handler(
        original_bridge,
        make_snapshot_record(
            sequence=12,
            name="Новое название",
        ),
    )

    async with http_client:
        result = await process(engine, bridge)

    assert result.disposition == "RETRY"
    assert last_sequence(engine) == 10
    assert resource_state(engine)["version_number"] == 1
    assert event_status(engine, event_id)[
        "last_error"
    ] == "OracleSnapshotNotReady"


@pytest.mark.asyncio
async def test_snapshot_wrong_id_fails(environment):
    engine, client, original_bridge = environment

    event_id = send(
        client, 11, operation=3,
        data={"name": "Новое название"},
    )

    record = make_snapshot_record(
        name="Новое название"
    )
    record["source_record_id"] = "another-server"

    bridge, http_client, _ = make_snapshot_handler(
        original_bridge,
        record,
    )

    async with http_client:
        result = await process(engine, bridge)

    assert result.disposition == "FAILED"
    assert event_status(engine, event_id)[
        "last_error"
    ] == "OracleInboxError"
    assert resource_state(engine) is None
    assert last_sequence(engine) is None


@pytest.mark.asyncio
async def test_snapshot_conflicting_field_fails(environment):
    engine, client, original_bridge = environment

    send(client, 10)
    await process(engine, original_bridge)

    event_id = send(
        client, 11, operation=3,
        data={"name": "Название из уведомления"},
    )

    bridge, http_client, _ = make_snapshot_handler(
        original_bridge,
        make_snapshot_record(
            name="Другое название",
        ),
    )

    async with http_client:
        result = await process(engine, bridge)

    assert result.disposition == "FAILED"
    assert last_sequence(engine) == 10
    assert resource_state(engine)["name"] == "Сервер ТПУ"
    assert event_status(engine, event_id)[
        "last_error"
    ] == "OracleInboxError"


@pytest.mark.asyncio
async def test_snapshot_duplicate_records_fail(environment):
    engine, client, original_bridge = environment

    event_id = send(
        client, 11, operation=3,
        data={"name": "Новое название"},
    )

    record = make_snapshot_record(
        name="Новое название"
    )

    bridge, http_client, _ = make_snapshot_handler(
        original_bridge,
        items=[record, record],
    )

    async with http_client:
        result = await process(engine, bridge)

    assert result.disposition == "FAILED"
    assert event_status(engine, event_id)[
        "last_error"
    ] == "OracleInboxError"
    assert last_sequence(engine) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "http_status,expected",
    [
        (401, "FAILED"),
        (503, "RETRY"),
    ],
)
async def test_snapshot_http_errors(
    environment,
    http_status,
    expected,
):
    engine, client, original_bridge = environment

    send(client, 10)
    await process(engine, original_bridge)

    event_id = send(
        client, 11, operation=3,
        data={"name": "Новое название"},
    )

    bridge, http_client, _ = make_snapshot_handler(
        original_bridge,
        status_code=http_status,
    )

    async with http_client:
        result = await process(engine, bridge)

    assert result.disposition == expected
    assert last_sequence(engine) == 10
    assert resource_state(engine)["version_number"] == 1

    saved = event_status(engine, event_id)
    assert saved["attempts"] == 1
    assert saved["processing_status"] == (
        "FAILED" if expected == "FAILED" else "PENDING"
    )


@pytest.mark.asyncio
async def test_full_update_skips_snapshot_fetch(environment):
    engine, client, original_bridge = environment

    send(client, 10)
    await process(engine, original_bridge)

    send(
        client,
        11,
        operation=3,
        data=full_data(name="Полное обновление"),
    )

    bridge, http_client, requests = make_snapshot_handler(
        original_bridge,
        make_snapshot_record(),
    )

    async with http_client:
        result = await process(engine, bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["name"] == (
        "Полное обновление"
    )
    assert len(requests) == 0


@pytest.mark.asyncio
async def test_delete_skips_snapshot_fetch(environment):
    engine, client, original_bridge = environment

    send(client, 10)
    await process(engine, original_bridge)

    send(
        client, 11, operation=2, data={},
    )

    bridge, http_client, requests = make_snapshot_handler(
        original_bridge,
        make_snapshot_record(),
    )

    async with http_client:
        result = await process(engine, bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["active"] == 0
    assert len(requests) == 0


@pytest.mark.asyncio
async def test_partial_update_recovers_after_entity_api_outage(
    environment,
):
    """Временный сбой API -> RETRY -> успешное обновление."""

    from datetime import timedelta

    engine, client, original_bridge = environment

    # Создаём исходный ресурс.
    send(client, 10)
    result = await process(engine, original_bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["version_number"] == 1

    # Отправляем частичный UPDATE.
    event_id = send(
        client,
        11,
        operation=3,
        data={"name": "Восстановленный сервер"},
    )

    # Первая попытка: Entity API недоступен (HTTP 503).
    broken_bridge, broken_client, _ = make_snapshot_handler(
        original_bridge,
        status_code=503,
    )

    first_now = datetime.now(timezone.utc) + timedelta(
        seconds=1
    )

    async with broken_client:
        first = await process(
            engine,
            broken_bridge,
            now=first_now,
            retry_base_seconds=5,
        )

    assert first.disposition == "RETRY"

    saved = event_status(engine, event_id)

    assert saved["processing_status"] == "PENDING"
    assert saved["attempts"] == 1
    assert saved["last_error"] == "EntityApiHttpError"

    # Неудачный запрос не изменил конфигурацию.
    assert resource_state(engine)["version_number"] == 1
    assert resource_state(engine)["interval_seconds"] == 60
    assert last_sequence(engine) == 10

    # Вторая попытка: API восстановился.
    recovered_bridge, recovered_client, requests = (
        make_snapshot_handler(
            original_bridge,
            make_snapshot_record(
                name="Восстановленный сервер",
                interval_seconds=180,
            ),
        )
    )

    async with recovered_client:
        second = await process(
            engine,
            recovered_bridge,
            now=first_now + timedelta(seconds=10),
            retry_base_seconds=5,
        )

    assert second.disposition == "PROCESSED"

    saved = event_status(engine, event_id)
    resource = resource_state(engine)

    assert saved["processing_status"] == "PROCESSED"
    assert saved["attempts"] == 2
    assert saved["last_error"] is None

    assert resource["name"] == "Восстановленный сервер"
    assert resource["version_number"] == 2
    assert resource["interval_seconds"] == 180
    assert last_sequence(engine) == 11

    assert len(requests) == 1


@pytest.mark.asyncio
async def test_partial_update_recovers_after_entity_api_outage(
    environment,
):
    """Временный сбой API -> RETRY -> успешное обновление."""

    from datetime import timedelta

    engine, client, original_bridge = environment

    # Создаём исходный ресурс.
    send(client, 10)
    result = await process(engine, original_bridge)

    assert result.disposition == "PROCESSED"
    assert resource_state(engine)["version_number"] == 1

    # Отправляем частичный UPDATE.
    event_id = send(
        client,
        11,
        operation=3,
        data={"name": "Восстановленный сервер"},
    )

    # Первая попытка: Entity API недоступен (HTTP 503).
    broken_bridge, broken_client, _ = make_snapshot_handler(
        original_bridge,
        status_code=503,
    )

    first_now = datetime.now(timezone.utc) + timedelta(
        seconds=1
    )

    async with broken_client:
        first = await process(
            engine,
            broken_bridge,
            now=first_now,
            retry_base_seconds=5,
        )

    assert first.disposition == "RETRY"

    saved = event_status(engine, event_id)

    assert saved["processing_status"] == "PENDING"
    assert saved["attempts"] == 1
    assert saved["last_error"] == "EntityApiHttpError"

    # Неудачный запрос не изменил конфигурацию.
    assert resource_state(engine)["version_number"] == 1
    assert resource_state(engine)["interval_seconds"] == 60
    assert last_sequence(engine) == 10

    # Вторая попытка: API восстановился.
    recovered_bridge, recovered_client, requests = (
        make_snapshot_handler(
            original_bridge,
            make_snapshot_record(
                name="Восстановленный сервер",
                interval_seconds=180,
            ),
        )
    )

    async with recovered_client:
        second = await process(
            engine,
            recovered_bridge,
            now=first_now + timedelta(seconds=10),
            retry_base_seconds=5,
        )

    assert second.disposition == "PROCESSED"

    saved = event_status(engine, event_id)
    resource = resource_state(engine)

    assert saved["processing_status"] == "PROCESSED"
    assert saved["attempts"] == 2
    assert saved["last_error"] is None

    assert resource["name"] == "Восстановленный сервер"
    assert resource["version_number"] == 2
    assert resource["interval_seconds"] == 180
    assert last_sequence(engine) == 11

    assert len(requests) == 1


@pytest.mark.asyncio
async def test_partial_update_timeout_then_recovers(environment):
    """Таймаут подготовки не изменяет ресурс."""

    from datetime import timedelta

    import httpx

    from integrations.tpu.entity_client import (
        EntityRoutes,
        TpuEntityClient,
    )

    engine, client, original_bridge = environment

    send(client, 10)
    initial = await process(engine, original_bridge)

    assert initial.disposition == "PROCESSED"

    event_id = send(
        client,
        11,
        operation=3,
        data={"name": "После таймаута"},
    )

    async def slow_response(request):
        await asyncio.sleep(0.2)
        return httpx.Response(
            200,
            json={
                "items": [
                    make_snapshot_record(
                        name="После таймаута",
                        interval_seconds=180,
                    )
                ],
                "next_cursor": None,
            },
        )

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(slow_response)
    )

    entity_client = TpuEntityClient(
        base_url="http://127.0.0.1:8001",
        routes=EntityRoutes.local_mock(),
        http_client=http_client,
    )

    timed_bridge = OracleInboxHandler(
        original_bridge._mapper,
        snapshot_client=entity_client,
        prepare_timeout_seconds=0.02,
    )

    first_now = (
        datetime.now(timezone.utc)
        + timedelta(seconds=1)
    )

    async with http_client:
        first = await process(
            engine,
            timed_bridge,
            now=first_now,
        )

    assert first.disposition == "RETRY"

    saved = event_status(engine, event_id)

    assert saved["processing_status"] == "PENDING"
    assert saved["last_error"] == "TimeoutError"
    assert saved["attempts"] == 1

    assert resource_state(engine)["version_number"] == 1
    assert resource_state(engine)["interval_seconds"] == 60
    assert last_sequence(engine) == 10

    recovered_bridge, recovered_client, _ = (
        make_snapshot_handler(
            original_bridge,
            make_snapshot_record(
                name="После таймаута",
                interval_seconds=180,
            ),
        )
    )

    async with recovered_client:
        second = await process(
            engine,
            recovered_bridge,
            now=first_now + timedelta(seconds=10),
        )

    assert second.disposition == "PROCESSED"

    saved = event_status(engine, event_id)
    resource = resource_state(engine)

    assert saved["processing_status"] == "PROCESSED"
    assert saved["attempts"] == 2
    assert saved["last_error"] is None

    assert resource["name"] == "После таймаута"
    assert resource["version_number"] == 2
    assert resource["interval_seconds"] == 180
    assert last_sequence(engine) == 11


@pytest.mark.asyncio
async def test_inbox_loop_stop_finishes_current_event(environment):
    """Штатная остановка не обрывает активную обработку."""

    engine, client, bridge = environment

    event_id = send(client, 10)

    started = asyncio.Event()
    release = asyncio.Event()

    async def paused_prepare(claim):
        started.set()
        await release.wait()
        return await bridge.prepare(claim)

    worker = InboxLoop(
        engine,
        prepare=paused_prepare,
        apply=bridge.apply,
        poll_seconds=0.01,
    )

    task = asyncio.create_task(worker.run())

    try:
        await asyncio.wait_for(started.wait(), timeout=3)

        worker.stop()

        assert not task.done()

    finally:
        release.set()
        await asyncio.wait_for(task, timeout=3)

    saved = event_status(engine, event_id)

    assert saved["processing_status"] == "PROCESSED"
    assert saved["attempts"] == 1
    assert resource_state(engine)["version_number"] == 1
    assert last_sequence(engine) == 10


@pytest.mark.asyncio
async def test_async_inbox_exhausts_retries_without_changes(
    environment,
):
    """После последней попытки событие получает FAILED."""

    from datetime import timedelta

    engine, client, bridge = environment

    send(client, 10)

    initial = await process(engine, bridge)
    assert initial.disposition == "PROCESSED"

    event_id = send(
        client,
        11,
        operation=3,
        data={"name": "Неполный UPDATE"},
    )

    now = datetime.now(timezone.utc) + timedelta(seconds=1)

    first = await process(
        engine,
        bridge,
        now=now,
        max_attempts=2,
        retry_base_seconds=5,
    )

    assert first.disposition == "RETRY"
    assert event_status(engine, event_id)["attempts"] == 1

    second = await process(
        engine,
        bridge,
        now=now + timedelta(seconds=5),
        max_attempts=2,
        retry_base_seconds=5,
    )

    assert second.disposition == "FAILED"

    saved = event_status(engine, event_id)

    assert saved["processing_status"] == "FAILED"
    assert saved["attempts"] == 2
    assert saved["last_error"] == "FullSnapshotRequired"

    assert resource_state(engine)["version_number"] == 1
    assert resource_state(engine)["name"] == "Сервер ТПУ"
    assert last_sequence(engine) == 10

    assert await process(
        engine,
        bridge,
        now=now + timedelta(days=1),
        max_attempts=2,
    ) is None
