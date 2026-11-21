
"""A22.4: тесты атомарного INSERT/UPDATE Oracle Sync."""

from __future__ import annotations

import hashlib
import json

from datetime import datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path
from uuid import uuid4

import pytest

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text

from checks.security import NetworkPolicy
from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)

from integrations.tpu.entity_mapper import (
    EntityMapper,
    load_mapping,
)
from integrations.tpu.oracle_models import (
    OPERATION_NAMES,
    OracleSyncEvent,
)
from integrations.tpu.oracle_prepare import (
    prepare_oracle_resource,
)

from services.inbox_worker import process_one_inbox_event
from services.oracle_apply import (
    apply_prepared_oracle_resource,
)
from shared.schemas import to_utc_iso


NOW = datetime(
    2026, 11, 21, 12, 0,
    tzinfo=timezone.utc,
)


@pytest.fixture
def engine(tmp_path):
    db = make_engine(tmp_path / "oracle_apply.sqlite")
    initialize(db)

    for name in (
        "003_tpu_inbox",
        "004_inbox_worker",
        "005_source_sequence",
    ):
        migration = import_module(
            f"migrations.versions.{name}"
        )

        with db.begin() as conn:
            context = MigrationContext.configure(conn)

            with Operations.context(context):
                migration.upgrade()

    yield db
    db.dispose()


@pytest.fixture
def mapper():
    path = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "tpu_oracle_mapping.yaml"
    )

    policy = NetworkPolicy(
        allowed_hosts=frozenset({"127.0.0.1"}),
        allowed_ports=frozenset({18080}),
        permit_loopback_for_tests=True,
    )

    return EntityMapper(load_mapping(path), policy)


def data(**changes):
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


def event(sequence, *, operation=1, payload=None):
    return OracleSyncEvent(
        event_id=uuid4(),
        source="tpu-oracle-sync",
        entity="servers",
        record_id="server-17",
        operation_code=operation,
        source_sequence=sequence,
        occurred_at=NOW,
        data=data() if payload is None else payload,
    )


def save_event(engine, item):
    raw = json.dumps(
        item.model_dump(mode="json"),
        ensure_ascii=False,
    )

    digest = hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()

    with write_transaction(engine) as conn:
        conn.execute(
            text("""
                INSERT INTO inbox_events (
                    event_id,
                    source_system,
                    source_entity,
                    source_record_id,
                    operation,
                    source_sequence,
                    occurred_at,
                    payload_json,
                    payload_hash,
                    received_at
                ) VALUES (
                    :id,
                    :source,
                    :entity,
                    :record_id,
                    :operation,
                    :sequence,
                    :at,
                    :payload,
                    :digest,
                    :at
                )
            """),
            {
                "id": str(item.event_id),
                "source": item.source,
                "entity": item.entity,
                "record_id": item.record_id,
                "operation": OPERATION_NAMES[
                    item.operation_code
                ],
                "sequence": item.source_sequence,
                "at": to_utc_iso(NOW),
                "payload": raw,
                "digest": digest,
            },
        )


async def run_event(
    engine,
    mapper,
    item,
    *,
    seconds=1,
    raise_after_apply=False,
):
    # Сетевую проверку выполняем ДО write_transaction.
    prepared = await prepare_oracle_resource(
        item,
        mapper,
    )

    save_event(engine, item)

    def handler(conn, claim):
        apply_prepared_oracle_resource(
            conn,
            claim,
            item,
            prepared,
            resource_id_prefix=(
                mapper.mapping.resource_id_prefix
            ),
            now=NOW + timedelta(seconds=seconds),
        )

        if raise_after_apply:
            raise RuntimeError("Simulated DB failure")

    return process_one_inbox_event(
        engine,
        handler,
        now=NOW + timedelta(seconds=seconds),
    )


def state(engine):
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT
                    r.name,
                    r.active,
                    c.target,
                    c.interval_seconds,
                    c.version_number
                FROM resources AS r
                JOIN resource_states AS s
                  ON s.resource_id=r.id
                JOIN resource_config_versions AS c
                  ON c.id=s.config_version_id
                WHERE r.id='tpu-oracle-server-17'
            """)
        ).mappings().one_or_none()


def sequence(engine):
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT last_sequence
                FROM resource_source_sequences
                WHERE source_record_id='server-17'
            """)
        ).scalar_one_or_none()


def inbox_status(engine, item):
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT processing_status
                FROM inbox_events
                WHERE event_id=:id
            """),
            {"id": str(item.event_id)},
        ).scalar_one()


@pytest.mark.asyncio
async def test_insert_creates_resource(engine, mapper):
    item = event(10)

    result = await run_event(engine, mapper, item)

    assert result.disposition == "PROCESSED"
    assert state(engine)["name"] == "Сервер ТПУ"
    assert state(engine)["version_number"] == 1
    assert sequence(engine) == 10

    with read_transaction(engine) as conn:
        mapping = conn.execute(
            text("""
                SELECT resource_id
                FROM resource_source_mappings
            """)
        ).scalar_one()

    assert mapping == "tpu-oracle-server-17"


@pytest.mark.asyncio
async def test_update_changes_configuration(engine, mapper):
    await run_event(engine, mapper, event(10))

    update = event(
        11,
        operation=3,
        payload=data(
            name="Обновлённый сервер",
            interval_seconds=120,
        ),
    )

    result = await run_event(
        engine,
        mapper,
        update,
        seconds=2,
    )

    assert result.disposition == "PROCESSED"
    assert state(engine)["name"] == "Обновлённый сервер"
    assert state(engine)["interval_seconds"] == 120
    assert state(engine)["version_number"] == 2
    assert sequence(engine) == 11


@pytest.mark.asyncio
async def test_old_event_cannot_rollback_update(
    engine, mapper
):
    await run_event(engine, mapper, event(11))

    old = event(
        9,
        operation=3,
        payload=data(name="Старое название"),
    )

    result = await run_event(
        engine,
        mapper,
        old,
        seconds=2,
    )

    assert result.disposition == "PROCESSED"
    assert state(engine)["name"] == "Сервер ТПУ"
    assert sequence(engine) == 11


@pytest.mark.asyncio
async def test_update_without_insert_is_not_applied(
    engine, mapper
):
    update = event(15, operation=3)

    result = await run_event(
        engine, mapper, update
    )

    assert result.disposition == "RETRY"
    assert state(engine) is None
    assert sequence(engine) is None
    assert inbox_status(engine, update) == "PENDING"


@pytest.mark.asyncio
async def test_target_change_rolls_back(engine, mapper):
    await run_event(engine, mapper, event(10))

    update = event(
        11,
        operation=3,
        payload=data(
            target="http://127.0.0.1:18080/other"
        ),
    )

    result = await run_event(
        engine,
        mapper,
        update,
        seconds=2,
    )

    assert result.disposition == "RETRY"
    assert state(engine)["target"].endswith("/health")
    assert sequence(engine) == 10
    assert inbox_status(engine, update) == "PENDING"


@pytest.mark.asyncio
async def test_handler_failure_rolls_back_everything(
    engine, mapper
):
    item = event(10)

    result = await run_event(
        engine,
        mapper,
        item,
        raise_after_apply=True,
    )

    assert result.disposition == "RETRY"
    assert state(engine) is None
    assert sequence(engine) is None

    with read_transaction(engine) as conn:
        count = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM resource_source_mappings
            """)
        ).scalar_one()

    assert count == 0
    assert inbox_status(engine, item) == "PENDING"


@pytest.mark.asyncio
async def test_existing_resource_cannot_be_hijacked(
    engine, mapper
):
    with write_transaction(engine) as conn:
        conn.execute(
            text("""
                INSERT INTO resources (
                    id, name, active,
                    created_at, updated_at
                ) VALUES (
                    'tpu-oracle-server-17',
                    'Manual',
                    1,
                    :at,
                    :at
                )
            """),
            {"at": to_utc_iso(NOW)},
        )

    item = event(10)

    result = await run_event(
        engine, mapper, item
    )

    assert result.disposition == "RETRY"
    assert sequence(engine) is None

    with read_transaction(engine) as conn:
        name = conn.execute(
            text("""
                SELECT name FROM resources
                WHERE id='tpu-oracle-server-17'
            """)
        ).scalar_one()

    assert name == "Manual"

from services.oracle_delete import apply_oracle_delete


def run_delete(
    engine,
    item,
    *,
    seconds=3,
    fail_after_apply=False,
):
    """Проверяем DELETE через настоящий механизм Inbox."""

    save_event(engine, item)

    actions = []

    def handler(conn, claim):
        result = apply_oracle_delete(
            conn,
            claim,
            item,
            expected_source="tpu-oracle-sync",
            expected_entity="servers",
            now=NOW + timedelta(seconds=seconds),
        )

        actions.append(result.action)

        if fail_after_apply:
            raise RuntimeError("Simulated failure")

    outcome = process_one_inbox_event(
        engine,
        handler,
        now=NOW + timedelta(seconds=seconds),
    )

    return outcome, actions


@pytest.mark.asyncio
async def test_delete_deactivates_and_keeps_mapping(
    engine, mapper
):
    await run_event(engine, mapper, event(10))

    delete = event(
        11,
        operation=2,
        payload={},
    )

    result, actions = run_delete(engine, delete)

    assert result.disposition == "PROCESSED"
    assert actions == ["DEACTIVATED"]
    assert state(engine)["active"] == 0
    assert state(engine)["version_number"] == 2
    assert sequence(engine) == 11

    with read_transaction(engine) as conn:
        assert conn.execute(
            text("""
                SELECT COUNT(*)
                FROM resource_source_mappings
            """)
        ).scalar_one() == 1

        assert conn.execute(
            text("""
                SELECT COUNT(*)
                FROM resources
            """)
        ).scalar_one() == 1


@pytest.mark.asyncio
async def test_old_delete_does_not_deactivate(
    engine, mapper
):
    await run_event(
        engine,
        mapper,
        event(20),
    )

    old_delete = event(
        19,
        operation=2,
        payload={},
    )

    result, actions = run_delete(
        engine,
        old_delete,
    )

    assert result.disposition == "PROCESSED"
    assert actions == ["STALE"]
    assert state(engine)["active"] == 1
    assert sequence(engine) == 20


def test_delete_without_mapping_creates_tombstone(engine):
    delete = event(
        30,
        operation=2,
        payload={},
    )

    result, actions = run_delete(
        engine,
        delete,
    )

    assert result.disposition == "PROCESSED"
    assert actions == ["TOMBSTONED"]
    assert state(engine) is None
    assert sequence(engine) == 30


@pytest.mark.asyncio
async def test_failed_delete_rolls_back(engine, mapper):
    await run_event(
        engine,
        mapper,
        event(10),
    )

    delete = event(
        11,
        operation=2,
        payload={},
    )

    result, actions = run_delete(
        engine,
        delete,
        fail_after_apply=True,
    )

    assert result.disposition == "RETRY"
    assert actions == ["DEACTIVATED"]

    # Изменение ресурса полностью откатилось.
    assert state(engine)["active"] == 1
    assert state(engine)["version_number"] == 1

    # Новый sequence тоже откатился.
    assert sequence(engine) == 10
    assert inbox_status(engine, delete) == "PENDING"


@pytest.mark.asyncio
async def test_second_delete_keeps_inactive_resource(
    engine, mapper
):
    await run_event(
        engine,
        mapper,
        event(10),
    )

    first = event(
        11,
        operation=2,
        payload={},
    )

    run_delete(engine, first)

    second = event(
        12,
        operation=2,
        payload={},
    )

    result, actions = run_delete(
        engine,
        second,
        seconds=4,
    )

    assert result.disposition == "PROCESSED"
    assert actions == ["ALREADY_INACTIVE"]
    assert state(engine)["active"] == 0
    assert state(engine)["version_number"] == 2
    assert sequence(engine) == 12


@pytest.mark.asyncio
async def test_newer_insert_can_reactivate(
    engine, mapper
):
    await run_event(
        engine,
        mapper,
        event(10),
    )

    delete = event(
        11,
        operation=2,
        payload={},
    )

    run_delete(engine, delete)

    newer_insert = event(
        12,
        operation=1,
    )

    result = await run_event(
        engine,
        mapper,
        newer_insert,
        seconds=4,
    )

    assert result.disposition == "PROCESSED"
    assert state(engine)["active"] == 1
    assert state(engine)["version_number"] == 3
    assert sequence(engine) == 12


def test_delete_without_sequence_is_not_applied(engine):
    delete = event(
        None,
        operation=2,
        payload={},
    )

    result, _actions = run_delete(
        engine,
        delete,
    )

    assert result.disposition == "RETRY"
    assert state(engine) is None
    assert sequence(engine) is None
    assert inbox_status(engine, delete) == "PENDING"


def test_delete_rejects_unexpected_source(engine):
    delete = event(
        11,
        operation=2,
        payload={},
    ).model_copy(
        update={"source": "unexpected-source"}
    )

    result, _actions = run_delete(
        engine,
        delete,
    )

    assert result.disposition == "RETRY"
    assert state(engine) is None
    assert sequence(engine) is None
