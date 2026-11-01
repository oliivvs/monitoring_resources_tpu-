
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DatabaseError
from db.session import initialize, make_engine
from db.session import read_transaction, write_transaction
from services.resources import upsert_configuration
from services.status import apply_check_result
from shared.schemas import (
    CheckResultIn,
    ResourceConfig,
    StatusEventPayload,
)
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from services.events import enqueue_heartbeat_if_due

BASE = datetime(2026, 11, 1, tzinfo=timezone.utc)


@pytest.fixture
def engine(tmp_path):
    # Для тестов используем отдельную базу SQLite
    db_engine = make_engine(tmp_path / "outbox_test.db")
    initialize(db_engine)

    yield db_engine

    db_engine.dispose()


def create_resource(engine):
    with write_transaction(engine) as conn:
        return upsert_configuration(
            conn,
            ResourceConfig(
                id="server-001",
                name="Original Server",
                target="https://example.org/health",
                check_type="HTTP",
            ),
            now=BASE,
        )


def send_success(engine, version, seconds):
    with write_transaction(engine) as conn:
        return apply_check_result(
            conn,
            result=CheckResultIn(
                resource_id="server-001",
                config_version_id=version,
                checked_at=BASE + timedelta(seconds=seconds),
                success=True,
                response_time_ms=12.5,
                http_status_code=200,
            ),
        )


def test_status_change_creates_one_event(engine):
    version = create_resource(engine)

    first = send_success(engine, version, 1)
    second = send_success(engine, version, 2)
    third = send_success(engine, version, 3)

    # UP наступает после двух успешных проверок
    assert first["status_changed"] is False
    assert second["status_changed"] is True
    assert third["status_changed"] is False

    with read_transaction(engine) as conn:
        events = conn.execute(text("""
            SELECT event_id, event_type, delivery_status,
                   attempts, payload_json
            FROM outbox_events
        """)).mappings().all()

    # Третья успешная проверка не создаёт дубль
    assert len(events) == 1

    saved = events[0]
    payload = StatusEventPayload.model_validate_json(
        saved["payload_json"]
    )

    assert saved["event_type"] == "STATUS_CHANGED"
    assert saved["delivery_status"] == "PENDING"
    assert saved["attempts"] == 0

    assert payload.event_id == UUID(saved["event_id"])
    assert payload.resource_id == "server-001"
    assert payload.status == "UP"
    assert payload.checked_at == BASE + timedelta(seconds=2)


def test_saved_event_is_immutable_snapshot(engine):
    version = create_resource(engine)
    send_success(engine, version, 1)
    send_success(engine, version, 2)

    with read_transaction(engine) as conn:
        original_json = conn.execute(text("""
            SELECT payload_json FROM outbox_events
        """)).scalar_one()

    # После события переименовываем ресурс
    with write_transaction(engine) as conn:
        upsert_configuration(
            conn,
            ResourceConfig(
                id="server-001",
                name="Renamed Server",
                target="https://example.org/health",
                check_type="HTTP",
            ),
            now=BASE + timedelta(seconds=10),
        )

    with read_transaction(engine) as conn:
        saved_json = conn.execute(text("""
            SELECT payload_json FROM outbox_events
        """)).scalar_one()

        current_name = conn.execute(text("""
            SELECT name FROM resources WHERE id = :id
        """), {"id": "server-001"}).scalar_one()

    # Изменение ресурса не меняет старое событие
    assert current_name == "Renamed Server"
    assert saved_json == original_json

    payload = StatusEventPayload.model_validate_json(saved_json)
    assert payload.resource_name == "Original Server"

def test_heartbeat_interval_and_payload(engine):
    # Создаём ресурс и переводим его в UP
    version = create_resource(engine)
    send_success(engine, version, 1)
    send_success(engine, version, 2)

    # До истечения 300 секунд HEARTBEAT не нужен
    with write_transaction(engine) as conn:
        early = enqueue_heartbeat_if_due(
            conn,
            resource_id="server-001",
            now=BASE + timedelta(seconds=301),
        )

    assert early is None

    # Через 300 секунд после смены статуса
    # создаётся HEARTBEAT
    with write_transaction(engine) as conn:
        event_id = enqueue_heartbeat_if_due(
            conn,
            resource_id="server-001",
            now=BASE + timedelta(seconds=302),
        )

    assert event_id is not None

    # Повторный вызов не должен создавать дубль
    with write_transaction(engine) as conn:
        repeated = enqueue_heartbeat_if_due(
            conn,
            resource_id="server-001",
            now=BASE + timedelta(seconds=303),
        )

    assert repeated is None

    with read_transaction(engine) as conn:
        events = conn.execute(text("""
            SELECT event_type, payload_json, delivery_status
            FROM outbox_events
            ORDER BY created_at
        """)).mappings().all()

    assert len(events) == 2
    assert [e["event_type"] for e in events] == [
        "STATUS_CHANGED",
        "HEARTBEAT",
    ]

    heartbeat = events[1]
    payload = StatusEventPayload.model_validate_json(
        heartbeat["payload_json"]
    )

    assert payload.event_id == event_id
    assert payload.event_type == "HEARTBEAT"
    assert payload.status == "UP"
    assert payload.checked_at == BASE + timedelta(seconds=2)
    assert heartbeat["delivery_status"] == "PENDING"


def test_concurrent_heartbeat_no_duplicates(engine):
    # Два потока одновременно пытаются создать HEARTBEAT
    version = create_resource(engine)
    send_success(engine, version, 1)
    send_success(engine, version, 2)

    barrier = Barrier(2)
    heartbeat_time = BASE + timedelta(seconds=302)

    def enqueue():
        barrier.wait(timeout=5)

        with write_transaction(engine) as conn:
            return enqueue_heartbeat_if_due(
                conn,
                resource_id="server-001",
                now=heartbeat_time,
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(enqueue) for _ in range(2)]
        results = [f.result(timeout=10) for f in futures]

    # Только один поток должен создать событие
    assert sum(result is not None for result in results) == 1

    with read_transaction(engine) as conn:
        count = conn.execute(text("""
            SELECT COUNT(*)
            FROM outbox_events
            WHERE event_type = 'HEARTBEAT'
        """)).scalar_one()

    assert count == 1

def test_heartbeat_failure_allows_retry(engine):
    # Подготавливаем ресурс со статусом UP
    version = create_resource(engine)
    send_success(engine, version, 1)
    send_success(engine, version, 2)

    heartbeat_time = BASE + timedelta(seconds=302)

    # Имитируем ошибку записи HEARTBEAT в SQLite
    with write_transaction(engine) as conn:
        conn.exec_driver_sql("""
            CREATE TRIGGER reject_heartbeat_event
            BEFORE INSERT ON outbox_events
            WHEN NEW.event_type = 'HEARTBEAT'
            BEGIN
                SELECT RAISE(ABORT, 'blocked heartbeat');
            END
        """)

    with pytest.raises(DatabaseError, match="blocked heartbeat"):
        with write_transaction(engine) as conn:
            enqueue_heartbeat_if_due(
                conn,
                resource_id="server-001",
                now=heartbeat_time,
            )

    # Проверяем, что неудачная попытка
    # не изменила состояние ресурса
    with read_transaction(engine) as conn:
        last_heartbeat = conn.execute(text("""
            SELECT last_heartbeat_enqueued_at
            FROM resource_states
            WHERE resource_id = 'server-001'
        """)).scalar_one()

        count = conn.execute(text("""
            SELECT COUNT(*)
            FROM outbox_events
            WHERE event_type = 'HEARTBEAT'
        """)).scalar_one()

    assert last_heartbeat is None
    assert count == 0

    # Устраняем имитированную ошибку
    with write_transaction(engine) as conn:
        conn.exec_driver_sql(
            "DROP TRIGGER reject_heartbeat_event"
        )

    # Повторная попытка должна создать событие
    with write_transaction(engine) as conn:
        event_id = enqueue_heartbeat_if_due(
            conn,
            resource_id="server-001",
            now=heartbeat_time,
        )

    assert event_id is not None

    with read_transaction(engine) as conn:
        count = conn.execute(text("""
            SELECT COUNT(*)
            FROM outbox_events
            WHERE event_type = 'HEARTBEAT'
        """)).scalar_one()

    assert count == 1
