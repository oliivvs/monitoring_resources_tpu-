
"""N22: тесты HEARTBEAT на временной SQLite, без внешней сети."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy import text

import checks.heartbeat_worker as heartbeat_module
from app.config import AppSettings
from checks.heartbeat_worker import HeartbeatWorker, enqueue_due_heartbeats
from db.session import initialize, make_engine, read_transaction, write_transaction
from services.resources import upsert_configuration
from services.sender import send_one_event
from services.status import apply_check_result
from shared.schemas import CheckResultIn, ResourceConfig, to_utc_iso


BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def engine(tmp_path):
    db = make_engine(tmp_path / "heartbeat_test.sqlite")
    initialize(db)
    yield db
    db.dispose()


def create_resource(engine, rid="service-1", *, checked=True, success=True):
    with write_transaction(engine) as conn:
        version = upsert_configuration(
            conn,
            ResourceConfig(
                id=rid,
                name=f"Сервис {rid}",
                target="https://example.org/health",
                check_type="HTTP",
                heartbeat_interval_seconds=75,
                up_after=1,
                down_after=1,
            ),
            now=BASE,
        )

    if checked:
        with write_transaction(engine) as conn:
            apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id=rid,
                    config_version_id=version,
                    checked_at=BASE + timedelta(seconds=1),
                    success=success,
                    response_time_ms=12.5,
                    http_status_code=200 if success else 503,
                    error_message=None if success else "HTTP 503",
                ),
            )

    return version


def heartbeat_rows(engine):
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT payload_json, checked_at, created_at
                FROM outbox_events
                WHERE event_type = 'HEARTBEAT'
                ORDER BY created_at, event_id
            """)
        ).mappings().all()


def test_heartbeat_not_due_before_interval(engine):
    create_resource(engine)
    batch = enqueue_due_heartbeats(
        engine, now=BASE + timedelta(seconds=75)
    )
    assert batch.scanned == 1
    assert batch.queued == 0
    assert heartbeat_rows(engine) == []


def test_due_heartbeat_uses_last_real_check_time(engine):
    create_resource(engine)
    due = BASE + timedelta(seconds=76)
    batch = enqueue_due_heartbeats(engine, now=due)

    assert batch.scanned == 1
    assert batch.queued == 1
    assert len(batch.event_ids) == 1

    rows = heartbeat_rows(engine)
    assert len(rows) == 1
    payload = json.loads(rows[0]["payload_json"])
    assert set(payload) == {
        "event_id", "event_type", "resource_id", "resource_name",
        "status", "check_type", "checked_at", "response_time_ms", "error",
    }
    assert payload["event_type"] == "HEARTBEAT"
    assert payload["status"] == "UP"
    assert payload["checked_at"] == to_utc_iso(BASE + timedelta(seconds=1))
    assert rows[0]["checked_at"] == to_utc_iso(BASE + timedelta(seconds=1))
    assert rows[0]["created_at"] == to_utc_iso(due)


def test_repeated_poll_does_not_duplicate(engine):
    create_resource(engine)
    first = enqueue_due_heartbeats(engine, now=BASE + timedelta(seconds=76))
    second = enqueue_due_heartbeats(engine, now=BASE + timedelta(seconds=76))
    later = enqueue_due_heartbeats(engine, now=BASE + timedelta(seconds=150))
    next_due = enqueue_due_heartbeats(engine, now=BASE + timedelta(seconds=151))

    assert (first.queued, second.queued, later.queued, next_due.queued) == (1, 0, 0, 1)
    assert len(heartbeat_rows(engine)) == 2


def test_no_real_checks_no_heartbeat(engine):
    create_resource(engine, checked=False)
    batch = enqueue_due_heartbeats(engine, now=BASE + timedelta(hours=1))
    assert batch.scanned == 0
    assert batch.queued == 0


def test_inactive_resource_is_not_enqueued(engine):
    create_resource(engine)
    with write_transaction(engine) as conn:
        upsert_configuration(
            conn,
            ResourceConfig(
                id="service-1",
                name="Сервис service-1",
                target="https://example.org/health",
                check_type="HTTP",
                heartbeat_interval_seconds=75,
                up_after=1,
                down_after=1,
                active=False,
            ),
            now=BASE + timedelta(seconds=3),
        )
    batch = enqueue_due_heartbeats(engine, now=BASE + timedelta(seconds=90))
    assert batch.scanned == 0
    assert batch.queued == 0


@pytest.mark.parametrize("success, expected", [(True, "UP"), (False, "DOWN")])
def test_heartbeat_preserves_real_status(engine, success, expected):
    create_resource(engine, success=success)
    enqueue_due_heartbeats(engine, now=BASE + timedelta(seconds=76))
    payload = json.loads(heartbeat_rows(engine)[0]["payload_json"])
    assert payload["status"] == expected
    assert payload["checked_at"] == to_utc_iso(BASE + timedelta(seconds=1))


def test_two_resources_are_queued(engine):
    create_resource(engine, rid="alpha")
    create_resource(engine, rid="beta")
    batch = enqueue_due_heartbeats(engine, now=BASE + timedelta(seconds=76))
    assert batch.scanned == 2
    assert batch.queued == 2
    assert len(set(batch.event_ids)) == 2


def test_transaction_rolls_back_all_heartbeats(engine, monkeypatch):
    create_resource(engine, rid="alpha")
    create_resource(engine, rid="beta")
    original = heartbeat_module.enqueue_heartbeat_if_due

    def fail_on_beta(conn, *, resource_id, now):
        if resource_id == "beta":
            raise RuntimeError("Тестовый сбой")
        return original(conn, resource_id=resource_id, now=now)

    monkeypatch.setattr(heartbeat_module, "enqueue_heartbeat_if_due", fail_on_beta)
    with pytest.raises(RuntimeError, match="Тестовый сбой"):
        enqueue_due_heartbeats(engine, now=BASE + timedelta(seconds=76))
    assert heartbeat_rows(engine) == []


@pytest.mark.asyncio
async def test_async_worker_once_and_stop(engine):
    create_resource(engine)
    worker = HeartbeatWorker(engine, poll_seconds=0.05)
    result = await worker.run_once(now=BASE + timedelta(seconds=76))
    assert result.queued == 1

    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)
    worker.stop()
    await asyncio.wait_for(task, timeout=3)
    assert task.done()


def test_naive_timestamp_rejected(engine):
    with pytest.raises(ValueError, match="timezone-aware"):
        enqueue_due_heartbeats(engine, now=datetime(2026, 11, 20, 10, 0))


def test_invalid_poll_interval(engine):
    for value in (0, -1, float("nan"), float("inf"), 61):
        with pytest.raises(ValueError):
            HeartbeatWorker(engine, poll_seconds=value)


def test_heartbeat_is_delivered_by_existing_sender(engine):
    create_resource(engine)
    enqueue_due_heartbeats(engine, now=BASE + timedelta(seconds=76))

    received: list[dict] = []

    def receiver(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer test-token"
        received.append(json.loads(request.content))
        return httpx.Response(202)

    settings = AppSettings(
        _env_file=None,
        event_post_url="https://oko.example/api/v1/status/events",
        event_token="test-token",
    )

    with httpx.Client(transport=httpx.MockTransport(receiver)) as client:
        # STATUS_CHANGED уже находится в Outbox, за ним HEARTBEAT.
        assert send_one_event(
            engine, settings, worker_id="test-worker", client=client,
            now=BASE + timedelta(seconds=90),
        ) == "SENT"
        assert send_one_event(
            engine, settings, worker_id="test-worker", client=client,
            now=BASE + timedelta(seconds=90),
        ) == "SENT"

    assert [item["event_type"] for item in received] == [
        "STATUS_CHANGED", "HEARTBEAT",
    ]
    assert received[1]["checked_at"] == received[0]["checked_at"]
    assert received[1]["event_id"] != received[0]["event_id"]
