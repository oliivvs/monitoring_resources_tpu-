
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from importlib import import_module
from uuid import uuid4

import pytest

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text

from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)

from services.inbox_worker import (
    claim_inbox_event,
    process_one_inbox_event,
)

from shared.schemas import to_utc_iso


NOW = datetime(
    2026, 11, 21, 9, 0,
    tzinfo=timezone.utc,
)


@pytest.fixture
def engine(tmp_path):
    result = make_engine(
        tmp_path / "inbox.sqlite"
    )

    initialize(result)

    for name in (
        "003_tpu_inbox",
        "004_inbox_worker",
    ):
        revision = import_module(
            f"migrations.versions.{name}"
        )

        with result.begin() as conn:
            context = MigrationContext.configure(conn)

            with Operations.context(context):
                revision.upgrade()

    with write_transaction(result) as conn:
        conn.execute(text("""
            CREATE TABLE applied_events (
                event_id TEXT PRIMARY KEY
            )
        """))

    yield result
    result.dispose()


def put(engine, *, event_id=None):
    event_id = event_id or str(uuid4())

    with write_transaction(engine) as conn:
        conn.execute(
            text("""
                INSERT INTO inbox_events (
                    event_id,
                    source_system,
                    source_entity,
                    source_record_id,
                    operation,
                    occurred_at,
                    payload_json,
                    payload_hash,
                    received_at
                ) VALUES (
                    :id,
                    'tpu',
                    'servers',
                    'record-A',
                    'UPDATE',
                    :ts,
                    '{}',
                    :hash,
                    :ts
                )
            """),
            {
                "id": event_id,
                "ts": to_utc_iso(NOW),
                "hash": "x" * 64,
            },
        )

    return event_id


def row(engine, event_id):
    with read_transaction(engine) as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM inbox_events
                WHERE event_id=:id
            """),
            {"id": event_id},
        ).mappings().one()

        return dict(result)


def apply_ok(conn, event):
    # Имитация полезного изменения в SQLite.
    # Это не обработчик production Oracle Sync.
    conn.execute(
        text("""
            INSERT INTO applied_events(event_id)
            VALUES (:id)
        """),
        {"id": event.event_id},
    )


def test_claim_once_and_persist(engine):
    event_id = put(engine)

    first = claim_inbox_event(
        engine, now=NOW
    )

    assert first.event_id == event_id
    assert first.attempts == 1

    assert claim_inbox_event(
        engine, now=NOW
    ) is None

    assert row(engine, event_id)[
        "processing_status"
    ] == "PROCESSING"


def test_success_atomically_marks_processed(engine):
    event_id = put(engine)

    done = process_one_inbox_event(
        engine, apply_ok, now=NOW
    )

    assert done.disposition == "PROCESSED"

    assert row(engine, event_id)[
        "processing_status"
    ] == "PROCESSED"

    assert row(engine, event_id)[
        "claim_token"
    ] is None

    with read_transaction(engine) as conn:
        count = conn.execute(
            text("""
                SELECT count(*) FROM applied_events
            """)
        ).scalar_one()

    assert count == 1

    assert process_one_inbox_event(
        engine, apply_ok, now=NOW
    ) is None


def test_exception_rolls_back_and_retries(engine):
    event_id = put(engine)

    def broken(conn, event):
        apply_ok(conn, event)
        raise RuntimeError("token=secret")

    result = process_one_inbox_event(
        engine,
        broken,
        now=NOW,
        retry_base_seconds=5,
    )

    assert result.disposition == "RETRY"

    assert row(engine, event_id)[
        "processing_status"
    ] == "PENDING"

    assert row(engine, event_id)[
        "last_error"
    ] == "RuntimeError"

    with read_transaction(engine) as conn:
        count = conn.execute(
            text("""
                SELECT count(*) FROM applied_events
            """)
        ).scalar_one()

    assert count == 0

    assert claim_inbox_event(
        engine,
        now=NOW + timedelta(seconds=4),
    ) is None

    result = process_one_inbox_event(
        engine,
        apply_ok,
        now=NOW + timedelta(seconds=5),
    )

    assert result.disposition == "PROCESSED"


def test_max_attempts_failed(engine):
    event_id = put(engine)

    def broken(conn, event):
        raise ValueError("invalid")

    first = process_one_inbox_event(
        engine,
        broken,
        now=NOW,
        max_attempts=2,
    )

    assert first.disposition == "RETRY"

    second = process_one_inbox_event(
        engine,
        broken,
        now=NOW + timedelta(seconds=5),
        max_attempts=2,
    )

    assert second.disposition == "FAILED"

    assert row(engine, event_id)["attempts"] == 2

    assert claim_inbox_event(
        engine,
        now=NOW + timedelta(days=1),
        max_attempts=2,
    ) is None


def test_crash_lease_reclaim_and_fencing(engine):
    event_id = put(engine)

    old = claim_inbox_event(
        engine,
        now=NOW,
        lease_seconds=10,
    )

    assert claim_inbox_event(
        engine,
        now=NOW + timedelta(seconds=9),
    ) is None

    new = claim_inbox_event(
        engine,
        now=NOW + timedelta(seconds=10),
        lease_seconds=10,
    )

    assert new.event_id == event_id
    assert new.claim_token != old.claim_token
    assert new.attempts == 2

    assert row(engine, event_id)[
        "claim_token"
    ] == new.claim_token


def test_expired_final_attempt_becomes_failed(engine):
    event_id = put(engine)

    claim_inbox_event(
        engine,
        now=NOW,
        max_attempts=1,
        lease_seconds=10,
    )

    assert claim_inbox_event(
        engine,
        now=NOW + timedelta(seconds=10),
        max_attempts=1,
    ) is None

    assert row(engine, event_id)[
        "processing_status"
    ] == "FAILED"


def test_competing_claims_only_one_wins(engine):
    event_id = put(engine)

    with ThreadPoolExecutor(max_workers=4) as pool:
        claims = list(
            pool.map(
                lambda _: claim_inbox_event(
                    engine, now=NOW
                ),
                range(4),
            )
        )

    assert sum(
        claim is not None
        for claim in claims
    ) == 1

    assert any(
        claim is not None
        and claim.event_id == event_id
        for claim in claims
    )


def test_wrong_handler_is_rejected(engine):
    put(engine)

    with pytest.raises(ValueError):
        process_one_inbox_event(
            engine,
            None,
            now=NOW,
        )

    assert claim_inbox_event(
        engine,
        now=NOW,
    ).attempts == 1

import asyncio

from services.inbox_loop import InboxLoop


@pytest.mark.asyncio
async def test_inbox_loop_processes_event(engine):
    """Цикл самостоятельно обрабатывает событие."""

    event_id = put(engine)

    worker = InboxLoop(
        engine,
        apply_ok,
        poll_seconds=0.01,
    )

    task = asyncio.create_task(worker.run())

    try:
        for _ in range(200):
            status = row(
                engine,
                event_id,
            )["processing_status"]

            if status == "PROCESSED":
                break

            await asyncio.sleep(0.01)

        else:
            pytest.fail(
                "Inbox Worker не обработал событие"
            )

    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=3)

    assert row(
        engine,
        event_id,
    )["processing_status"] == "PROCESSED"

    with read_transaction(engine) as conn:
        count = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM applied_events
            """)
        ).scalar_one()

    assert count == 1


@pytest.mark.asyncio
async def test_inbox_loop_stops_without_events(engine):
    """Пустой Worker штатно завершает работу."""

    worker = InboxLoop(
        engine,
        apply_ok,
        poll_seconds=0.01,
    )

    task = asyncio.create_task(worker.run())

    await asyncio.sleep(0.05)

    worker.stop()

    await asyncio.wait_for(task, timeout=3)

    assert task.done()

    with pytest.raises(RuntimeError):
        await worker.run()


def test_inbox_loop_requires_handler(engine):
    """Нельзя запускать Worker без обработчика."""

    with pytest.raises(ValueError):
        InboxLoop(
            engine,
            None,
        )

    with pytest.raises(ValueError):
        InboxLoop(
            engine,
            apply_ok,
            poll_seconds=0,
        )

    with pytest.raises(ValueError):
        InboxLoop(
            engine,
            apply_ok,
            max_attempts=0,
        )

def test_permanent_failure_has_no_retry(engine):
    from services.inbox_worker import PermanentInboxError

    event_id = put(engine)

    def permanent_failure(conn, claim):
        apply_ok(conn, claim)
        raise PermanentInboxError("secret=must-not-leak")

    result = process_one_inbox_event(
        engine,
        permanent_failure,
        now=NOW,
        max_attempts=5,
    )

    assert result.disposition == "FAILED"

    saved = row(engine, event_id)

    assert saved["processing_status"] == "FAILED"
    assert saved["attempts"] == 1
    assert saved["last_error"] == "PermanentInboxError"
    assert saved["next_attempt_at"] is None
    assert saved["claim_token"] is None
    assert "secret" not in str(saved["last_error"])

    with read_transaction(engine) as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM applied_events")
        ).scalar_one()

    assert count == 0

    assert process_one_inbox_event(
        engine,
        apply_ok,
        now=NOW + timedelta(days=1),
    ) is None
