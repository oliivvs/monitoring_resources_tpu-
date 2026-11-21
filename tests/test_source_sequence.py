
from __future__ import annotations

from datetime import datetime, timezone
from importlib import import_module

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

from services.source_sequence import (
    MissingSourceSequence,
    SourceSequenceConflict,
    check_and_advance_sequence,
)


NOW = datetime(
    2026, 11, 21,
    tzinfo=timezone.utc,
)


@pytest.fixture
def engine(tmp_path):
    db = make_engine(tmp_path / "sequence.sqlite")
    initialize(db)

    for version in (
        "003_tpu_inbox",
        "004_inbox_worker",
        "005_source_sequence",
    ):
        migration = import_module(
            f"migrations.versions.{version}"
        )

        with db.begin() as conn:
            context = MigrationContext.configure(conn)
            with Operations.context(context):
                migration.upgrade()

    yield db
    db.dispose()


def apply(
    engine,
    sequence,
    event_id,
    *,
    record_id="server-1",
    operation="UPDATE",
):
    with write_transaction(engine) as conn:
        return check_and_advance_sequence(
            conn,
            source_system="tpu",
            source_entity="servers",
            source_record_id=record_id,
            event_id=event_id,
            source_sequence=sequence,
            operation=operation,
            now=NOW,
        )


def current(engine, record_id="server-1"):
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT last_sequence,
                       last_event_id,
                       last_operation
                FROM resource_source_sequences
                WHERE source_record_id=:id
            """),
            {"id": record_id},
        ).mappings().first()


def test_first_event_is_applied(engine):
    assert apply(engine, 100, "event-100") == "APPLY"
    assert current(engine)["last_sequence"] == 100


def test_newer_event_advances_sequence(engine):
    apply(engine, 100, "event-100")

    assert apply(engine, 105, "event-105") == "APPLY"
    assert current(engine)["last_sequence"] == 105


def test_old_event_is_ignored(engine):
    apply(engine, 105, "event-105")

    assert apply(engine, 103, "event-103") == "STALE"
    assert current(engine)["last_sequence"] == 105


def test_duplicate_is_ignored(engine):
    apply(engine, 100, "event-100")

    assert (
        apply(engine, 100, "event-100")
        == "DUPLICATE"
    )


def test_same_sequence_different_event_rejected(engine):
    apply(engine, 100, "event-100")

    with pytest.raises(SourceSequenceConflict):
        apply(engine, 100, "event-other")

    assert current(engine)["last_event_id"] == "event-100"


def test_delete_keeps_tombstone(engine):
    apply(engine, 100, "event-100", operation="DELETE")

    assert (
        apply(engine, 99, "old-insert", operation="INSERT")
        == "STALE"
    )

    assert current(engine)["last_operation"] == "DELETE"


def test_sequence_is_required(engine):
    with pytest.raises(MissingSourceSequence):
        apply(engine, None, "event-1")

    assert current(engine) is None


def test_transaction_rollback(engine):
    with pytest.raises(RuntimeError):
        with write_transaction(engine) as conn:
            decision = check_and_advance_sequence(
                conn,
                source_system="tpu",
                source_entity="servers",
                source_record_id="server-1",
                event_id="event-100",
                source_sequence=100,
                operation="UPDATE",
                now=NOW,
            )

            assert decision == "APPLY"

            # Имитируем ошибку обновления ресурса.
            raise RuntimeError("Resource update failed")

    # И запись sequence тоже должна откатиться.
    assert current(engine) is None


def test_different_records_are_independent(engine):
    assert apply(
        engine, 50, "event-a",
        record_id="server-A",
    ) == "APPLY"

    assert apply(
        engine, 10, "event-b",
        record_id="server-B",
    ) == "APPLY"

    assert current(engine, "server-A")["last_sequence"] == 50
    assert current(engine, "server-B")["last_sequence"] == 10
