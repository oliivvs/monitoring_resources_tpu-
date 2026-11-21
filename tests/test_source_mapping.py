
"""Тесты сопоставления Oracle Sync с внутренними ресурсами."""

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

from services.source_mapping import (
    SourceMappingAmbiguous,
    SourceMappingConflict,
    SourceResourceMissing,
    link_existing_resource,
    lookup_resource_id,
)

from shared.schemas import to_utc_iso


NOW = datetime(
    2026, 11, 21, 12, 0,
    tzinfo=timezone.utc,
)

SOURCE = {
    "source_system": "tpu-oracle-sync",
    "source_entity": "servers",
    "source_record_id": "server-17",
}


@pytest.fixture
def engine(tmp_path):
    db = make_engine(tmp_path / "mapping.sqlite")
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


def seed_resource(engine, resource_id):
    with write_transaction(engine) as conn:
        conn.execute(
            text("""
                INSERT INTO resources (
                    id, name, active,
                    created_at, updated_at
                ) VALUES (
                    :id, :name, 1, :at, :at
                )
            """),
            {
                "id": resource_id,
                "name": "Тестовый ресурс",
                "at": to_utc_iso(NOW),
            },
        )


def link(engine, resource_id, **changes):
    source = {**SOURCE, **changes}

    with write_transaction(engine) as conn:
        return link_existing_resource(
            conn,
            **source,
            resource_id=resource_id,
            now=NOW,
        )


def lookup(engine, **changes):
    source = {**SOURCE, **changes}

    with read_transaction(engine) as conn:
        return lookup_resource_id(
            conn,
            **source,
        )


def test_no_mapping_returns_none(engine):
    assert lookup(engine) is None


def test_create_mapping(engine):
    seed_resource(engine, "tpu-server-17")

    assert link(
        engine, "tpu-server-17"
    ) == "CREATED"

    assert lookup(engine) == "tpu-server-17"


def test_repeated_mapping_is_idempotent(engine):
    seed_resource(engine, "tpu-server-17")

    assert link(
        engine, "tpu-server-17"
    ) == "CREATED"

    assert link(
        engine, "tpu-server-17"
    ) == "UNCHANGED"

    with read_transaction(engine) as conn:
        count = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM resource_source_mappings
            """)
        ).scalar_one()

    assert count == 1


def test_same_source_different_resource_conflict(engine):
    seed_resource(engine, "resource-A")
    seed_resource(engine, "resource-B")

    link(engine, "resource-A")

    with pytest.raises(SourceMappingConflict):
        link(engine, "resource-B")

    assert lookup(engine) == "resource-A"


def test_same_resource_other_source_conflict(engine):
    seed_resource(engine, "resource-A")

    link(engine, "resource-A")

    with pytest.raises(SourceMappingConflict):
        link(
            engine,
            "resource-A",
            source_record_id="server-99",
        )

    assert lookup(engine) == "resource-A"


def test_unknown_resource_is_rejected(engine):
    with pytest.raises(SourceResourceMissing):
        link(engine, "resource-does-not-exist")

    assert lookup(engine) is None


def test_ambiguous_existing_mapping_rejected(engine):
    seed_resource(engine, "resource-A")
    seed_resource(engine, "resource-B")

    # Имитируем неоднозначные данные, которые
    # текущая схема 003 пока технически допускает.
    with write_transaction(engine) as conn:
        for resource_id in ("resource-A", "resource-B"):
            conn.execute(
                text("""
                    INSERT INTO resource_source_mappings (
                        source_system,
                        source_entity,
                        source_record_id,
                        resource_id,
                        created_at,
                        updated_at
                    ) VALUES (
                        :source,
                        :entity,
                        :record_id,
                        :resource_id,
                        :at,
                        :at
                    )
                """),
                {
                    "source": SOURCE["source_system"],
                    "entity": SOURCE["source_entity"],
                    "record_id": SOURCE["source_record_id"],
                    "resource_id": resource_id,
                    "at": to_utc_iso(NOW),
                },
            )

    with pytest.raises(SourceMappingAmbiguous):
        lookup(engine)

    with pytest.raises(SourceMappingAmbiguous):
        link(engine, "resource-A")


def test_mapping_rolls_back_with_transaction(engine):
    seed_resource(engine, "resource-A")

    with pytest.raises(RuntimeError):
        with write_transaction(engine) as conn:
            result = link_existing_resource(
                conn,
                **SOURCE,
                resource_id="resource-A",
                now=NOW,
            )

            assert result == "CREATED"

            # Имитируем ошибку дальнейшей обработки.
            raise RuntimeError("Simulated failure")

    assert lookup(engine) is None


def test_link_requires_write_transaction(engine):
    seed_resource(engine, "resource-A")

    with read_transaction(engine) as conn:
        with pytest.raises(RuntimeError):
            link_existing_resource(
                conn,
                **SOURCE,
                resource_id="resource-A",
                now=NOW,
            )
