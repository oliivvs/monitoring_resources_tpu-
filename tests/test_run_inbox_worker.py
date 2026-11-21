
"""Проверки настроек и запуска Oracle Inbox Worker."""

from __future__ import annotations

import asyncio

from importlib import import_module
from pathlib import Path

import pytest

from alembic.migration import MigrationContext
from alembic.operations import Operations

from db.session import initialize, make_engine

from scripts.run_inbox_worker import (
    OracleWorkerSettings,
    build_network_policy,
    build_worker,
    verify_database,
)


MAPPING = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "tpu_oracle_mapping.yaml"
)


@pytest.fixture
def engine(tmp_path):
    db = make_engine(tmp_path / "worker.sqlite")
    initialize(db)

    yield db

    db.dispose()


def settings(**changes):
    values = {
        "enabled": True,
        "mapping_path": MAPPING,
        "allowed_hosts": "example.org",
        "allowed_ports": "443",
        "poll_seconds": 0.01,
    }

    values.update(changes)

    return OracleWorkerSettings(
        _env_file=None,
        **values,
    )


def upgrade_database(engine):
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


def test_disabled_by_default(engine):
    config = settings(enabled=False)

    with pytest.raises(RuntimeError):
        build_worker(engine, config)


def test_empty_allowlist_rejected(engine):
    config = settings(
        allowed_hosts="",
        allowed_ports="",
    )

    with pytest.raises(ValueError):
        build_worker(engine, config)


def test_invalid_ports_rejected():
    with pytest.raises(ValueError):
        build_network_policy(
            settings(allowed_ports="not-a-port")
        )


def test_mapping_file_required(engine):
    config = settings(
        mapping_path=Path(
            "/nonexistent/oracle_mapping.yaml"
        )
    )

    with pytest.raises(FileNotFoundError):
        build_worker(engine, config)


def test_old_database_rejected(engine):
    # initialize создаёт базовую схему,
    # но не отмечает миграцию 005.
    with pytest.raises(Exception):
        verify_database(engine)


@pytest.mark.asyncio
async def test_worker_starts_and_stops(engine):
    upgrade_database(engine)

    worker = build_worker(
        engine,
        settings(),
    )

    # На базе нет входящих событий.
    # Никаких внешних сетевых обращений не будет.
    task = asyncio.create_task(worker.run())

    try:
        await asyncio.sleep(0.05)
        assert not task.done()

    finally:
        worker.stop()
        await asyncio.wait_for(
            task,
            timeout=3,
        )

    assert task.done()
