
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


# A23.2.3: настройки клиента полного снимка.

def test_snapshot_disabled_by_default():
    from scripts.run_inbox_worker import build_snapshot_client

    assert build_snapshot_client(settings()) is None


def test_snapshot_requires_url():
    from scripts.run_inbox_worker import build_snapshot_client

    with pytest.raises(ValueError):
        build_snapshot_client(
            settings(snapshot_enabled=True)
        )


def test_snapshot_requires_token():
    from scripts.run_inbox_worker import build_snapshot_client

    with pytest.raises(ValueError):
        build_snapshot_client(
            settings(
                snapshot_enabled=True,
                snapshot_base_url="https://entity.example.org",
                snapshot_allowed_hosts="entity.example.org",
            )
        )


@pytest.mark.parametrize(
    "base_url",
    [
        "http://entity.example.org",
        "https://other.example.org",
        "https://entity.example.org:8443",
        "https://127.0.0.1",
        "https://entity.example.org/other-path",
    ],
)
def test_snapshot_rejects_unsafe_endpoint(base_url):
    from scripts.run_inbox_worker import build_snapshot_client

    with pytest.raises(ValueError):
        build_snapshot_client(
            settings(
                snapshot_enabled=True,
                snapshot_base_url=base_url,
                snapshot_allowed_hosts="entity.example.org",
                snapshot_token="test-only-secret",
            )
        )


def test_snapshot_client_created_for_allowed_https():
    from scripts.run_inbox_worker import build_snapshot_client
    from integrations.tpu.entity_client import TpuEntityClient

    client = build_snapshot_client(
        settings(
            snapshot_enabled=True,
            snapshot_base_url="https://entity.example.org",
            snapshot_allowed_hosts="entity.example.org",
            snapshot_token="test-only-secret",
        )
    )

    assert isinstance(client, TpuEntityClient)


def test_worker_receives_snapshot_client(engine):
    worker = build_worker(
        engine,
        settings(
            snapshot_enabled=True,
            snapshot_base_url="https://entity.example.org",
            snapshot_allowed_hosts="entity.example.org",
            snapshot_token="test-only-secret",
        ),
    )

    handler = worker._prepare.__self__

    assert handler._snapshot_client is not None






def test_worker_rejects_prepare_timeout_near_lease(engine):
    with pytest.raises(ValueError, match="lease_seconds"):
        build_worker(
            engine,
            settings(
                lease_seconds=60,
                prepare_timeout_seconds=55,
            ),
        )


def test_worker_rejects_snapshot_timeout_over_prepare(engine):
    with pytest.raises(ValueError, match="Snapshot API"):
        build_worker(
            engine,
            settings(
                snapshot_enabled=True,
                snapshot_base_url="https://entity.example.org",
                snapshot_allowed_hosts="entity.example.org",
                snapshot_token="test-only-secret",
                snapshot_timeout_seconds=20,
                prepare_timeout_seconds=15,
            ),
        )


# A25.1: настраиваемый маршрут Oracle Entity API.

def test_snapshot_records_path_defaults_to_mock():
    from scripts.run_inbox_worker import build_snapshot_client

    client = build_snapshot_client(
        settings(
            snapshot_enabled=True,
            snapshot_base_url="https://entity.example.org",
            snapshot_allowed_hosts="entity.example.org",
            snapshot_token="test-only-token",
        )
    )

    assert client._routes.records == (
        "/mock/tpu/entity/v1/entities/{entity_id}/records"
    )


def test_snapshot_records_path_can_be_configured():
    from scripts.run_inbox_worker import build_snapshot_client

    client = build_snapshot_client(
        settings(
            snapshot_enabled=True,
            snapshot_base_url="https://entity.example.org",
            snapshot_allowed_hosts="entity.example.org",
            snapshot_token="test-only-token",
            snapshot_records_path=(
                "/api/v1/entities/{entity_id}/records"
            ),
        )
    )

    assert client._routes.records == (
        "/api/v1/entities/{entity_id}/records"
    )


@pytest.mark.parametrize(
    "bad_path",
    [
        "https://evil.example.org/records/{entity_id}",
        "//evil.example.org/{entity_id}",
        "/api/../entities/{entity_id}",
        "/api/{entity_id}/records?token=secret",
        "/api/entities/records",
        "/api/{entity_id}/{entity_id}/records",
        "/api/%2F/{entity_id}",
    ],
)
def test_snapshot_records_path_rejects_unsafe_values(
    bad_path,
):
    from scripts.run_inbox_worker import build_snapshot_client

    with pytest.raises(
        ValueError,
        match="Некорректный маршрут Snapshot API",
    ):
        build_snapshot_client(
            settings(
                snapshot_enabled=True,
                snapshot_base_url="https://entity.example.org",
                snapshot_allowed_hosts="entity.example.org",
                snapshot_token="test-only-token",
                snapshot_records_path=bad_path,
            )
        )
