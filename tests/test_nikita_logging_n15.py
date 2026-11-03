
"""N15: безопасные логи и обработка ошибок мониторинга без сети."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

import checks.runner as runner
from checks.security import NetworkPolicy, TargetNotAllowed
from db.session import initialize, make_engine, read_transaction, write_transaction
from services.resources import upsert_configuration
from shared.schemas import CheckResultIn, ResourceConfig


BASE = datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def engine(tmp_path):
    # Каждому тесту — отдельная временная БД без внешних HTTP/TCP запросов.
    db = make_engine(tmp_path / "n15.sqlite")
    initialize(db)
    with write_transaction(db) as conn:
        upsert_configuration(
            conn,
            ResourceConfig(
                id="local-n15",
                name="Локальный ресурс",
                target="https://example.org/health?token=hidden-from-logs",
                check_type="HTTP",
            ),
            now=BASE,
        )
    yield db
    db.dispose()


def fake_result(snapshot, number, *, success=True, error=None):
    return CheckResultIn(
        resource_id=snapshot.resource_id,
        config_version_id=snapshot.config_version_id,
        checked_at=BASE + timedelta(seconds=number),
        success=success,
        response_time_ms=17.5,
        http_status_code=200 if success else 503,
        error_message=error,
    )


def count_checks(engine):
    with read_transaction(engine) as conn:
        return conn.execute(text("SELECT count(*) FROM check_results")).scalar_one()


@pytest.mark.asyncio
async def test_success_has_useful_metadata_without_url(engine, monkeypatch, caplog):
    """Лог успешной проверки содержит ID, статус и время, но не URL."""
    number = 0

    async def fake_probe(snapshot, policy):
        nonlocal number
        number += 1
        return fake_result(snapshot, number)

    monkeypatch.setattr(runner, "probe", fake_probe)
    with caplog.at_level(logging.INFO, logger="checks.runner"):
        first = await runner.check_and_store(engine, "local-n15", NetworkPolicy())
        second = await runner.check_and_store(engine, "local-n15", NetworkPolicy())

    assert first.status == "UNKNOWN"
    assert second.status == "UP"
    assert second.event_id is not None
    logs = [r for r in caplog.records if "outcome=APPLIED" in r.getMessage()]
    assert len(logs) == 2
    assert all(r.levelno == logging.INFO for r in logs)
    assert "status=UP" in logs[-1].getMessage()
    assert "success=True" in logs[-1].getMessage()
    assert "http_code=200" in logs[-1].getMessage()
    assert "duration_ms=17.5" in logs[-1].getMessage()
    assert "check_id=" in logs[-1].getMessage()
    assert "event_id=" in logs[-1].getMessage()
    assert "hidden-from-logs" not in caplog.text
    assert "https://example.org" not in caplog.text


@pytest.mark.asyncio
async def test_failed_probe_is_warning_but_still_saved(engine, monkeypatch, caplog):
    """Сбой доступности — предупреждение, а не программная ошибка."""
    async def fake_probe(snapshot, policy):
        return fake_result(
            snapshot, 1,
            success=False,
            error="secret-cookie-must-not-appear",
        )

    monkeypatch.setattr(runner, "probe", fake_probe)
    with caplog.at_level(logging.INFO, logger="checks.runner"):
        outcome = await runner.check_and_store(engine, "local-n15", NetworkPolicy())

    assert outcome.disposition == "APPLIED"
    assert outcome.status == "UNKNOWN"
    assert count_checks(engine) == 1
    record = next(r for r in caplog.records if "outcome=APPLIED" in r.getMessage())
    assert record.levelno == logging.WARNING
    assert "success=False" in record.getMessage()
    assert "http_code=503" in record.getMessage()
    assert "secret-cookie-must-not-appear" not in caplog.text


@pytest.mark.asyncio
async def test_denied_is_not_down_or_database_write(engine, monkeypatch, caplog):
    """Отклонение сетевой политики не является DOWN."""
    async def fake_probe(snapshot, policy):
        raise TargetNotAllowed("token=denied-private")

    monkeypatch.setattr(runner, "probe", fake_probe)
    with caplog.at_level(logging.INFO, logger="checks.runner"):
        outcome = await runner.check_and_store(engine, "local-n15", NetworkPolicy())

    assert outcome.disposition == "DENIED"
    assert count_checks(engine) == 0
    assert "outcome=DENIED" in caplog.text
    assert "token=denied-private" not in caplog.text


@pytest.mark.asyncio
async def test_missing_resource_logs_inactive(engine, caplog):
    """Неизвестный ресурс не делает сетевой запрос и не меняет БД."""
    with caplog.at_level(logging.INFO, logger="checks.runner"):
        outcome = await runner.check_and_store(engine, "missing", NetworkPolicy())

    assert outcome.disposition == "INACTIVE"
    assert count_checks(engine) == 0
    assert "outcome=INACTIVE" in caplog.text
    assert "error_type=LookupError" in caplog.text


@pytest.mark.asyncio
async def test_duplicate_is_logged_as_stale(engine, monkeypatch, caplog):
    """Один и тот же результат не записывается дважды."""
    async def fake_probe(snapshot, policy):
        return fake_result(snapshot, 1)

    monkeypatch.setattr(runner, "probe", fake_probe)
    with caplog.at_level(logging.INFO, logger="checks.runner"):
        first = await runner.check_and_store(engine, "local-n15", NetworkPolicy())
        second = await runner.check_and_store(engine, "local-n15", NetworkPolicy())

    assert first.disposition == "APPLIED"
    assert second.disposition == "STALE"
    assert count_checks(engine) == 1
    assert "outcome=STALE" in caplog.text
    assert "reason=StaleCheckResult" in caplog.text


@pytest.mark.asyncio
async def test_unexpected_probe_error_is_logged_without_message(engine, monkeypatch, caplog):
    """Сбой программы передаётся планировщику, детали ошибки не логируются."""
    async def fake_probe(snapshot, policy):
        raise RuntimeError("Authorization: Bearer private-token-123")

    monkeypatch.setattr(runner, "probe", fake_probe)
    with caplog.at_level(logging.INFO, logger="checks.runner"):
        with pytest.raises(RuntimeError, match="private-token-123"):
            await runner.check_and_store(engine, "local-n15", NetworkPolicy())

    assert count_checks(engine) == 0
    assert "outcome=ERROR phase=probe" in caplog.text
    assert "error_type=RuntimeError" in caplog.text
    assert "private-token-123" not in caplog.text


@pytest.mark.asyncio
async def test_persist_error_is_logged_and_propagated(engine, monkeypatch, caplog):
    """Неожиданная ошибка сохранения не маскируется под сетевой DOWN."""
    async def fake_probe(snapshot, policy):
        return fake_result(snapshot, 1)

    def broken_apply(conn, *, result):
        raise RuntimeError("sqlite-file-with-secret-path")

    monkeypatch.setattr(runner, "probe", fake_probe)
    monkeypatch.setattr(runner, "apply_check_result", broken_apply)

    with caplog.at_level(logging.INFO, logger="checks.runner"):
        with pytest.raises(RuntimeError, match="sqlite-file-with-secret-path"):
            await runner.check_and_store(engine, "local-n15", NetworkPolicy())

    assert count_checks(engine) == 0
    assert "outcome=ERROR phase=persist" in caplog.text
    assert "error_type=RuntimeError" in caplog.text
    assert "sqlite-file-with-secret-path" not in caplog.text


@pytest.mark.asyncio
async def test_cancelled_probe_propagates_cancellation(engine, monkeypatch, caplog):
    """Отмена проверки не превращается в ERROR или DOWN."""
    entered = asyncio.Event()

    async def fake_probe(snapshot, policy):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runner, "probe", fake_probe)
    task = asyncio.create_task(runner.check_and_store(engine, "local-n15", NetworkPolicy()))
    await asyncio.wait_for(entered.wait(), 2)

    with caplog.at_level(logging.INFO, logger="checks.runner"):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)

    assert "outcome=CANCELLED phase=probe" in caplog.text
    assert count_checks(engine) == 0
