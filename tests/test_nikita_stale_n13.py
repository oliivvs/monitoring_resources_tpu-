
"""N13: устаревшие версии и повторные результаты не портят состояние."""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

import checks.runner as runner
from checks.security import NetworkPolicy
from db.session import initialize, make_engine, read_transaction, write_transaction
from services.resources import upsert_configuration
from services.status import StaleCheckResult, apply_check_result
from shared.schemas import CheckResultIn, ResourceConfig


BASE = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def context(tmp_path):
    # Используем отдельную SQLite и не обращаемся к ресурсам ТПУ.
    engine = make_engine(tmp_path / "n13.sqlite")
    initialize(engine)

    config = ResourceConfig(
        id="n13-local",
        name="Тестовый ресурс",
        target="https://example.org/health",
        check_type="HTTP",
    )

    with write_transaction(engine) as conn:
        version = upsert_configuration(conn, config, now=BASE)

    yield engine, config, version
    engine.dispose()


def check(snapshot, seconds, *, status_code=200):
    """Создаём результат с версией, прочитанной до проверки."""
    return CheckResultIn(
        resource_id=snapshot.resource_id,
        config_version_id=snapshot.config_version_id,
        checked_at=BASE + timedelta(seconds=seconds),
        success=True,
        http_status_code=status_code,
    )


def db_snapshot(engine):
    """Сохраняем состояние для сравнения до и после проверки."""
    with read_transaction(engine) as conn:
        state = dict(conn.execute(text("""
            SELECT config_version_id, current_status, ok_count, fail_count,
                   last_checked_at, status_changed_at, revision
            FROM resource_states WHERE resource_id='n13-local'
        """)).mappings().one())

        counts = tuple(
            conn.execute(text(
                f"SELECT count(*) FROM {table} WHERE resource_id='n13-local'"
            )).scalar_one()
            for table in (
                "check_results",
                "status_transitions",
                "outbox_events",
            )
        )

    return state, counts


@pytest.mark.asyncio
async def test_exact_duplicate_is_stale_and_does_not_write(
    context, monkeypatch,
):
    """Повтор с тем же временем не меняет БД."""
    engine, _, version = context

    async def fake_probe(snapshot, policy):
        return check(snapshot, 1)

    monkeypatch.setattr(runner, "probe", fake_probe)

    first = await runner.check_and_store(
        engine, "n13-local", NetworkPolicy()
    )
    before = db_snapshot(engine)

    duplicate = await runner.check_and_store(
        engine, "n13-local", NetworkPolicy()
    )

    assert first.disposition == "APPLIED"
    assert duplicate.disposition == "STALE"
    assert duplicate.config_version_id == version
    assert duplicate.check_id is None
    assert db_snapshot(engine) == before


@pytest.mark.asyncio
async def test_older_timestamp_is_rejected(context, monkeypatch):
    """Опоздавший старый результат не перезаписывает новый."""
    engine, _, _ = context
    when = iter([2, 1])

    async def fake_probe(snapshot, policy):
        return check(snapshot, next(when))

    monkeypatch.setattr(runner, "probe", fake_probe)

    newer = await runner.check_and_store(
        engine, "n13-local", NetworkPolicy()
    )
    before = db_snapshot(engine)

    older = await runner.check_and_store(
        engine, "n13-local", NetworkPolicy()
    )

    assert newer.disposition == "APPLIED"
    assert older.disposition == "STALE"
    assert db_snapshot(engine) == before


@pytest.mark.asyncio
async def test_newer_timestamp_is_accepted_after_duplicate(
    context, monkeypatch,
):
    """После отклонённого дубля новые проверки продолжают работать."""
    engine, _, _ = context
    when = iter([1, 1, 2])

    async def fake_probe(snapshot, policy):
        return check(snapshot, next(when))

    monkeypatch.setattr(runner, "probe", fake_probe)

    outcomes = [
        await runner.check_and_store(
            engine, "n13-local", NetworkPolicy()
        )
        for _ in range(3)
    ]

    assert [o.disposition for o in outcomes] == [
        "APPLIED", "STALE", "APPLIED"
    ]
    assert outcomes[-1].status == "UP"

    # Два результата, один переход статуса, одно событие.
    assert db_snapshot(engine)[1] == (2, 1, 1)


def test_duplicate_has_its_own_exception_type(context):
    """Обработчик выдаёт специальный тип ошибки вместо обычного ValueError."""
    engine, _, version = context

    with write_transaction(engine) as conn:
        apply_check_result(
            conn,
            result=CheckResultIn(
                resource_id="n13-local",
                config_version_id=version,
                checked_at=BASE + timedelta(seconds=1),
                success=True,
                http_status_code=200,
            ),
        )

    before = db_snapshot(engine)

    with pytest.raises(
        StaleCheckResult,
        match="Повторная или устаревшая",
    ):
        with write_transaction(engine) as conn:
            apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id="n13-local",
                    config_version_id=version,
                    checked_at=BASE + timedelta(seconds=1),
                    success=True,
                    http_status_code=200,
                ),
            )

    assert db_snapshot(engine) == before


@pytest.mark.asyncio
async def test_changed_configuration_rejects_inflight_result(
    context, monkeypatch,
):
    """Результат старой версии не применяется после обновления настроек."""
    engine, config, old_version = context

    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_probe(snapshot, policy):
        started.set()
        await release.wait()
        return check(snapshot, 20)

    monkeypatch.setattr(runner, "probe", fake_probe)

    task = asyncio.create_task(
        runner.check_and_store(
            engine, "n13-local", NetworkPolicy()
        )
    )

    try:
        await asyncio.wait_for(started.wait(), 2)

        # Пока старая проверка ждёт ответа, меняем интервал.
        with write_transaction(engine) as conn:
            new_version = upsert_configuration(
                conn,
                config.model_copy(
                    update={"interval_seconds": 120}
                ),
                now=BASE + timedelta(seconds=10),
            )

        assert new_version != old_version
        before = db_snapshot(engine)

    finally:
        release.set()

    outcome = await asyncio.wait_for(task, 2)

    assert outcome.disposition == "STALE"
    assert outcome.config_version_id == old_version
    assert db_snapshot(engine) == before


@pytest.mark.asyncio
async def test_old_result_cannot_overwrite_new_version_result(
    context, monkeypatch,
):
    """Новая версия сохраняется, даже если старая завершилась позже."""
    engine, config, old_version = context

    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_probe(snapshot, policy):
        if snapshot.config_version_id == old_version:
            started.set()
            await release.wait()

        return check(
            snapshot,
            20 if snapshot.config_version_id == old_version else 15,
        )

    monkeypatch.setattr(runner, "probe", fake_probe)

    old_task = asyncio.create_task(
        runner.check_and_store(
            engine, "n13-local", NetworkPolicy()
        )
    )

    try:
        await asyncio.wait_for(started.wait(), 2)

        with write_transaction(engine) as conn:
            upsert_configuration(
                conn,
                config.model_copy(
                    update={"timeout_seconds": 8}
                ),
                now=BASE + timedelta(seconds=10),
            )

        current = await runner.check_and_store(
            engine, "n13-local", NetworkPolicy()
        )

        assert current.disposition == "APPLIED"
        before = db_snapshot(engine)

    finally:
        release.set()

    old = await asyncio.wait_for(old_task, 2)

    assert old.disposition == "STALE"
    assert db_snapshot(engine) == before


@pytest.mark.asyncio
async def test_disable_during_probe_does_not_save_result(
    context, monkeypatch,
):
    """Отключение ресурса делает выполнявшуюся проверку устаревшей."""
    engine, config, _ = context

    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_probe(snapshot, policy):
        started.set()
        await release.wait()
        return check(snapshot, 20)

    monkeypatch.setattr(runner, "probe", fake_probe)

    task = asyncio.create_task(
        runner.check_and_store(
            engine, "n13-local", NetworkPolicy()
        )
    )

    try:
        await asyncio.wait_for(started.wait(), 2)

        with write_transaction(engine) as conn:
            upsert_configuration(
                conn,
                config.model_copy(
                    update={"active": False}
                ),
                now=BASE + timedelta(seconds=10),
            )

        before = db_snapshot(engine)

    finally:
        release.set()

    outcome = await asyncio.wait_for(task, 2)

    assert outcome.disposition == "STALE"
    assert db_snapshot(engine) == before


@pytest.mark.asyncio
async def test_other_validation_error_is_not_mistaken_for_stale(
    context, monkeypatch,
):
    """Недопустимый HTTP-ответ остаётся ошибкой валидации."""
    engine, _, _ = context

    async def fake_probe(snapshot, policy):
        return check(snapshot, 1, status_code=503)

    monkeypatch.setattr(runner, "probe", fake_probe)
    before = db_snapshot(engine)

    with pytest.raises(
        ValueError,
        match="HTTP-код не допускается",
    ):
        await runner.check_and_store(
            engine, "n13-local", NetworkPolicy()
        )

    assert db_snapshot(engine) == before
