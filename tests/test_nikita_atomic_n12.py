
"""N12: интеграционные тесты атомарной записи проверок в SQLite."""
import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DatabaseError

import checks.runner as runner
from checks.security import NetworkPolicy, TargetNotAllowed
from db.session import initialize, make_engine, read_transaction, write_transaction
from services.resources import upsert_configuration
from shared.schemas import CheckResultIn, ResourceConfig


BASE = datetime(2026, 11, 3, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def engine(tmp_path):
    # Тесты не обращаются к рабочей базе или реальным серверам ТПУ.
    db = make_engine(tmp_path / "n12.sqlite")
    initialize(db)

    with write_transaction(db) as conn:
        upsert_configuration(
            conn,
            ResourceConfig(
                id="local-test",
                name="Локальный тест",
                target="https://example.org/health",
                check_type="HTTP",
            ),
            now=BASE,
        )
    yield db
    db.dispose()


def result_for(snapshot, number, *, success=True, code=None):
    # Метка версии всегда берётся из снимка, а не из текущей БД.
    return CheckResultIn(
        resource_id=snapshot.resource_id,
        config_version_id=snapshot.config_version_id,
        checked_at=BASE + timedelta(seconds=number),
        success=success,
        http_status_code=(200 if success else 503) if code is None else code,
        error_message=None if success else "HTTP error",
    )


def database_snapshot(engine):
    # Возвращаем состояние всех таблиц, затрагиваемых проверкой.
    with read_transaction(engine) as conn:
        state = dict(conn.execute(text("""
            SELECT current_status, ok_count, fail_count,
                   last_checked_at, status_changed_at, revision
            FROM resource_states WHERE resource_id='local-test'
        """)).mappings().one())
        counts = {
            table: conn.execute(
                text(f"SELECT count(*) FROM {table}")
            ).scalar_one()
            for table in (
                "check_results", "status_transitions", "outbox_events"
            )
        }
    return state, counts


@pytest.mark.asyncio
async def test_confirmed_up_saves_check_state_transition_and_outbox(engine, monkeypatch):
    """Подтверждённый UP сохраняет связанные записи одной операцией."""
    number = 0

    async def fake_probe(snapshot, policy):
        nonlocal number
        number += 1
        return result_for(snapshot, number)

    monkeypatch.setattr(runner, "probe", fake_probe)

    first = await runner.check_and_store(engine, "local-test", NetworkPolicy())
    second = await runner.check_and_store(engine, "local-test", NetworkPolicy())

    assert first.disposition == "APPLIED"
    assert first.status == "UNKNOWN"
    assert first.event_id is None
    assert second.disposition == "APPLIED"
    assert second.status == "UP"
    assert second.event_id is not None

    state, counts = database_snapshot(engine)
    assert state["current_status"] == "UP"
    assert (state["ok_count"], state["fail_count"]) == (2, 0)
    assert counts == {
        "check_results": 2,
        "status_transitions": 1,
        "outbox_events": 1,
    }

    with read_transaction(engine) as conn:
        record = conn.execute(text("""
            SELECT e.event_id, e.payload_json, e.delivery_status, e.attempts,
                   t.check_result_id, t.previous_status, t.new_status
            FROM outbox_events AS e
            JOIN status_transitions AS t ON t.id = e.status_transition_id
        """)).mappings().one()

    assert record["check_result_id"] == second.check_id
    assert (record["previous_status"], record["new_status"]) == (
        "UNKNOWN", "UP"
    )
    assert record["event_id"] == second.event_id
    assert json.loads(record["payload_json"])["event_id"] == second.event_id
    assert (record["delivery_status"], record["attempts"]) == ("PENDING", 0)


@pytest.mark.asyncio
async def test_outbox_failure_rolls_back_entire_async_check(engine, monkeypatch):
    """Если INSERT Outbox завершился ошибкой, остальные записи тоже откатываются."""
    number = 0

    async def fake_probe(snapshot, policy):
        nonlocal number
        number += 1
        return result_for(snapshot, number)

    monkeypatch.setattr(runner, "probe", fake_probe)

    first = await runner.check_and_store(engine, "local-test", NetworkPolicy())
    assert first.status == "UNKNOWN"
    before = database_snapshot(engine)

    # Сбой возникает после INSERT check_results и UPDATE resource_states.
    with write_transaction(engine) as conn:
        conn.exec_driver_sql("""
            CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox_events
            BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END
        """)

    with pytest.raises(DatabaseError):
        await runner.check_and_store(engine, "local-test", NetworkPolicy())

    assert database_snapshot(engine) == before

    # После устранения ошибки тот же ресурс должен проверяться дальше.
    with write_transaction(engine) as conn:
        conn.exec_driver_sql("DROP TRIGGER fail_outbox")

    retry = await runner.check_and_store(engine, "local-test", NetworkPolicy())
    assert retry.disposition == "APPLIED"
    assert retry.status == "UP"
    assert retry.event_id is not None


@pytest.mark.asyncio
async def test_invalid_http_result_does_not_leave_partial_data(engine, monkeypatch):
    """Нельзя сохранить HTTP 503 как успешную проверку."""
    async def fake_probe(snapshot, policy):
        return result_for(snapshot, 1, success=True, code=503)

    monkeypatch.setattr(runner, "probe", fake_probe)
    before = database_snapshot(engine)

    with pytest.raises(ValueError, match="HTTP-код не допускается"):
        await runner.check_and_store(engine, "local-test", NetworkPolicy())

    assert database_snapshot(engine) == before


@pytest.mark.asyncio
async def test_denied_check_does_not_touch_sqlite(engine, monkeypatch):
    """Запрет сетевой политики не должен считаться падением ресурса."""
    async def fake_probe(snapshot, policy):
        raise TargetNotAllowed("Адрес запрещён")

    monkeypatch.setattr(runner, "probe", fake_probe)
    before = database_snapshot(engine)

    outcome = await runner.check_and_store(engine, "local-test", NetworkPolicy())

    assert outcome.disposition == "DENIED"
    assert outcome.check_id is None
    assert database_snapshot(engine) == before


@pytest.mark.asyncio
async def test_parallel_equal_timestamp_only_one_result_saved(engine, monkeypatch):
    """Два результата с одинаковым временем не должны записаться дважды."""
    arrived = 0
    both_arrived = asyncio.Event()

    async def fake_probe(snapshot, policy):
        nonlocal arrived
        arrived += 1
        if arrived == 2:
            both_arrived.set()
        await both_arrived.wait()
        return result_for(snapshot, 1)

    monkeypatch.setattr(runner, "probe", fake_probe)

    outcomes = await asyncio.wait_for(asyncio.gather(
        runner.check_and_store(engine, "local-test", NetworkPolicy()),
        runner.check_and_store(engine, "local-test", NetworkPolicy()),
    ), timeout=3)

    assert {outcome.disposition for outcome in outcomes} == {"APPLIED", "STALE"}
    state, counts = database_snapshot(engine)
    assert (state["current_status"], state["ok_count"]) == ("UNKNOWN", 1)
    assert counts == {
        "check_results": 1,
        "status_transitions": 0,
        "outbox_events": 0,
    }


@pytest.mark.asyncio
async def test_cancel_during_probe_leaves_database_unchanged(engine, monkeypatch):
    """Отмена до получения сетевого результата ничего не сохраняет."""
    entered = asyncio.Event()

    async def fake_probe(snapshot, policy):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runner, "probe", fake_probe)
    before = database_snapshot(engine)

    task = asyncio.create_task(
        runner.check_and_store(engine, "local-test", NetworkPolicy())
    )
    await asyncio.wait_for(entered.wait(), timeout=2)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2)

    assert database_snapshot(engine) == before
