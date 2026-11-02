
"""N08: тесты планировщика без внешних сетевых запросов."""
import asyncio

import pytest

from checks.runner import RunOutcome
from checks.scheduler import ResourceScheduler, load_active_schedules
from checks.security import NetworkPolicy
from db.session import initialize, make_engine, write_transaction
from services.resources import upsert_configuration
from shared.schemas import ResourceConfig


@pytest.fixture
def engine(tmp_path):
    # Для каждого теста создаётся отдельная временная SQLite.
    db = make_engine(tmp_path / "schedule.sqlite")
    initialize(db)
    yield db
    db.dispose()


def resource(resource_id, *, interval=1, active=True):
    return ResourceConfig(
        id=resource_id,
        name=resource_id,
        target="http://127.0.0.1:18080/health",
        check_type="HTTP",
        interval_seconds=interval,
        active=active,
    )


def save(engine, config):
    with write_transaction(engine) as conn:
        return upsert_configuration(conn, config)


async def wait_until(predicate, *, timeout=2):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


def test_active_only_and_version_refresh(engine):
    a = resource("a")
    save(engine, a)
    save(engine, resource("b", active=False))

    rows = load_active_schedules(engine)
    assert [r.resource_id for r in rows] == ["a"]
    assert rows[0].interval_seconds == 1

    version = rows[0].config_version_id
    save(engine, a.model_copy(update={"interval_seconds": 2}))

    rows = load_active_schedules(engine)
    assert rows[0].config_version_id != version
    assert rows[0].interval_seconds == 2


@pytest.mark.asyncio
async def test_runs_enabled_resource_and_stops(engine):
    save(engine, resource("a"))
    calls = []
    started = asyncio.Event()

    async def fake_check(engine, rid, policy):
        calls.append(rid)
        started.set()
        return RunOutcome(rid, 1, "APPLIED")

    scheduler = ResourceScheduler(
        engine, NetworkPolicy(),
        refresh_seconds=0.02,
        checker=fake_check,
    )
    task = asyncio.create_task(scheduler.run())

    try:
        await asyncio.wait_for(started.wait(), 2)
    finally:
        scheduler.stop()
        await asyncio.wait_for(task, 2)

    assert calls == ["a"]


@pytest.mark.asyncio
async def test_one_failure_does_not_block_other_resource(engine):
    save(engine, resource("broken"))
    save(engine, resource("healthy"))
    calls = set()

    async def fake_check(engine, rid, policy):
        calls.add(rid)
        if rid == "broken":
            raise RuntimeError("private details must not be logged")
        return RunOutcome(rid, 1, "APPLIED")

    scheduler = ResourceScheduler(
        engine, NetworkPolicy(),
        refresh_seconds=0.02,
        checker=fake_check,
    )
    task = asyncio.create_task(scheduler.run())

    try:
        await wait_until(
            lambda: calls == {"broken", "healthy"}
        )
    finally:
        scheduler.stop()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_no_overlap_for_same_resource(engine):
    save(engine, resource("slow"))
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fake_check(engine, rid, policy):
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return RunOutcome(rid, 1, "APPLIED")

    scheduler = ResourceScheduler(
        engine, NetworkPolicy(),
        refresh_seconds=0.02,
        checker=fake_check,
    )
    task = asyncio.create_task(scheduler.run())

    try:
        await asyncio.wait_for(started.wait(), 2)

        # Интервал прошёл, но предыдущая проверка ещё работает.
        await asyncio.sleep(1.1)
        assert calls == 1
    finally:
        release.set()
        scheduler.stop()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_newly_enabled_resource_discovered(engine):
    save(engine, resource("later", active=False))
    called = asyncio.Event()

    async def fake_check(engine, rid, policy):
        called.set()
        return RunOutcome(rid, 1, "APPLIED")

    scheduler = ResourceScheduler(
        engine, NetworkPolicy(),
        refresh_seconds=0.02,
        checker=fake_check,
    )
    task = asyncio.create_task(scheduler.run())

    try:
        await asyncio.sleep(0.1)
        assert not called.is_set()

        save(engine, resource("later", active=True))
        await asyncio.wait_for(called.wait(), 2)
    finally:
        scheduler.stop()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_version_change_during_running_check_triggers_new_run(engine):
    config = resource("a")
    save(engine, config)
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fake_check(engine, rid, policy):
        nonlocal calls
        calls += 1
        entered.set()

        if calls == 1:
            await release.wait()

        return RunOutcome(
            rid, 1,
            "STALE" if calls == 1 else "APPLIED",
        )

    scheduler = ResourceScheduler(
        engine, NetworkPolicy(),
        refresh_seconds=0.02,
        checker=fake_check,
    )
    task = asyncio.create_task(scheduler.run())

    try:
        await asyncio.wait_for(entered.wait(), 2)

        # Пока v1 выполняется, создаём конфигурацию v2.
        save(engine, config.model_copy(
            update={"interval_seconds": 2}
        ))
        await asyncio.sleep(0.1)

        release.set()
        await wait_until(lambda: calls == 2)
        assert calls == 2
    finally:
        release.set()
        scheduler.stop()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_disabling_resource_stops_future_launches(engine):
    config = resource("a")
    save(engine, config)
    first = asyncio.Event()
    calls = 0

    async def fake_check(engine, rid, policy):
        nonlocal calls
        calls += 1
        first.set()
        return RunOutcome(rid, 1, "APPLIED")

    scheduler = ResourceScheduler(
        engine, NetworkPolicy(),
        refresh_seconds=0.02,
        checker=fake_check,
    )
    task = asyncio.create_task(scheduler.run())

    try:
        await asyncio.wait_for(first.wait(), 2)

        save(engine, config.model_copy(update={"active": False}))
        await asyncio.sleep(1.15)

        assert calls == 1
    finally:
        scheduler.stop()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_stop_waits_for_running_check(engine):
    save(engine, resource("a"))
    entered = asyncio.Event()
    release = asyncio.Event()

    async def fake_check(engine, rid, policy):
        entered.set()
        await release.wait()
        return RunOutcome(rid, 1, "APPLIED")

    scheduler = ResourceScheduler(
        engine, NetworkPolicy(),
        refresh_seconds=0.02,
        checker=fake_check,
    )
    task = asyncio.create_task(scheduler.run())

    try:
        await asyncio.wait_for(entered.wait(), 2)
        scheduler.stop()

        await asyncio.sleep(0.1)
        assert not task.done()
    finally:
        release.set()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_transient_database_read_failure_is_retried(
    engine, monkeypatch,
):
    save(engine, resource("a"))

    import checks.scheduler as module

    original = module.load_active_schedules
    attempts = 0
    called = asyncio.Event()

    def flaky_load(engine):
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            raise RuntimeError("credential-looking-string-not-for-logs")

        return original(engine)

    async def fake_check(engine, rid, policy):
        called.set()
        return RunOutcome(rid, 1, "APPLIED")

    monkeypatch.setattr(
        module, "load_active_schedules", flaky_load
    )

    scheduler = ResourceScheduler(
        engine, NetworkPolicy(),
        refresh_seconds=0.02,
        checker=fake_check,
    )
    task = asyncio.create_task(scheduler.run())

    try:
        await asyncio.wait_for(called.wait(), 2)
        assert attempts >= 2
    finally:
        scheduler.stop()
        await asyncio.wait_for(task, 2)


def test_reject_bad_refresh_interval(engine):
    with pytest.raises(ValueError):
        ResourceScheduler(
            engine,
            NetworkPolicy(),
            refresh_seconds=0,
        )
