
"""N09: тесты ограничения параллельности и остановки."""
import asyncio

import pytest

from checks.runner import RunOutcome
from checks.scheduler import ResourceScheduler
from checks.security import NetworkPolicy
from db.session import initialize, make_engine, write_transaction
from services.resources import upsert_configuration
from shared.schemas import ResourceConfig


@pytest.fixture
def engine(tmp_path):
    # Каждому тесту выделяем собственную временную базу.
    db = make_engine(tmp_path / "n09.sqlite")
    initialize(db)
    yield db
    db.dispose()


def add_resource(engine, name, *, active=True):
    config = ResourceConfig(
        id=name,
        name=name,
        target="http://127.0.0.1:18080/health",
        check_type="HTTP",
        interval_seconds=30,
        active=active,
    )

    with write_transaction(engine) as conn:
        upsert_configuration(conn, config)

    return config


async def wait_for(predicate, timeout=2.0):
    """Ждём выполнения условия, не используя длительные sleep."""
    async def poll():
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


def make_scheduler(engine, checker, *, limit=2, shutdown=1.0):
    return ResourceScheduler(
        engine,
        NetworkPolicy(),
        refresh_seconds=0.02,
        max_concurrent_checks=limit,
        shutdown_timeout_seconds=shutdown,
        checker=checker,
    )


@pytest.mark.asyncio
async def test_global_limit_and_no_waiting_tasks(engine):
    """Из пяти ресурсов одновременно работают только два."""
    for name in ("a", "b", "c", "d", "e"):
        add_resource(engine, name)

    release = asyncio.Event()
    running = 0
    maximum = 0
    calls = []

    async def fake_check(engine, name, policy):
        nonlocal running, maximum

        running += 1
        maximum = max(maximum, running)
        calls.append(name)

        try:
            await release.wait()
        finally:
            running -= 1

        return RunOutcome(name, 1, "APPLIED")

    scheduler = make_scheduler(engine, fake_check, limit=2)
    task = asyncio.create_task(scheduler.run())

    try:
        await wait_for(lambda: len(calls) == 2)
        await asyncio.sleep(0.1)

        assert len(calls) == 2
        assert sum(
            job.task is not None
            for job in scheduler._jobs.values()
        ) == 2
        assert maximum == 2

    finally:
        release.set()
        scheduler.stop()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_waiting_resources_start_in_due_order(engine):
    """Проверяем порядок запуска ожидающих ресурсов."""
    for name in ("a", "b", "c"):
        add_resource(engine, name)

    gates = {
        name: asyncio.Event()
        for name in ("a", "b", "c")
    }
    calls = []

    async def fake_check(engine, name, policy):
        calls.append(name)
        await gates[name].wait()
        return RunOutcome(name, 1, "APPLIED")

    scheduler = make_scheduler(engine, fake_check, limit=1)
    task = asyncio.create_task(scheduler.run())

    try:
        await wait_for(lambda: calls == ["a"])

        gates["a"].set()
        await wait_for(lambda: calls == ["a", "b"])

        gates["b"].set()
        await wait_for(lambda: calls == ["a", "b", "c"])

    finally:
        for gate in gates.values():
            gate.set()

        scheduler.stop()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_failed_check_releases_slot(engine):
    """Ошибка одной проверки освобождает место для следующей."""
    add_resource(engine, "a")
    add_resource(engine, "b")
    calls = []

    async def fake_check(engine, name, policy):
        calls.append(name)

        if name == "a":
            raise RuntimeError("secret information")

        return RunOutcome(name, 1, "APPLIED")

    scheduler = make_scheduler(engine, fake_check, limit=1)
    task = asyncio.create_task(scheduler.run())

    try:
        await wait_for(lambda: "b" in calls)
        assert calls[:2] == ["a", "b"]

    finally:
        scheduler.stop()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_disabled_running_check_still_occupies_slot(engine):
    """Отключение ресурса не освобождает место преждевременно."""
    config_a = add_resource(engine, "a")
    add_resource(engine, "b")

    release = asyncio.Event()
    calls = []

    async def fake_check(engine, name, policy):
        calls.append(name)

        if name == "a":
            await release.wait()

        return RunOutcome(name, 1, "APPLIED")

    scheduler = make_scheduler(engine, fake_check, limit=1)
    task = asyncio.create_task(scheduler.run())

    try:
        await wait_for(lambda: calls == ["a"])

        with write_transaction(engine) as conn:
            upsert_configuration(
                conn,
                config_a.model_copy(update={"active": False}),
            )

        await asyncio.sleep(0.1)
        assert calls == ["a"]

        release.set()
        await wait_for(lambda: calls == ["a", "b"])

    finally:
        release.set()
        scheduler.stop()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_stop_does_not_start_waiting_checks(engine):
    """После stop() очередь больше не запускается."""
    for name in ("a", "b", "c"):
        add_resource(engine, name)

    release = asyncio.Event()
    calls = []

    async def fake_check(engine, name, policy):
        calls.append(name)
        await release.wait()
        return RunOutcome(name, 1, "APPLIED")

    scheduler = make_scheduler(engine, fake_check, limit=2)
    task = asyncio.create_task(scheduler.run())

    try:
        await wait_for(lambda: len(calls) == 2)

        scheduler.stop()
        await asyncio.sleep(0.1)

        assert not task.done()
        assert "c" not in calls

    finally:
        release.set()
        await asyncio.wait_for(task, 2)

    assert calls == ["a", "b"]


@pytest.mark.asyncio
async def test_shutdown_timeout_cancels_unfinished_check(engine):
    """После таймаута зависшая проверка получает отмену."""
    add_resource(engine, "a")

    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def fake_check(engine, name, policy):
        entered.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    scheduler = make_scheduler(
        engine, fake_check, limit=1, shutdown=0.1
    )
    task = asyncio.create_task(scheduler.run())

    await asyncio.wait_for(entered.wait(), 2)

    scheduler.stop()
    await asyncio.wait_for(task, 2)

    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_external_cancel_waits_for_running_check(engine):
    """Отмена самого планировщика учитывает текущие проверки."""
    add_resource(engine, "a")

    entered = asyncio.Event()
    release = asyncio.Event()

    async def fake_check(engine, name, policy):
        entered.set()
        await release.wait()
        return RunOutcome(name, 1, "APPLIED")

    scheduler = make_scheduler(
        engine, fake_check, shutdown=1.0
    )
    task = asyncio.create_task(scheduler.run())

    try:
        await asyncio.wait_for(entered.wait(), 2)

        task.cancel()
        await asyncio.sleep(0.1)

        assert not task.done()

    finally:
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)


@pytest.mark.parametrize(
    "wrong",
    [0, -1, 129, True, 1.5, "2"],
)
def test_invalid_limit_rejected(engine, wrong):
    """Невозможно задать некорректное количество задач."""
    with pytest.raises(ValueError, match="max_concurrent_checks"):
        ResourceScheduler(
            engine,
            NetworkPolicy(),
            max_concurrent_checks=wrong,
        )


@pytest.mark.parametrize(
    "wrong",
    [0, -1, 301, float("nan"), float("inf")],
)
def test_invalid_shutdown_timeout_rejected(engine, wrong):
    """Таймаут остановки должен быть конечным положительным числом."""
    with pytest.raises(ValueError, match="shutdown_timeout_seconds"):
        ResourceScheduler(
            engine,
            NetworkPolicy(),
            shutdown_timeout_seconds=wrong,
        )
