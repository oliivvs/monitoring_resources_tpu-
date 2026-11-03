
"""N11: дополнительные проверки автомата статусов без сети."""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)
from services.resources import upsert_configuration
from services.status import apply_check_result
from shared.schemas import CheckResultIn, ResourceConfig


BASE = datetime(2026, 11, 3, 9, 0, tzinfo=timezone.utc)


@pytest.fixture
def engine(tmp_path):
    # Каждый тест получает собственную временную SQLite.
    db = make_engine(tmp_path / "n11.sqlite")
    initialize(db)
    yield db
    db.dispose()


def add_resource(
    engine,
    rid="web",
    *,
    up_after=2,
    down_after=3,
    check_type="HTTP",
):
    """Создаём ресурс и получаем ID версии конфигурации."""
    config = ResourceConfig(
        id=rid,
        name=rid,
        target=(
            "http://127.0.0.1:18080/health"
            if check_type == "HTTP"
            else "127.0.0.1"
        ),
        check_type=check_type,
        port=None if check_type == "HTTP" else 18080,
        expected_http_codes=(
            [{"start": 200, "end": 299}]
            if check_type == "HTTP"
            else []
        ),
        up_after=up_after,
        down_after=down_after,
    )

    with write_transaction(engine) as conn:
        version = upsert_configuration(conn, config, now=BASE)

    return config, version


def send_result(
    engine,
    version,
    success,
    index,
    *,
    rid="web",
    check_type="HTTP",
):
    """Имитируем результат проверки без реального запроса."""
    result = CheckResultIn(
        resource_id=rid,
        config_version_id=version,
        checked_at=BASE + timedelta(seconds=index),
        success=success,
        http_status_code=(
            (200 if success else 503)
            if check_type == "HTTP"
            else None
        ),
        error_message=None if success else "Ошибка сети",
    )

    with write_transaction(engine) as conn:
        return apply_check_result(conn, result=result)


def state(engine, rid="web"):
    """Получаем текущее состояние и счётчики ресурса."""
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT current_status, ok_count, fail_count,
                       status_changed_at
                FROM resource_states
                WHERE resource_id = :rid
            """),
            {"rid": rid},
        ).mappings().one()


def event_count(engine, rid="web"):
    """Считаем только события реальной смены статуса."""
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT count(*)
                FROM outbox_events
                WHERE resource_id = :rid
                  AND event_type = 'STATUS_CHANGED'
            """),
            {"rid": rid},
        ).scalar_one()


# Проверка разных порогов перехода.
@pytest.mark.parametrize(
    "up_after,down_after,results,expected",
    [
        (
            1, 1,
            [True, False, True],
            ["UP", "DOWN", "UP"],
        ),
        (
            3, 2,
            [True, True, False, True, True, True, False, False],
            [
                "UNKNOWN", "UNKNOWN", "UNKNOWN",
                "UNKNOWN", "UNKNOWN", "UP", "UP", "DOWN",
            ],
        ),
        (
            2, 3,
            [False, False, True, False, False, False],
            [
                "UNKNOWN", "UNKNOWN", "UNKNOWN",
                "UNKNOWN", "UNKNOWN", "DOWN",
            ],
        ),
    ],
)
def test_configurable_thresholds_and_reset(
    engine, up_after, down_after, results, expected,
):
    _, version = add_resource(
        engine,
        up_after=up_after,
        down_after=down_after,
    )

    statuses = [
        send_result(engine, version, success, i + 1)[
            "current_status"
        ]
        for i, success in enumerate(results)
    ]

    assert statuses == expected

    # Событий должно быть столько же, сколько смен статуса.
    previous = ["UNKNOWN"] + expected[:-1]
    changes = sum(
        old != new
        for old, new in zip(previous, expected)
    )

    assert event_count(engine) == changes


def test_stable_status_does_not_create_duplicate_events(engine):
    """Повторные успехи не создают лишних событий UP."""
    _, version = add_resource(engine)

    outputs = [
        send_result(engine, version, True, i)
        for i in range(1, 6)
    ]

    assert [
        result["status_changed"] for result in outputs
    ] == [False, True, False, False, False]

    assert event_count(engine) == 1

    saved = state(engine)

    assert (
        saved["current_status"],
        saved["ok_count"],
        saved["fail_count"],
    ) == ("UP", 5, 0)

    # Статус подтвердился на второй проверке.
    expected_time = (
        BASE + timedelta(seconds=2)
    ).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")

    assert saved["status_changed_at"] == expected_time


def test_failed_sequence_resets_after_single_success(engine):
    """Один успех разрывает последовательность ошибок."""
    _, version = add_resource(engine)

    results = [False, False, True, False, False]

    for i, success in enumerate(results, start=1):
        send_result(engine, version, success, i)

    saved = state(engine)

    assert (
        saved["current_status"],
        saved["ok_count"],
        saved["fail_count"],
    ) == ("UNKNOWN", 0, 2)

    assert event_count(engine) == 0

    # Только третья ошибка после успеха подтверждает DOWN.
    send_result(engine, version, False, 6)

    assert state(engine)["current_status"] == "DOWN"
    assert event_count(engine) == 1


def test_two_resources_have_independent_counters(engine):
    """Проверки разных ресурсов не смешивают счётчики."""
    _, version_a = add_resource(engine, rid="a")
    _, version_b = add_resource(engine, rid="b")

    send_result(engine, version_a, True, 1, rid="a")
    send_result(engine, version_b, False, 2, rid="b")
    send_result(engine, version_a, True, 3, rid="a")
    send_result(engine, version_b, False, 4, rid="b")
    send_result(engine, version_b, False, 5, rid="b")

    assert state(engine, "a")["current_status"] == "UP"
    assert state(engine, "b")["current_status"] == "DOWN"

    assert event_count(engine, "a") == 1
    assert event_count(engine, "b") == 1


def test_tcp_thresholds_use_success_without_http_code(engine):
    """Для TCP действуют те же пороги, но HTTP-кода нет."""
    _, version = add_resource(
        engine, rid="tcp", check_type="TCP"
    )

    results = [True, True, False, False, False]

    outcomes = [
        send_result(
            engine,
            version,
            success,
            i,
            rid="tcp",
            check_type="TCP",
        )
        for i, success in enumerate(results, start=1)
    ]

    assert [
        outcome["current_status"] for outcome in outcomes
    ] == ["UNKNOWN", "UP", "UP", "UP", "DOWN"]

    assert event_count(engine, "tcp") == 2


def test_invalid_success_cannot_corrupt_state_or_outbox(engine):
    """HTTP 503 нельзя сохранить как успешную проверку."""
    _, version = add_resource(engine)

    with pytest.raises(
        ValueError, match="HTTP-код не допускается"
    ):
        with write_transaction(engine) as conn:
            apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id="web",
                    config_version_id=version,
                    checked_at=BASE + timedelta(seconds=1),
                    success=True,
                    http_status_code=503,
                ),
            )

    assert state(engine)["current_status"] == "UNKNOWN"
    assert state(engine)["ok_count"] == 0
    assert event_count(engine) == 0

    with read_transaction(engine) as conn:
        checks = conn.execute(
            text("SELECT count(*) FROM check_results")
        ).scalar_one()

    assert checks == 0


def test_reconfigured_threshold_starts_new_series_without_spurious_event(
    engine,
):
    """Новый порог сбрасывает серию, но сохраняет статус."""
    config, v1 = add_resource(engine)

    send_result(engine, v1, True, 1)
    send_result(engine, v1, True, 2)

    assert state(engine)["current_status"] == "UP"

    # Меняем порог DOWN с трёх ошибок на две.
    with write_transaction(engine) as conn:
        v2 = upsert_configuration(
            conn,
            config.model_copy(
                update={"down_after": 2}
            ),
            now=BASE + timedelta(seconds=3),
        )

    assert v1 != v2

    saved = state(engine)

    assert saved["current_status"] == "UP"
    assert saved["ok_count"] == 0
    assert saved["fail_count"] == 0
    assert event_count(engine) == 1

    send_result(engine, v2, False, 4)
    assert state(engine)["current_status"] == "UP"

    send_result(engine, v2, False, 5)
    assert state(engine)["current_status"] == "DOWN"
    assert event_count(engine) == 2
