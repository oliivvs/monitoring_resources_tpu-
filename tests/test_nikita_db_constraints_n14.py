
"""N14: ограничения SQLite и транзакционный откат (без внешней сети)."""

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db.session import initialize, make_engine, read_transaction, write_transaction
from services.resources import upsert_configuration
from services.status import apply_check_result
from shared.schemas import CheckResultIn, ResourceConfig, to_utc_iso


BASE = datetime(2026, 11, 3, 8, 0, tzinfo=timezone.utc)


@pytest.fixture
def database(tmp_path):
    # Изолированная БД: изменения не затрагивают monitoring.db.
    engine = make_engine(tmp_path / "n14.sqlite")
    initialize(engine)
    versions = {}

    with write_transaction(engine) as conn:
        for name in ("a", "b"):
            versions[name] = upsert_configuration(
                conn,
                ResourceConfig(
                    id=name,
                    name=f"Ресурс {name}",
                    target=f"http://127.0.0.1:18080/{name}",
                    check_type="HTTP",
                ),
                now=BASE,
            )

    yield engine, versions
    engine.dispose()


def read_one(engine, sql, **params):
    """Читаем одну запись из БД."""
    with read_transaction(engine) as conn:
        return conn.execute(
            text(sql), params
        ).mappings().one()


def count(engine, table):
    """Подсчитываем записи в тестовой таблице."""
    # Имена таблиц в этом тесте задаются только константами.
    with read_transaction(engine) as conn:
        return conn.execute(
            text(f"SELECT count(*) FROM {table}")
        ).scalar_one()


def save_check(engine, version, number):
    """Сохраняем успешную проверку через общую функцию проекта."""
    with write_transaction(engine) as conn:
        return apply_check_result(
            conn,
            result=CheckResultIn(
                resource_id="a",
                config_version_id=version,
                checked_at=BASE + timedelta(seconds=number),
                success=True,
                http_status_code=200,
            ),
        )


@pytest.mark.parametrize(
    "column,value",
    [
        ("current_status", "BROKEN"),
        ("ok_count", -1),
        ("fail_count", -1),
        ("revision", -1),
    ],
)
def test_invalid_resource_state_is_rejected(database, column, value):
    """CHECK не позволяет сохранить недопустимое состояние."""
    engine, _ = database

    before = read_one(
        engine,
        "SELECT * FROM resource_states WHERE resource_id='a'",
    )

    # Имя столбца берём из фиксированного списка параметризации.
    with pytest.raises(IntegrityError):
        with write_transaction(engine) as conn:
            conn.execute(
                text(
                    f"UPDATE resource_states SET {column}=:value "
                    "WHERE resource_id='a'"
                ),
                {"value": value},
            )

    after = read_one(
        engine,
        "SELECT * FROM resource_states WHERE resource_id='a'",
    )

    assert after == before


def test_counters_cannot_be_positive_together(database):
    """Серии успехов и ошибок не могут быть ненулевыми вместе."""
    engine, _ = database

    with pytest.raises(IntegrityError):
        with write_transaction(engine) as conn:
            conn.execute(text("""
                UPDATE resource_states
                SET ok_count=1, fail_count=1
                WHERE resource_id='a'
            """))

    row = read_one(
        engine,
        "SELECT ok_count, fail_count "
        "FROM resource_states WHERE resource_id='a'",
    )

    assert (row["ok_count"], row["fail_count"]) == (0, 0)


@pytest.mark.parametrize(
    "column,value",
    [
        ("interval_seconds", 0),
        ("timeout_seconds", 0),
        ("up_after", 0),
        ("down_after", 0),
        ("heartbeat_interval_seconds", 0),
    ],
)
def test_invalid_configuration_parameters_are_rejected(
    database, column, value,
):
    """Даже прямой SQL не обходит ограничения конфигурации."""
    engine, versions = database

    with pytest.raises(IntegrityError):
        with write_transaction(engine) as conn:
            conn.execute(
                text(
                    f"UPDATE resource_config_versions "
                    f"SET {column}=:value WHERE id=:id"
                ),
                {
                    "value": value,
                    "id": versions["a"],
                },
            )

    row = read_one(
        engine,
        "SELECT interval_seconds, timeout_seconds, up_after, "
        "down_after, heartbeat_interval_seconds "
        "FROM resource_config_versions WHERE id=:id",
        id=versions["a"],
    )

    assert all(row[k] > 0 for k in row.keys())


def test_version_from_another_resource_is_rejected(database):
    """Составной FK связывает состояние с версией того же ресурса."""
    engine, versions = database

    with pytest.raises(IntegrityError):
        with write_transaction(engine) as conn:
            conn.execute(
                text("""
                    UPDATE resource_states
                    SET config_version_id=:wrong
                    WHERE resource_id='a'
                """),
                {"wrong": versions["b"]},
            )

    row = read_one(
        engine,
        "SELECT config_version_id "
        "FROM resource_states WHERE resource_id='a'",
    )

    assert row["config_version_id"] == versions["a"]


def test_two_active_versions_are_rejected(database):
    """UNIQUE запрещает две действующие версии одного ресурса."""
    engine, versions = database

    with pytest.raises(IntegrityError):
        with write_transaction(engine) as conn:
            conn.execute(
                text("""
                    INSERT INTO resource_config_versions (
                        resource_id, version_number, target,
                        check_type, port, interval_seconds,
                        timeout_seconds, up_after, down_after,
                        heartbeat_interval_seconds, effective_at
                    )
                    SELECT
                        resource_id,
                        version_number + 1,
                        target,
                        check_type,
                        port,
                        interval_seconds,
                        timeout_seconds,
                        up_after,
                        down_after,
                        heartbeat_interval_seconds,
                        effective_at
                    FROM resource_config_versions
                    WHERE id=:id
                """),
                {"id": versions["a"]},
            )

    assert count(engine, "resource_config_versions") == 2


def test_repeated_raw_check_is_rejected(database):
    """Уникальный ключ истории не допускает дубликаты времени."""
    engine, versions = database

    save_check(engine, versions["a"], 1)

    before = count(engine, "check_results")

    with pytest.raises(IntegrityError):
        with write_transaction(engine) as conn:
            conn.execute(
                text("""
                    INSERT INTO check_results (
                        resource_id,
                        config_version_id,
                        checked_at,
                        success,
                        resulting_status
                    )
                    VALUES (
                        'a', :version, :at, 1, 'UNKNOWN'
                    )
                """),
                {
                    "version": versions["a"],
                    "at": to_utc_iso(
                        BASE + timedelta(seconds=1)
                    ),
                },
            )

    assert count(engine, "check_results") == before


def test_exception_rolls_back_state_check_and_event(database):
    """Python-ошибка откатывает результат, статус и событие."""
    engine, versions = database

    # Первый успех ещё не переводит ресурс в UP.
    save_check(engine, versions["a"], 1)

    before = read_one(
        engine,
        "SELECT * FROM resource_states WHERE resource_id='a'",
    )

    with pytest.raises(RuntimeError, match="тестовый сбой"):
        with write_transaction(engine) as conn:
            result = apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id="a",
                    config_version_id=versions["a"],
                    checked_at=BASE + timedelta(seconds=2),
                    success=True,
                    http_status_code=200,
                ),
            )

            # Статус изменён внутри транзакции.
            assert result["current_status"] == "UP"

            # Имитируем ошибку до фиксации транзакции.
            raise RuntimeError("тестовый сбой")

    after = read_one(
        engine,
        "SELECT * FROM resource_states WHERE resource_id='a'",
    )

    assert after == before
    assert count(engine, "check_results") == 1
    assert count(engine, "status_transitions") == 0
    assert count(engine, "outbox_events") == 0


def test_outbox_event_payload_is_immutable(database):
    """Триггер запрещает менять сформированный JSON события."""
    engine, versions = database

    # После второго успеха появляется событие STATUS_CHANGED.
    save_check(engine, versions["a"], 1)
    save_check(engine, versions["a"], 2)

    before = read_one(
        engine,
        "SELECT event_id, payload_json FROM outbox_events",
    )

    with pytest.raises(IntegrityError):
        with write_transaction(engine) as conn:
            conn.execute(
                text("""
                    UPDATE outbox_events
                    SET payload_json='{}'
                    WHERE event_id=:id
                """),
                {"id": before["event_id"]},
            )

    after = read_one(
        engine,
        "SELECT event_id, payload_json FROM outbox_events",
    )

    assert after == before


@pytest.mark.parametrize(
    "changes",
    [
        {"attempts": -1},
        {"delivery_status": "SENT"},
        {"lease_owner": "worker-a"},
    ],
)
def test_invalid_delivery_state_is_rejected(database, changes):
    """Поля доставки Outbox должны оставаться согласованными."""
    engine, versions = database

    save_check(engine, versions["a"], 1)
    save_check(engine, versions["a"], 2)

    before = read_one(
        engine,
        "SELECT * FROM outbox_events",
    )

    # Столбцы берём только из фиксированной тестовой параметризации.
    columns = ", ".join(
        f"{key}=:{key}"
        for key in changes
    )

    with pytest.raises(IntegrityError):
        with write_transaction(engine) as conn:
            conn.execute(
                text(
                    f"UPDATE outbox_events SET {columns} "
                    "WHERE event_id=:id"
                ),
                {
                    **changes,
                    "id": before["event_id"],
                },
            )

    after = read_one(
        engine,
        "SELECT * FROM outbox_events",
    )

    assert after == before


def test_payload_fields_must_match_outbox_columns(database):
    """Нельзя вставить событие с чужим resource_id внутри JSON."""
    engine, _ = database

    event_id = str(uuid4())
    checked_at = to_utc_iso(
        BASE + timedelta(seconds=1)
    )

    payload = json.dumps({
        "event_id": event_id,
        "resource_id": "b",  # Намеренно не совпадает с 'a'.
        "status": "UNKNOWN",
        "event_type": "HEARTBEAT",
        "checked_at": checked_at,
    })

    with pytest.raises(IntegrityError):
        with write_transaction(engine) as conn:
            conn.execute(
                text("""
                    INSERT INTO outbox_events (
                        event_id,
                        resource_id,
                        event_type,
                        status,
                        checked_at,
                        payload_json,
                        created_at
                    )
                    VALUES (
                        :eid,
                        'a',
                        'HEARTBEAT',
                        'UNKNOWN',
                        :at,
                        :payload,
                        :at
                    )
                """),
                {
                    "eid": event_id,
                    "at": checked_at,
                    "payload": payload,
                },
            )

    assert count(engine, "outbox_events") == 0
