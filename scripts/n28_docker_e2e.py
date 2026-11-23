
"""N28: проверка реальных Docker-процессов HEARTBEAT и Sender.

Запускать только через изолированный тестовый Docker Compose.
Рабочие базы и реальные сервисы ТПУ не используются.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from app.database import get_engine
from db.session import read_transaction, write_transaction
from integrations.oko.event_contract import validate_oko_payload
from services.resources import upsert_configuration
from services.status import apply_check_result
from shared.schemas import CheckResultIn, ResourceConfig, to_utc_iso


RESOURCE_ID = "n28-docker-heartbeat"
EXPECTED_DATABASE = "sqlite:////data/monitoring.db"


def check_environment() -> None:
    """Запрещаем случайный запуск на рабочей базе."""

    if os.getenv("N28_E2E_ENABLED") != "true":
        raise RuntimeError(
            "N28 запрещён вне специального тестового запуска"
        )

    if os.getenv("MONITORING_DATABASE_URL") != EXPECTED_DATABASE:
        raise RuntimeError(
            "N28 требует тестовую SQLite внутри Docker"
        )

    if not Path("/data/monitoring.db").is_file():
        raise RuntimeError("Тестовая база не найдена")


def read_events(engine):
    with read_transaction(engine) as conn:
        rows = conn.execute(
            text("""
                SELECT
                    event_id,
                    event_type,
                    payload_json,
                    checked_at,
                    delivery_status,
                    attempts
                FROM outbox_events
                WHERE resource_id = :rid
                ORDER BY created_at, event_id
            """),
            {"rid": RESOURCE_ID},
        ).mappings().all()

    return [dict(row) for row in rows]


def main() -> int:
    check_environment()

    engine = get_engine()

    try:
        # Не допускаем запуск на уже заполненной БД.
        with read_transaction(engine) as conn:
            resource_count = conn.execute(
                text("SELECT COUNT(*) FROM resources")
            ).scalar_one()

            outbox_count = conn.execute(
                text("SELECT COUNT(*) FROM outbox_events")
            ).scalar_one()

            inbox_count = conn.execute(
                text("SELECT COUNT(*) FROM inbox_events")
            ).scalar_one()

        if resource_count or outbox_count or inbox_count:
            raise RuntimeError(
                "N28 требует новую пустую тестовую БД"
            )

        # Проверка происходит в прошлом, чтобы HEARTBEAT
        # уже был положен по времени, когда worker её увидит.
        base = datetime.now(timezone.utc) - timedelta(
            seconds=90
        )
        checked_at = base + timedelta(seconds=1)

        config = ResourceConfig(
            id=RESOURCE_ID,
            name="Тестовый HEARTBEAT Docker",
            target="https://example.org/health",
            check_type="HTTP",
            active=True,
            interval_seconds=3600,
            heartbeat_interval_seconds=60,
            up_after=1,
            down_after=1,
        )

        with write_transaction(engine) as conn:
            version = upsert_configuration(
                conn,
                config,
                now=base,
            )

        # Фиксируем результат реальной по модели проверки.
        # Никакого сетевого обращения к example.org нет.
        with write_transaction(engine) as conn:
            result = apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id=RESOURCE_ID,
                    config_version_id=version,
                    checked_at=checked_at,
                    success=True,
                    http_status_code=200,
                    response_time_ms=15.0,
                ),
            )

        assert result["current_status"] == "UP"
        assert result["status_changed"] is True

        print("PASS: STATUS_CHANGED создан", flush=True)

        # HEARTBEAT и Sender здесь не вызываются!
        # Ждём, пока отдельные Docker-контейнеры
        # поставят HEARTBEAT и доставят оба события.
        deadline = time.monotonic() + 55
        events = []

        while time.monotonic() < deadline:
            events = read_events(engine)

            kinds = {
                item["event_type"]
                for item in events
            }

            if (
                len(events) == 2
                and kinds == {
                    "STATUS_CHANGED",
                    "HEARTBEAT",
                }
                and all(
                    item["delivery_status"] == "SENT"
                    for item in events
                )
            ):
                break

            time.sleep(0.5)
        else:
            summary = [
                (
                    item["event_type"],
                    item["delivery_status"],
                    item["attempts"],
                )
                for item in events
            ]

            raise RuntimeError(
                "Не дождались HEARTBEAT и доставки: "
                f"{summary}"
            )

        # Проверяем контракт и сохранение checked_at.
        for event in events:
            payload = json.loads(
                validate_oko_payload(
                    event["payload_json"]
                )
            )

            assert payload["event_id"] == event["event_id"]
            assert payload["resource_id"] == RESOURCE_ID
            assert payload["status"] == "UP"
            assert payload["checked_at"] == to_utc_iso(
                checked_at
            )
            assert event["attempts"] >= 1

        with read_transaction(engine) as conn:
            attempts = conn.execute(
                text("""
                    SELECT
                        e.event_type,
                        d.http_status_code
                    FROM outbox_events AS e
                    JOIN delivery_attempts AS d
                      ON d.event_id = e.event_id
                    WHERE e.resource_id = :rid
                    ORDER BY e.event_type, d.attempt_number
                """),
                {"rid": RESOURCE_ID},
            ).mappings().all()

            state = conn.execute(
                text("""
                    SELECT current_status, last_checked_at
                    FROM resource_states
                    WHERE resource_id = :rid
                """),
                {"rid": RESOURCE_ID},
            ).mappings().one()

            foreign_keys = conn.exec_driver_sql(
                "PRAGMA foreign_key_check"
            ).fetchall()

        assert len(attempts) == 2
        assert all(
            row["http_status_code"] == 202
            for row in attempts
        )

        assert state["current_status"] == "UP"
        assert state["last_checked_at"] == to_utc_iso(
            checked_at
        )
        assert not foreign_keys

        print(
            "PASS: отдельный контейнер HEARTBEAT "
            "создал событие",
            flush=True,
        )
        print(
            "PASS: отдельный Sender доставил "
            "оба события, HTTP 202",
            flush=True,
        )
        print(
            "PASS: JSON ОКО, checked_at, "
            "SQLite и внешние ключи",
            flush=True,
        )
        print("N28: SUCCESS", flush=True)

        return 0

    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
