
"""N26: HEARTBEAT -> SQLite Outbox -> HTTP sender -> Mock API.

Запуск из корня проекта:
    python -m scripts.heartbeat_e2e

Только временная SQLite и HTTP-транспорт в памяти, без внешней сети.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.config import AppSettings
from checks.heartbeat_worker import HeartbeatWorker
from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)
from integrations.oko.event_contract import validate_oko_payload
from mock_api.main import create_mock_app
from services.resources import upsert_configuration
from services.sender import send_one_event
from services.status import apply_check_result
from shared.schemas import (
    CheckResultIn,
    ResourceConfig,
    to_utc_iso,
)


BASE = datetime(2026, 11, 23, 10, 0, tzinfo=timezone.utc)
TOKEN = "n26-local-test-token"
POST_PATH = "/api/v1/status/events"
POST_URL = "https://oko.example" + POST_PATH


@dataclass(frozen=True, slots=True)
class SmokeResult:
    status_event_id: str
    heartbeat_event_id: str
    http_attempts: int
    heartbeat_attempts: int
    duplicate_http_status: int
    conflicting_http_status: int


def assert_http(
    response: httpx.Response,
    expected: int,
    step: str,
) -> None:
    """Показываем HTTP-ответ при сбое, а не голый AssertionError."""

    assert response.status_code == expected, (
        f"{step}: ожидали HTTP {expected}, "
        f"получили HTTP {response.status_code}; "
        f"ответ: {response.text[:1000]}"
    )


def run_smoke() -> SmokeResult:
    """Проверяет формирование и доставку событий на временной БД."""

    with tempfile.TemporaryDirectory(
        prefix="tpu-n26-heartbeat-"
    ) as directory:

        engine = make_engine(
            Path(directory) / "smoke.sqlite"
        )

        try:
            initialize(engine)

            # Создаём тестовый ресурс.
            with write_transaction(engine) as conn:
                version = upsert_configuration(
                    conn,
                    ResourceConfig(
                        id="n26-service",
                        name="Тестовый сервис N26",
                        target="https://example.org/health",
                        check_type="HTTP",
                        up_after=1,
                        down_after=1,
                        heartbeat_interval_seconds=60,
                    ),
                    now=BASE,
                )

            # Сохраняем успешную проверку.
            # UNKNOWN -> UP -> STATUS_CHANGED.
            checked_at = BASE + timedelta(seconds=1)

            with write_transaction(engine) as conn:
                change = apply_check_result(
                    conn,
                    result=CheckResultIn(
                        resource_id="n26-service",
                        config_version_id=version,
                        checked_at=checked_at,
                        success=True,
                        http_status_code=200,
                        response_time_ms=12.5,
                    ),
                )

            assert change["status_changed"] is True
            assert change["current_status"] == "UP"
            assert change["event_id"] is not None

            # Проверяем HEARTBEAT.
            worker = HeartbeatWorker(
                engine,
                poll_seconds=0.1,
            )

            due_at = BASE + timedelta(seconds=61)

            first = asyncio.run(
                worker.run_once(now=due_at)
            )

            # Повторный опрос в то же время.
            repeat = asyncio.run(
                worker.run_once(now=due_at)
            )

            # До следующего интервала событие не создаётся.
            early = asyncio.run(
                worker.run_once(
                    now=BASE + timedelta(seconds=120)
                )
            )

            assert first.scanned == 1

            assert (
                first.queued,
                repeat.queued,
                early.queued,
            ) == (1, 0, 0)

            # Читаем события из Outbox.
            with read_transaction(engine) as conn:
                events = conn.execute(
                    text("""
                        SELECT
                            event_id,
                            event_type,
                            payload_json,
                            checked_at
                        FROM outbox_events
                        ORDER BY created_at, event_id
                    """)
                ).mappings().all()

            assert len(events) == 2

            assert [
                row["event_type"]
                for row in events
            ] == [
                "STATUS_CHANGED",
                "HEARTBEAT",
            ]

            status_event, heartbeat_event = events

            assert (
                status_event["event_id"]
                != heartbeat_event["event_id"]
            )

            assert (
                status_event["event_id"]
                == change["event_id"]
            )

            # Проверяем контракт ОКО для каждого события.
            for event in events:
                data = json.loads(
                    validate_oko_payload(
                        event["payload_json"]
                    )
                )

                assert (
                    data["event_id"]
                    == event["event_id"]
                )

                # checked_at — время реальной проверки.
                assert (
                    data["checked_at"]
                    == to_utc_iso(checked_at)
                )

                assert (
                    event["checked_at"]
                    == to_utc_iso(checked_at)
                )

            # Тестовые настройки отправителя.
            settings = AppSettings(
                _env_file=None,
                event_post_url=POST_URL,
                event_token=TOKEN,
            )

            headers = {
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/json",
            }

            requests: list[tuple[str, bytes]] = []

            fail_heartbeat_once = True

            # Создаём один Mock API на весь сценарий.
            # Он запоминает принятые event_id.
            with TestClient(
                create_mock_app(settings)
            ) as mock_receiver:

                def receive(
                    request: httpx.Request,
                ) -> httpx.Response:

                    nonlocal fail_heartbeat_once

                    assert request.url.path == POST_PATH

                    assert (
                        request.headers["Authorization"]
                        == f"Bearer {TOKEN}"
                    )

                    raw = validate_oko_payload(
                        request.content
                    )

                    kind = json.loads(raw)["event_type"]

                    requests.append((kind, raw))

                    # Имитируем HTTP 503 при первой
                    # отправке HEARTBEAT.
                    if (
                        kind == "HEARTBEAT"
                        and fail_heartbeat_once
                    ):
                        fail_heartbeat_once = False

                        return httpx.Response(503)

                    # Передаём запрос в реальный
                    # обработчик тестового FastAPI.
                    accepted = mock_receiver.post(
                        POST_PATH,
                        content=raw,
                        headers=headers,
                    )

                    return httpx.Response(
                        accepted.status_code,
                        content=accepted.content,
                        headers={
                            "Content-Type": "application/json"
                        },
                    )

                transport = httpx.MockTransport(receive)

                with httpx.Client(
                    transport=transport
                ) as client:

                    def deliver(
                        seconds: int,
                    ) -> str | None:

                        return send_one_event(
                            engine,
                            settings,
                            worker_id="n26-sender",
                            client=client,
                            now=BASE + timedelta(
                                seconds=seconds
                            ),
                        )

                    # STATUS_CHANGED успешно доставлен.
                    assert deliver(65) == "SENT"

                    # HEARTBEAT получил HTTP 503.
                    assert deliver(65) == "PENDING"

                    # Рано повторять: ожидаем backoff.
                    assert deliver(66) is None

                    # Повторная отправка HEARTBEAT.
                    assert deliver(67) == "SENT"

                    # Больше отправлять нечего.
                    assert deliver(67) is None

                # Проверяем порядок HTTP-запросов.
                assert [
                    kind for kind, _ in requests
                ] == [
                    "STATUS_CHANGED",
                    "HEARTBEAT",
                    "HEARTBEAT",
                ]

                # При повторной доставке JSON не меняется.
                assert requests[1][1] == requests[2][1]

                heartbeat_bytes = (
                    heartbeat_event["payload_json"]
                    .encode("utf-8")
                )

                assert requests[2][1] == heartbeat_bytes

                # Проверяем идемпотентность:
                # отправляем тот же JSON и те же event_id.
                duplicate = mock_receiver.post(
                    POST_PATH,
                    content=heartbeat_bytes,
                    headers=headers,
                )

                assert_http(
                    duplicate,
                    200,
                    "Повторное событие",
                )

                # Тот же event_id, но изменённые данные.
                # Получатель должен отклонить запрос.
                modified = json.loads(
                    heartbeat_bytes
                )

                modified["response_time_ms"] = 99.0

                conflict = mock_receiver.post(
                    POST_PATH,
                    json=modified,
                    headers={
                        "Authorization": f"Bearer {TOKEN}"
                    },
                )

                assert_http(
                    conflict,
                    409,
                    "Конфликт event_id",
                )

            # Проверяем итоговые данные SQLite.
            with read_transaction(engine) as conn:
                delivered = conn.execute(
                    text("""
                        SELECT
                            event_id,
                            delivery_status,
                            attempts
                        FROM outbox_events
                        ORDER BY created_at, event_id
                    """)
                ).mappings().all()

                current_status = conn.execute(
                    text("""
                        SELECT current_status
                        FROM resource_states
                        WHERE resource_id = 'n26-service'
                    """)
                ).scalar_one()

                bad_foreign_keys = conn.exec_driver_sql(
                    "PRAGMA foreign_key_check"
                ).fetchall()

            assert [
                (
                    row["delivery_status"],
                    row["attempts"],
                )
                for row in delivered
            ] == [
                ("SENT", 1),
                ("SENT", 2),
            ]

            # HEARTBEAT не меняет статус ресурса.
            assert current_status == "UP"

            assert not bad_foreign_keys

            return SmokeResult(
                status_event_id=(
                    status_event["event_id"]
                ),
                heartbeat_event_id=(
                    heartbeat_event["event_id"]
                ),
                http_attempts=len(requests),
                heartbeat_attempts=(
                    delivered[1]["attempts"]
                ),
                duplicate_http_status=(
                    duplicate.status_code
                ),
                conflicting_http_status=(
                    conflict.status_code
                ),
            )

        finally:
            engine.dispose()


def main() -> int:
    result = run_smoke()

    print(
        "PASS: STATUS_CHANGED -> HEARTBEAT "
        "-> Outbox -> Mock API"
    )

    print(
        "PASS: HTTP 503 -> повторная доставка "
        "без изменения JSON и event_id"
    )

    print(
        "PASS: повтор HTTP 200, "
        "конфликт event_id HTTP 409"
    )

    print(
        f"PASS: HTTP-попыток={result.http_attempts}, "
        f"HEARTBEAT попыток={result.heartbeat_attempts}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
