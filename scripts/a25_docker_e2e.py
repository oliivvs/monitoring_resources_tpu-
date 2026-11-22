"""A25.6: POST -> Inbox -> HTTPS Snapshot -> SQLite."""

from __future__ import annotations

import os
import time

from datetime import datetime, timezone
from uuid import uuid4

import httpx
from sqlalchemy import text
from sqlalchemy.engine import make_url

from app.config import AppSettings
from db.session import make_engine, read_transaction


API_URL = os.environ["A25_E2E_API_URL"].rstrip("/")
SYNC_TOKEN = os.environ["MONITORING_TPU_SYNC_TOKEN"]

EVENT_PATH = "/api/v1/integrations/tpu/resource-events"

SOURCE_ID = "docker-lab-snapshot-001"
RESOURCE_ID = "tpu-oracle-" + SOURCE_ID


def make_event(sequence, operation, data):
    return {
        "event_id": str(uuid4()),
        "source": "tpu-oracle-sync",
        "entity": "servers",
        "record_id": SOURCE_ID,
        "operation_code": operation,
        "source_sequence": sequence,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }


def wait_api(client):
    for _ in range(60):
        try:
            response = client.get(API_URL + "/health")

            if response.status_code == 200:
                return
        except httpx.RequestError:
            pass

        time.sleep(0.25)

    raise RuntimeError("FastAPI не стал доступен")


def send_event(client, event):
    response = client.post(
        API_URL + EVENT_PATH,
        json=event,
        headers={
            "Authorization": "Bearer " + SYNC_TOKEN,
        },
    )

    if response.status_code != 202:
        raise RuntimeError(
            "POST не принят: HTTP "
            + str(response.status_code)
        )

    return event["event_id"]


def wait_processed(engine, event_id):
    deadline = time.monotonic() + 30

    while time.monotonic() < deadline:
        with read_transaction(engine) as conn:
            row = conn.execute(
                text("""
                    SELECT processing_status,
                           attempts,
                           last_error
                    FROM inbox_events
                    WHERE event_id=:event_id
                """),
                {"event_id": event_id},
            ).mappings().one_or_none()

        if row is not None:
            if row["processing_status"] == "PROCESSED":
                return row

            if row["processing_status"] == "FAILED":
                raise RuntimeError(
                    "Worker завершил событие ошибкой: "
                    + str(row["last_error"])
                )

        time.sleep(0.2)

    raise RuntimeError(
        "Таймаут ожидания PROCESSED для события"
    )


def read_resource(engine):
    with read_transaction(engine) as conn:
        row = conn.execute(
            text("""
                SELECT
                    r.name,
                    c.version_number,
                    c.interval_seconds,
                    seq.last_sequence
                FROM resources AS r
                JOIN resource_states AS s
                  ON s.resource_id = r.id
                JOIN resource_config_versions AS c
                  ON c.id = s.config_version_id
                JOIN resource_source_sequences AS seq
                  ON seq.source_record_id = :source_id
                 AND seq.source_system = 'tpu-oracle-sync'
                 AND seq.source_entity = 'servers'
                WHERE r.id = :resource_id
            """),
            {
                "source_id": SOURCE_ID,
                "resource_id": RESOURCE_ID,
            },
        ).mappings().one_or_none()

    return dict(row) if row is not None else None


def main():
    database_path = make_url(
        AppSettings().database_url
    ).database

    engine = make_engine(database_path)

    try:
        # В свежем проекте ресурс отсутствует.
        assert read_resource(engine) is None, (
            "Тестовая база не пустая"
        )

        with httpx.Client(
            timeout=5,
            trust_env=False,
        ) as client:
            wait_api(client)
            print("FastAPI: OK")

            # 1. Создаём исходный ресурс через POST.
            insert = make_event(
                6,
                1,
                {
                    "name": "Сервер до обновления",
                    "target": "https://1.1.1.1/health",
                    "check_type": "HTTP",
                    "active": True,
                    "port": None,
                    "interval_seconds": 60,
                    "timeout_seconds": 5.0,
                    "expected_http_codes": [
                        {"start": 200, "end": 299}
                    ],
                    "up_after": 2,
                    "down_after": 3,
                    "heartbeat_interval_seconds": 300,
                },
            )

            insert_id = send_event(client, insert)
            insert_status = wait_processed(engine, insert_id)

            state = read_resource(engine)

            assert state is not None
            assert state["name"] == "Сервер до обновления"
            assert state["version_number"] == 1
            assert state["interval_seconds"] == 60
            assert state["last_sequence"] == 6

            print(
                "INSERT: PROCESSED,"
                " version=1, sequence=6"
            )

            # 2. Отправляем частичный UPDATE.
            # Полный снимок должен прийти через HTTPS.
            update = make_event(
                7,
                3,
                {
                    "name": "Обновлённый сервер Docker",
                },
            )

            update_id = send_event(client, update)
            update_status = wait_processed(engine, update_id)

            state = read_resource(engine)

            assert state is not None
            assert state["name"] == "Обновлённый сервер Docker"
            assert state["version_number"] == 2
            assert state["interval_seconds"] == 90
            assert state["last_sequence"] == 7

            assert insert_status["attempts"] == 1
            assert update_status["attempts"] == 1

            print(
                "PARTIAL UPDATE: PROCESSED,"
                " version=2, sequence=7"
            )
            print("HTTPS SNAPSHOT: OK")
            print("SQLITE ATOMIC UPDATE: OK")
            print("A25.6 DOCKER E2E: PASSED")

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
