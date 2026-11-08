"""Сквозной тест Docker Compose: Scheduler -> SQLite -> Sender -> Mock API."""

import asyncio
import os
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import text

from app.database import get_engine
from checks.scheduler import ResourceScheduler
from checks.security import NetworkPolicy
from db.session import read_transaction, write_transaction
from services.resources import upsert_configuration
from shared.schemas import ResourceConfig


async def handle_http(reader, writer):
    """Простой локальный HTTP-сервер для проверки."""
    try:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Length: 0\r\n"
            b"Connection: close\r\n\r\n"
        )
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


def get_test_state(engine, resource_id):
    """Читаем состояние проверки и доставки из SQLite."""
    with read_transaction(engine) as conn:
        state = conn.execute(
            text("""
                SELECT
                    s.current_status AS status,
                    (
                        SELECT COUNT(*)
                        FROM check_results
                        WHERE resource_id = :rid
                    ) AS checks
                FROM resource_states AS s
                WHERE s.resource_id = :rid
            """),
            {"rid": resource_id},
        ).mappings().one()

        event = conn.execute(
            text("""
                SELECT event_id, delivery_status, attempts
                FROM outbox_events
                WHERE resource_id = :rid
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"rid": resource_id},
        ).mappings().first()

        http_code = None

        if event is not None and event["attempts"] > 0:
            http_code = conn.execute(
                text("""
                    SELECT http_status_code
                    FROM delivery_attempts
                    WHERE event_id = :event_id
                    ORDER BY attempt_number DESC
                    LIMIT 1
                """),
                {"event_id": event["event_id"]},
            ).scalar_one_or_none()

    return {
        "status": state["status"],
        "checks": state["checks"],
        "delivery": (
            event["delivery_status"] if event else None
        ),
        "http_code": http_code,
    }


async def main():
    # Защита от случайного запуска на локальной рабочей БД.
    if os.getenv("MONITORING_DATABASE_URL") != (
        "sqlite:////data/monitoring.db"
    ):
        raise RuntimeError(
            "Запускайте тест только через Docker Compose"
        )

    server = await asyncio.start_server(
        handle_http, "127.0.0.1", 0
    )
    port = server.sockets[0].getsockname()[1]

    engine = get_engine()

    resource_id = "a09-e2e-" + uuid4().hex[:8]

    config = ResourceConfig(
        id=resource_id,
        name="A09 Automated E2E Test",
        target=f"http://127.0.0.1:{port}/health",
        check_type="HTTP",
        interval_seconds=1,
        timeout_seconds=2,
    )

    created = False
    scheduler = None
    scheduler_task = None

    try:
        # Регистрируем временный ресурс в общей SQLite.
        with write_transaction(engine) as conn:
            upsert_configuration(conn, config)

        created = True

        print("Тестовый ресурс:", resource_id, flush=True)

        # Разрешаем только локальный адрес тестового сервера.
        policy = NetworkPolicy(
            allowed_hosts=frozenset({"127.0.0.1"}),
            allowed_ports=frozenset({port}),
            permit_loopback_for_tests=True,
        )

        scheduler = ResourceScheduler(
            engine,
            policy,
            refresh_seconds=0.1,
            max_concurrent_checks=1,
        )

        scheduler_task = asyncio.create_task(
            scheduler.run()
        )

        deadline = asyncio.get_running_loop().time() + 30

        while asyncio.get_running_loop().time() < deadline:
            if scheduler_task.done():
                await scheduler_task
                raise RuntimeError(
                    "Scheduler неожиданно остановился"
                )

            state = get_test_state(engine, resource_id)

            if (
                state["checks"] >= 2
                and state["status"] == "UP"
                and state["delivery"] == "SENT"
                and state["http_code"] == 202
            ):
                print(
                    "E2E OK:",
                    f"checks={state['checks']}",
                    f"status={state['status']}",
                    f"delivery={state['delivery']}",
                    f"mock_http={state['http_code']}",
                    flush=True,
                )
                return

            await asyncio.sleep(0.3)

        raise RuntimeError(
            "E2E FAILED: событие не доставлено за 30 секунд"
        )

    finally:
        try:
            # Останавливаем тестовый планировщик.
            if scheduler is not None:
                scheduler.stop()

            if scheduler_task is not None:
                await scheduler_task
        finally:
            try:
                # Отключаем тестовый ресурс даже при ошибке.
                if created:
                    with write_transaction(engine) as conn:
                        upsert_configuration(
                            conn,
                            config.model_copy(
                                update={"active": False}
                            ),
                            now=datetime.now(timezone.utc),
                        )
                    print("Тестовый ресурс отключён", flush=True)
            finally:
                server.close()
                await server.wait_closed()
                engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
