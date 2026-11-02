
"""Демонстрация N08 на локальном HTTP-сервере."""
from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path

from sqlalchemy import text

from checks.scheduler import ResourceScheduler
from checks.security import NetworkPolicy
from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)
from services.resources import upsert_configuration
from shared.schemas import ResourceConfig


async def serve_http(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
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


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )

    # httpx может логировать URL с query-параметрами.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    # Порт 0 означает: система сама выбирает свободный порт.
    server = await asyncio.start_server(
        serve_http, "127.0.0.1", 0
    )
    port = server.sockets[0].getsockname()[1]

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = make_engine(
                Path(temp_dir) / "scheduler-demo.sqlite"
            )

            try:
                initialize(engine)

                with write_transaction(engine) as conn:
                    upsert_configuration(
                        conn,
                        ResourceConfig(
                            id="local-demo",
                            name="Локальный тестовый веб-сервис",
                            target=f"http://127.0.0.1:{port}/health",
                            check_type="HTTP",
                            interval_seconds=1,
                            timeout_seconds=1,
                        ),
                    )

                # Разрешаем исключительно наш локальный сервер.
                policy = NetworkPolicy(
                    allowed_hosts=frozenset({"127.0.0.1"}),
                    allowed_ports=frozenset({port}),
                    permit_loopback_for_tests=True,
                )

                scheduler = ResourceScheduler(
                    engine,
                    policy,
                    refresh_seconds=0.1,
                )

                task = asyncio.create_task(scheduler.run())

                try:
                    # Даём планировщику выполнить несколько проверок.
                    await asyncio.sleep(2.5)
                finally:
                    scheduler.stop()
                    await task

                # Смотрим, какие данные записались в SQLite.
                with read_transaction(engine) as conn:
                    checks = conn.execute(text(
                        "SELECT count(*) FROM check_results "
                        "WHERE resource_id='local-demo'"
                    )).scalar_one()

                    status = conn.execute(text(
                        "SELECT current_status FROM resource_states "
                        "WHERE resource_id='local-demo'"
                    )).scalar_one()

                    events = conn.execute(text(
                        "SELECT count(*) FROM outbox_events "
                        "WHERE resource_id='local-demo'"
                    )).scalar_one()

                print(
                    f"Проверок: {checks}; "
                    f"статус: {status}; "
                    f"событий: {events}"
                )

                assert checks >= 2
                assert status == "UP"
                assert events == 1

            finally:
                engine.dispose()

    finally:
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
