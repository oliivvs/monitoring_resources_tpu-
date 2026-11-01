"""Самодостаточная проверка на 127.0.0.1 без доступа к ТПУ и внешней сети.

Запуск из корня репозитория: python -m scripts.local_smoke
"""
import asyncio
import tempfile
from pathlib import Path

from checks.runner import check_and_store
from checks.security import NetworkPolicy
from db.session import make_engine, write_transaction
from db.session import initialize
from services.resources import upsert_configuration
from shared.schemas import ResourceConfig


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def main() -> None:
    server = await asyncio.start_server(_handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    with tempfile.TemporaryDirectory() as tmp:
        engine = make_engine(Path(tmp) / "demo.sqlite")
        try:
            initialize(engine)  # только временная БД демонстрации!
            with write_transaction(engine) as conn:
                upsert_configuration(conn, ResourceConfig(
                    id="local-http", name="Локальный тестовый сервер",
                    target=f"http://127.0.0.1:{port}/health", check_type="HTTP"
                ))
            policy = NetworkPolicy(
                allowed_hosts=frozenset({"127.0.0.1"}),
                allowed_ports=frozenset({port}),
                permit_loopback_for_tests=True,
            )
            outcomes = [await check_and_store(engine, "local-http", policy)
                        for _ in range(2)]
            assert [o.status for o in outcomes] == ["UNKNOWN", "UP"]
            assert outcomes[-1].event_id is not None
            print("OK: UNKNOWN -> UP, STATUS_CHANGED сохранён в SQLite")
        finally:
            engine.dispose()
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
