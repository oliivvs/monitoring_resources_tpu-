
"""N16: полный цикл JSON/YAML -> SQLite -> scheduler -> HTTP/TCP -> события."""

import asyncio
import json

import pytest
import yaml
from sqlalchemy import text

from checks.scheduler import ResourceScheduler
from checks.security import NetworkPolicy
from db.session import initialize, make_engine, read_transaction
from services.config_import import import_resources_file
from shared.schemas import ResourceConfig


@pytest.fixture
def engine(tmp_path):
    # Отдельная SQLite для каждого теста. Рабочую базу не затрагиваем.
    db = make_engine(tmp_path / "n16.sqlite")
    initialize(db)
    yield db
    db.dispose()


def write_config(tmp_path, *resources):
    """Сохраняем реальные Pydantic-конфигурации в YAML."""
    path = tmp_path / "n16_resources.yaml"
    path.write_text(
        yaml.safe_dump(
            {"resources": [resource.model_dump(mode="json") for resource in resources]},
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def read_statuses(engine):
    """Получаем статусы из SQLite для ожидания результатов планировщика."""
    with read_transaction(engine) as conn:
        rows = conn.execute(text(
            "SELECT resource_id, current_status FROM resource_states"
        )).all()
    return dict(rows)


async def wait_for_statuses(engine, expected, *, timeout=12.0):
    """Ждём состояния, а не спим фиксированное число секунд."""
    async def poll():
        while True:
            statuses = await asyncio.to_thread(read_statuses, engine)
            if all(statuses.get(rid) == status for rid, status in expected.items()):
                return
            await asyncio.sleep(0.05)

    await asyncio.wait_for(poll(), timeout=timeout)


def history(engine, resource_id):
    """Проверяем историю результатов и соответствующие события."""
    with read_transaction(engine) as conn:
        checks = conn.execute(text("""
            SELECT success, http_status_code, resulting_status
            FROM check_results
            WHERE resource_id = :rid
            ORDER BY checked_at, id
        """), {"rid": resource_id}).mappings().all()
        transitions = conn.execute(text("""
            SELECT previous_status, new_status
            FROM status_transitions
            WHERE resource_id = :rid
            ORDER BY changed_at, id
        """), {"rid": resource_id}).mappings().all()
        events = conn.execute(text("""
            SELECT event_type, status, delivery_status
            FROM outbox_events
            WHERE resource_id = :rid
            ORDER BY checked_at, event_id
        """), {"rid": resource_id}).mappings().all()
    return [dict(x) for x in checks], [dict(x) for x in transitions], [dict(x) for x in events]


async def serve_tcp(reader, writer):
    """Локальная TCP-служба: принимает подключение и закрывает его."""
    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_yaml_scheduler_http_tcp_and_outbox(engine, tmp_path):
    """HTTP и TCP достигают UP; отказ HTTP даёт DOWN, TCP остаётся UP."""
    response = {"code": 200}

    async def serve_http(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            status = response["code"]
            reason = "OK" if status == 200 else "Service Unavailable"
            writer.write(
                f"HTTP/1.1 {status} {reason}\r\n"
                "Content-Length: 0\r\nConnection: close\r\n\r\n".encode("ascii")
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    http_server = await asyncio.start_server(serve_http, "127.0.0.1", 0)
    tcp_server = await asyncio.start_server(serve_tcp, "127.0.0.1", 0)
    http_port = http_server.sockets[0].getsockname()[1]
    tcp_port = tcp_server.sockets[0].getsockname()[1]

    try:
        path = write_config(
            tmp_path,
            ResourceConfig(
                id="local-http", name="Локальный HTTP",
                target=f"http://127.0.0.1:{http_port}/health",
                check_type="HTTP", interval_seconds=1,
                timeout_seconds=1.0, up_after=2, down_after=2,
            ),
            ResourceConfig(
                id="local-tcp", name="Локальный TCP",
                target="127.0.0.1", check_type="TCP", port=tcp_port,
                expected_http_codes=[], interval_seconds=1,
                timeout_seconds=1.0, up_after=2, down_after=2,
            ),
            ResourceConfig(
                id="inactive", name="Отключённая проверка",
                target="127.0.0.1", check_type="TCP", port=tcp_port,
                expected_http_codes=[], active=False, interval_seconds=1,
            ),
        )
        report = import_resources_file(engine, path)
        assert report.count("CREATED") == 3
        assert import_resources_file(engine, path).count("UNCHANGED") == 3

        # Только два наших локальных порта разрешены сетевой политикой.
        policy = NetworkPolicy(
            allowed_hosts=frozenset({"127.0.0.1"}),
            allowed_ports=frozenset({http_port, tcp_port}),
            permit_loopback_for_tests=True,
        )
        scheduler = ResourceScheduler(
            engine, policy, refresh_seconds=0.05,
            max_concurrent_checks=2,
        )
        task = asyncio.create_task(scheduler.run())
        try:
            await wait_for_statuses(engine, {
                "local-http": "UP", "local-tcp": "UP", "inactive": "UNKNOWN",
            })
            response["code"] = 503
            await wait_for_statuses(engine, {"local-http": "DOWN", "local-tcp": "UP"})
        finally:
            scheduler.stop()
            await asyncio.wait_for(task, 4)

        http_checks, http_changes, http_events = history(engine, "local-http")
        tcp_checks, tcp_changes, tcp_events = history(engine, "local-tcp")
        inactive_checks, inactive_changes, inactive_events = history(engine, "inactive")

        assert len(http_checks) >= 4
        assert sum(bool(c["success"]) for c in http_checks) >= 2
        assert sum(not bool(c["success"]) for c in http_checks) >= 2
        assert any(c["http_status_code"] == 503 for c in http_checks)
        assert http_changes == [
            {"previous_status": "UNKNOWN", "new_status": "UP"},
            {"previous_status": "UP", "new_status": "DOWN"},
        ]
        assert [(e["event_type"], e["status"], e["delivery_status"]) for e in http_events] == [
            ("STATUS_CHANGED", "UP", "PENDING"),
            ("STATUS_CHANGED", "DOWN", "PENDING"),
        ]

        assert len(tcp_checks) >= 2
        assert all(bool(c["success"]) and c["http_status_code"] is None for c in tcp_checks)
        assert tcp_changes == [{"previous_status": "UNKNOWN", "new_status": "UP"}]
        assert len(tcp_events) == 1 and tcp_events[0]["status"] == "UP"
        assert (inactive_checks, inactive_changes, inactive_events) == ([], [], [])
    finally:
        http_server.close()
        tcp_server.close()
        await http_server.wait_closed()
        await tcp_server.wait_closed()


@pytest.mark.asyncio
async def test_json_config_http_down_then_recovers(engine, tmp_path):
    """Ошибки 503 -> DOWN, затем ответы 200 -> UP, с сохранением истории."""
    response = {"code": 503}

    async def serve_http(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            status = response["code"]
            reason = "OK" if status == 200 else "Service Unavailable"
            writer.write(
                f"HTTP/1.1 {status} {reason}\r\n"
                "Content-Length: 0\r\nConnection: close\r\n\r\n".encode("ascii")
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve_http, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        resource = ResourceConfig(
            id="recovery", name="Локальный сервис",
            target=f"http://127.0.0.1:{port}/health",
            check_type="HTTP", interval_seconds=1,
            timeout_seconds=1.0, up_after=2, down_after=2,
        )
        path = tmp_path / "n16_resources.json"
        path.write_text(
            json.dumps({"resources": [resource.model_dump(mode="json")]}, ensure_ascii=False),
            encoding="utf-8",
        )
        assert import_resources_file(engine, path).count("CREATED") == 1

        policy = NetworkPolicy(
            allowed_hosts=frozenset({"127.0.0.1"}),
            allowed_ports=frozenset({port}),
            permit_loopback_for_tests=True,
        )
        scheduler = ResourceScheduler(engine, policy, refresh_seconds=0.05)
        task = asyncio.create_task(scheduler.run())
        try:
            await wait_for_statuses(engine, {"recovery": "DOWN"})
            response["code"] = 200
            await wait_for_statuses(engine, {"recovery": "UP"})
        finally:
            scheduler.stop()
            await asyncio.wait_for(task, 4)

        checks, changes, events = history(engine, "recovery")
        assert len(checks) >= 4
        assert checks[0]["success"] == 0
        assert checks[-1]["success"] == 1
        assert changes == [
            {"previous_status": "UNKNOWN", "new_status": "DOWN"},
            {"previous_status": "DOWN", "new_status": "UP"},
        ]
        assert [e["status"] for e in events] == ["DOWN", "UP"]
    finally:
        server.close()
        await server.wait_closed()
