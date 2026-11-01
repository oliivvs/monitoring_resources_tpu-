"""Сетевые проверки только локальных тестовых служб, без публичного сканирования."""
import asyncio
import json
import socket

import pytest
from sqlalchemy import text

from checks.config_loader import load_resources
from checks.probes import probe
from checks.runner import check_and_store
from checks.security import NetworkPolicy, TargetNotAllowed
from db.session import initialize, make_engine, read_transaction, write_transaction
from services.resources import get_active_check_snapshot, upsert_configuration
from shared.schemas import ExpectedHttpRange, ResourceConfig


@pytest.fixture
def engine(tmp_path):
    engine = make_engine(tmp_path / "checkers.sqlite")
    initialize(engine)
    yield engine
    engine.dispose()


def cfg(url, *, check_type="HTTP", port=None, expected=None, resource_id="web"):
    return ResourceConfig(id=resource_id, name="Local test",target=url,check_type=check_type,
        port=port,timeout_seconds=0.4,
        expected_http_codes=expected if expected is not None else
        ([ExpectedHttpRange(start=200,end=299)] if check_type == "HTTP" else []))


def create(engine, config):
    with write_transaction(engine) as con:
        return upsert_configuration(con,config)


def snapshot(engine, name="web"):
    with read_transaction(engine) as con:
        return get_active_check_snapshot(con,resource_id=name)


def policy(port, *, allow_loopback=True):
    return NetworkPolicy(allowed_hosts=frozenset({"127.0.0.1"}),
                         allowed_ports=frozenset({port}),permit_loopback_for_tests=allow_loopback)


async def start_server(handler):
    server=await asyncio.start_server(handler,"127.0.0.1",0)
    return server, server.sockets[0].getsockname()[1]


def count(engine, table):
    with read_transaction(engine) as con:
        return con.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def http_response(status, *, redirect=False):
    reason={200:"OK",302:"Found",503:"Service Unavailable"}[status]
    extra="Location: http://127.0.0.1:9/\r\n" if redirect else ""
    return f"HTTP/1.1 {status} {reason}\r\n{extra}Content-Length: 0\r\nConnection: close\r\n\r\n".encode()


@pytest.mark.asyncio
@pytest.mark.parametrize("status, expected",[(200,True),(503,False),(302,False)])
async def test_http_status_codes_and_no_redirect(engine,status,expected):
    requests=[]
    async def serve(reader,writer):
        data=await reader.readuntil(b"\r\n\r\n")
        requests.append(data)
        writer.write(http_response(status,redirect=status==302))
        await writer.drain()
        writer.close(); await writer.wait_closed()
    server,port=await start_server(serve)
    try:
        create(engine,cfg(f"http://127.0.0.1:{port}/health"))
        result=await probe(snapshot(engine),policy(port))
        assert result.success is expected
        assert result.http_status_code==status
        assert result.config_version_id>0
        assert len(requests)==1 and b"GET /health HTTP/1.1" in requests[0]
        assert result.response_time_ms>=0
    finally:
        server.close();await server.wait_closed()


@pytest.mark.asyncio
async def test_http_timeout_does_not_block_next_checks(engine):
    async def serve(reader,writer):
        try:
            await asyncio.sleep(1.5)
            writer.close(); await writer.wait_closed()
        except (OSError,ConnectionError):
            pass
    server,port=await start_server(serve)
    try:
        create(engine,cfg(f"http://127.0.0.1:{port}/slow"))
        result=await probe(snapshot(engine),policy(port))
        assert result.success is False
        assert "ожидания" in result.error_message
        assert result.response_time_ms < 1300
    finally:
        server.close();await server.wait_closed()


@pytest.mark.asyncio
async def test_tcp_open_closed_and_denied(engine):
    async def serve(reader, writer):
        writer.close(); await writer.wait_closed()
    server,port=await start_server(serve)
    try:
        create(engine,cfg("127.0.0.1",check_type="TCP",port=port,resource_id="tcp"))
        result=await probe(snapshot(engine,"tcp"),policy(port))
        assert result.success and result.http_status_code is None
        with pytest.raises(TargetNotAllowed):
            await probe(snapshot(engine,"tcp"),NetworkPolicy())
    finally:
        server.close();await server.wait_closed()
    result=await probe(snapshot(engine,"tcp"),policy(port))
    # Windows may report a closed TCP port as a connect timeout rather than
    # an immediate connection refusal; both are correct failed probes.
    assert result.success is False
    assert result.http_status_code is None
    assert result.error_message is not None
    assert (
        result.error_message.startswith("Ошибка сети:")
        or result.error_message == "Превышено время ожидания"
    )


@pytest.mark.asyncio
async def test_tcp_refused_connection_is_network_failure(engine, monkeypatch):
    """Deterministically exercise the connection-refused branch on all OSes."""
    async def refused_connection(*args, **kwargs):
        raise ConnectionRefusedError("connection refused")

    monkeypatch.setattr(asyncio, "open_connection", refused_connection)
    port = 18723
    create(engine, cfg("127.0.0.1", check_type="TCP", port=port, resource_id="refused"))
    result = await probe(snapshot(engine, "refused"), policy(port))
    assert result.success is False
    assert result.http_status_code is None
    assert result.error_message == "Ошибка сети: ConnectionError"


@pytest.mark.asyncio
async def test_tcp_connect_timeout_is_failed_probe(engine, monkeypatch):
    """A filtered TCP port is a failed probe, never an application exception."""
    async def timed_out_connection(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncio, "open_connection", timed_out_connection)
    port = 18724
    create(engine, cfg("127.0.0.1", check_type="TCP", port=port, resource_id="timedout"))
    result = await probe(snapshot(engine, "timedout"), policy(port))
    assert result.success is False
    assert result.http_status_code is None
    assert result.error_message == "Превышено время ожидания"


@pytest.mark.asyncio
async def test_tcp_cleanup_timeout_never_reports_success_with_error(engine, monkeypatch):
    """Успешный connect не должен скрыть сбой на закрытии соединения."""
    class BrokenWriter:
        def close(self):
            pass

        async def wait_closed(self):
            raise TimeoutError("close failed")

    async def connected(*args, **kwargs):
        return object(), BrokenWriter()

    monkeypatch.setattr(asyncio, "open_connection", connected)
    port = 18725
    create(engine, cfg("127.0.0.1", check_type="TCP", port=port,
                       resource_id="tcp-cleanup"))
    result = await probe(snapshot(engine, "tcp-cleanup"), policy(port))
    assert result.success is False
    assert result.http_status_code is None
    assert result.error_message == "Превышено время ожидания"


@pytest.mark.asyncio
async def test_explicit_allowlist_and_loopback(engine):
    create(engine,cfg("http://127.0.0.1:8080/health"))
    with pytest.raises(TargetNotAllowed,match="списке разрешённых целей"):
        await probe(snapshot(engine),NetworkPolicy(allowed_hosts=frozenset({"example.org"}),allowed_ports=frozenset({8080})))
    with pytest.raises(TargetNotAllowed,match="DNS вернул запрещённый"):
        await probe(snapshot(engine),policy(8080,allow_loopback=False))
    assert count(engine,"check_results")==0


@pytest.mark.asyncio
async def test_integrated_check_creates_outbox_after_two_successes(engine):
    async def serve(reader,writer):
        await reader.readuntil(b"\r\n\r\n")
        writer.write(http_response(200));await writer.drain()
        writer.close();await writer.wait_closed()
    server,port=await start_server(serve)
    try:
        create(engine,cfg(f"http://127.0.0.1:{port}/health"))
        first=await check_and_store(engine,"web",policy(port))
        second=await check_and_store(engine,"web",policy(port))
        assert first.disposition==second.disposition=="APPLIED"
        assert first.status=="UNKNOWN" and second.status=="UP"
        assert second.event_id is not None
        assert count(engine,"check_results")==2
        assert count(engine,"status_transitions")==1
        assert count(engine,"outbox_events")==1
    finally:
        server.close();await server.wait_closed()


@pytest.mark.asyncio
async def test_network_denial_does_not_write_database(engine):
    create(engine,cfg("http://127.0.0.1:9090/secret"))
    result=await check_and_store(engine,"web",NetworkPolicy())
    assert result.disposition=="DENIED"
    assert count(engine,"check_results")==0
    assert count(engine,"outbox_events")==0


@pytest.mark.asyncio
async def test_stale_inflight_network_response_dropped(engine):
    reached=asyncio.Event()
    release=asyncio.Event()
    async def serve(reader,writer):
        await reader.readuntil(b"\r\n\r\n")
        reached.set()
        await release.wait()
        writer.write(http_response(200)); await writer.drain()
        writer.close();await writer.wait_closed()
    server,port=await start_server(serve)
    try:
        old_config=cfg(f"http://127.0.0.1:{port}/delay")
        create(engine,old_config)
        task=asyncio.create_task(check_and_store(engine,"web",policy(port)))
        await asyncio.wait_for(reached.wait(),timeout=2)
        with write_transaction(engine) as con:
            from datetime import datetime,timezone,timedelta
            upsert_configuration(con,old_config.model_copy(update={"up_after":5}),
                now=datetime.now(timezone.utc)+timedelta(seconds=1))
        release.set()
        result=await asyncio.wait_for(task,timeout=3)
        assert result.disposition=="STALE"
        assert count(engine,"check_results")==0
        assert count(engine,"outbox_events")==0
    finally:
        release.set(); server.close();await server.wait_closed()


def test_json_and_yaml_loading_validated(tmp_path):
    data={"resources":[cfg("https://example.org/health").model_dump(mode="json")]}
    p=tmp_path/"resources.json";p.write_text(json.dumps(data),encoding="utf8")
    assert load_resources(p).resources[0].target=="https://example.org/health"
    p=tmp_path/"resources.yaml";p.write_text('''resources:\n  - id: tcp\n    name: Test TCP\n    target: 127.0.0.1\n    check_type: TCP\n    port: 8080\n    expected_http_codes: []\n''')
    assert load_resources(p).resources[0].port==8080


@pytest.mark.parametrize("contents",[
    '{"resources":[],"resources":[]}',
    '{"resources":[{"id":"dup","id":"dup"}]}',
    '{"resources": [NaN]}',
])
def test_invalid_json_rejected(tmp_path,contents):
    p=tmp_path/"resources.json";p.write_text(contents)
    with pytest.raises(Exception):
        load_resources(p)


def test_reject_yaml_alias_and_duplicate_keys(tmp_path):
    p=tmp_path/"resources.yaml"
    p.write_text("a: &anchor 5\nb: *anchor\n")
    with pytest.raises(ValueError,match="aliases"):
        load_resources(p)
    p.write_text("resources: []\nresources: []")
    with pytest.raises(ValueError,match="Повтор"):
        load_resources(p)


def test_config_size_limit(tmp_path):
    p=tmp_path/"resources.json";p.write_bytes(b"a"*100)
    with pytest.raises(ValueError,match="лимит"):
        load_resources(p,max_bytes=20)


@pytest.mark.asyncio
async def test_dns_is_pinned_and_original_host_header_survives(monkeypatch, engine):
    """Подставное доменное имя разрешается только предварительно проверенным IP."""
    observed = []
    async def serve(reader, writer):
        req = await reader.readuntil(b"\r\n\r\n")
        observed.append(req)
        writer.write(http_response(200))
        await writer.drain()
        writer.close(); await writer.wait_closed()
    server, port = await start_server(serve)
    try:
        create(engine, cfg(f"http://monitoring-test.invalid:{port}/hello"))
        loop=asyncio.get_running_loop()
        original=loop.getaddrinfo
        async def fake_resolver(host, port_no, **kwargs):
            if host == "monitoring-test.invalid":
                return [(socket.AF_INET,socket.SOCK_STREAM,6,"",("127.0.0.1",port_no))]
            return await original(host,port_no,**kwargs)
        monkeypatch.setattr(loop,"getaddrinfo",fake_resolver)
        result=await probe(snapshot(engine),NetworkPolicy(
            allowed_hosts=frozenset({"monitoring-test.invalid"}),
            allowed_ports=frozenset({port}), permit_loopback_for_tests=True))
        assert result.success
        assert b"Host: monitoring-test.invalid:" in observed[0] or b"host: monitoring-test.invalid:" in observed[0].lower()
    finally:
        server.close();await server.wait_closed()


@pytest.mark.asyncio
async def test_mixed_public_private_dns_answers_are_blocked(monkeypatch, engine):
    from checks.security import authorize_destination
    create(engine,cfg("https://dns-test.invalid/"))
    loop=asyncio.get_running_loop()
    async def mixed(host,port,**kwargs):
        return [
            (socket.AF_INET,socket.SOCK_STREAM,6,"",("8.8.8.8",port)),
            (socket.AF_INET,socket.SOCK_STREAM,6,"",("10.0.0.5",port)),
        ]
    monkeypatch.setattr(loop,"getaddrinfo",mixed)
    with pytest.raises(TargetNotAllowed, match="запрещённый сетевой адрес"):
        await authorize_destination(snapshot(engine),NetworkPolicy(
            allowed_hosts=frozenset({"dns-test.invalid"}),allowed_ports=frozenset({443})))


@pytest.mark.asyncio
async def test_url_with_credentials_rejected_before_network(engine):
    create(engine,cfg("http://user:secret@127.0.0.1:8080/"))
    with pytest.raises(TargetNotAllowed, match="Учетные данные"):
        await probe(snapshot(engine),policy(8080))
    assert count(engine,"check_results")==0


@pytest.mark.asyncio
async def test_missing_resource_handled_without_database_writes(engine):
    result=await check_and_store(engine,"missing",NetworkPolicy())
    assert result.disposition=="INACTIVE"
    assert count(engine,"check_results")==0


@pytest.mark.asyncio
async def test_cancel_network_task_is_propagated(engine):
    entered=asyncio.Event()
    async def serve(reader,writer):
        await reader.readuntil(b"\r\n\r\n")
        entered.set()
        try:
            await asyncio.wait_for(reader.read(1), timeout=2)
        finally:
            writer.close();await writer.wait_closed()
    server,port=await start_server(serve)
    try:
        create(engine,cfg(f"http://127.0.0.1:{port}/slow"))
        task=asyncio.create_task(probe(snapshot(engine),policy(port)))
        await asyncio.wait_for(entered.wait(),2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        server.close();await server.wait_closed()
