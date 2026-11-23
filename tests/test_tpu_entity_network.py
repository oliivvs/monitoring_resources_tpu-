"""A24.2: безопасность сетевого клиента Entity API."""

import ssl

import httpx
import pytest

from integrations.tpu.entity_client import (
    EntityRoutes,
    TpuEntityClient,
)
from scripts.run_inbox_worker import (
    OracleWorkerSettings,
    build_snapshot_client,
)


@pytest.mark.asyncio
async def test_entity_client_ignores_proxy_environment(
    monkeypatch,
):
    """Прокси из окружения не влияет на Entity API."""

    monkeypatch.setenv(
        "HTTPS_PROXY",
        "http://127.0.0.1:9999",
    )
    monkeypatch.setenv(
        "ALL_PROXY",
        "http://127.0.0.1:9998",
    )

    actual_client = httpx.AsyncClient
    captured = {}

    def recording_client(*args, **kwargs):
        captured.update(kwargs)

        transport = httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "items": [],
                    "next_cursor": None,
                },
            )
        )

        return actual_client(
            *args,
            transport=transport,
            **kwargs,
        )

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        recording_client,
    )

    client = TpuEntityClient(
        base_url="https://entity.example.org",
        routes=EntityRoutes.local_mock(),
    )

    response = await client.get_entity_records(
        "servers",
        limit=2,
    )

    assert len(response.items) == 0

    assert captured["trust_env"] is False
    assert captured["follow_redirects"] is False
    assert isinstance(
        captured["verify"],
        ssl.SSLContext,
    )

    assert (
        captured["verify"].verify_mode
        == ssl.CERT_REQUIRED
    )


def test_snapshot_allows_explicit_docker_lab_host():
    """Разрешённый Docker-host не блокируется."""

    settings = OracleWorkerSettings(
        _env_file=None,
        enabled=True,
        snapshot_enabled=True,
        snapshot_base_url="https://oracle-snapshot",
        snapshot_allowed_hosts="oracle-snapshot",
        snapshot_token="test-only-token",
    )

    client = build_snapshot_client(settings)

    assert isinstance(client, TpuEntityClient)
    assert client._base_url == "https://oracle-snapshot"


from integrations.tpu.snapshot_transport import (
    PinnedSnapshotTransport,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "addresses,cidrs,accepted",
    [
        (["1.1.1.1"], (), True),
        (["172.22.0.3"], (), False),
        (["172.22.0.3"], ("172.16.0.0/12",), True),
        (["127.0.0.1"], ("127.0.0.0/8",), False),
        (["169.254.169.254"], ("169.254.0.0/16",), False),
        (["1.1.1.1", "127.0.0.1"], (), False),
        (["::ffff:127.0.0.1"], (), False),
        (["2001:4860:4860::8888"], (), True),
    ],
)
async def test_snapshot_dns_pinning(
    monkeypatch,
    addresses,
    cidrs,
    accepted,
):
    received = []

    async def mock_handler(request):
        received.append(request)

        return httpx.Response(
            200,
            json={
                "items": [],
                "next_cursor": None,
            },
        )

    transport = PinnedSnapshotTransport(
        expected_host="oracle-snapshot",
        allowed_cidrs=cidrs,
        inner=httpx.MockTransport(mock_handler),
    )

    async def fake_resolve(host, port):
        assert host == "oracle-snapshot"
        assert port == 443
        return addresses

    monkeypatch.setattr(
        transport,
        "_resolve",
        fake_resolve,
    )

    async with httpx.AsyncClient(
        transport=transport,
        trust_env=False,
    ) as client:
        if accepted:
            response = await client.get(
                "https://oracle-snapshot/health"
            )

            assert response.status_code == 200

        else:
            with pytest.raises(httpx.ConnectError):
                await client.get(
                    "https://oracle-snapshot/health"
                )

    if accepted:
        assert len(received) == 1

        request = received[0]

        assert request.url.host == addresses[0]
        assert request.headers["host"] == "oracle-snapshot"
        assert (
            request.extensions["sni_hostname"]
            == "oracle-snapshot"
        )

    else:
        assert received == []


def test_snapshot_config_passes_allowed_cidrs():
    settings = OracleWorkerSettings(
        _env_file=None,
        snapshot_enabled=True,
        snapshot_base_url="https://oracle-snapshot",
        snapshot_allowed_hosts="oracle-snapshot",
        snapshot_allowed_cidrs="172.16.0.0/12",
        snapshot_token="test-only-token",
    )

    client = build_snapshot_client(settings)

    assert client._pinned_allowed_cidrs == (
        "172.16.0.0/12",
    )


def test_snapshot_rejects_invalid_cidr():
    settings = OracleWorkerSettings(
        _env_file=None,
        snapshot_enabled=True,
        snapshot_base_url="https://oracle-snapshot",
        snapshot_allowed_hosts="oracle-snapshot",
        snapshot_allowed_cidrs="not-a-network",
        snapshot_token="test-only-token",
    )

    with pytest.raises(ValueError):
        build_snapshot_client(settings)
