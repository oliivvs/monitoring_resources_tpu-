
"""Тесты TpuEntityClient без обращения к реальному ТПУ."""

import httpx
import pytest

from pydantic import SecretStr

from integrations.tpu.entity_client import (
    EntityApiHttpError,
    EntityApiSchemaError,
    EntityApiTransportError,
    EntityRoutes,
    TpuEntityClient,
)

from mock_entity_api.main import app


def build_client(
    transport: httpx.AsyncBaseTransport,
    *,
    token: str | None = None,
):
    http_client = httpx.AsyncClient(
        transport=transport
    )

    entity = TpuEntityClient(
        base_url="http://127.0.0.1:18082",
        routes=EntityRoutes.local_mock(),
        token=SecretStr(token) if token else None,
        http_client=http_client,
    )

    return http_client, entity


@pytest.mark.asyncio
async def test_all_five_methods():
    http_client, entity = build_client(
        httpx.ASGITransport(app=app)
    )

    async with http_client:
        entities = await entity.get_entities()

        structure = await entity.get_entity_structure(
            "servers"
        )

        parameters = await entity.get_entity_parameters(
            "servers"
        )

        filters = await entity.get_entity_filters(
            "servers"
        )

        records = await entity.get_entity_records(
            "servers"
        )

    assert entities.items[0].entity_id == "servers"
    assert len(structure.fields) == 5
    assert parameters.parameters[0].name == "limit"
    assert filters.filters[0].name == "active"
    assert len(records.items) == 3


@pytest.mark.asyncio
async def test_pagination_and_filter():
    http_client, entity = build_client(
        httpx.ASGITransport(app=app)
    )

    async with http_client:
        first = await entity.get_entity_records(
            "servers",
            limit=2,
        )

        second = await entity.get_entity_records(
            "servers",
            cursor=first.next_cursor,
            limit=2,
        )

        active = await entity.get_entity_records(
            "servers",
            filters={"active": True},
        )

    assert len(first.items) == 2
    assert first.next_cursor == "2"

    assert len(second.items) == 1
    assert second.next_cursor is None

    assert len(active.items) == 2
    assert all(
        record["active"] is True
        for record in active.items
    )


@pytest.mark.asyncio
async def test_unknown_entity_and_http_errors():
    http_client, entity = build_client(
        httpx.ASGITransport(app=app)
    )

    async with http_client:
        with pytest.raises(EntityApiHttpError) as exc:
            await entity.get_entity_structure("unknown")

    assert exc.value.status_code == 404
    assert exc.value.retryable is False


@pytest.mark.asyncio
async def test_bad_inputs_are_rejected_before_network():
    http_client, entity = build_client(
        httpx.ASGITransport(app=app)
    )

    async with http_client:
        with pytest.raises(ValueError, match="entity_id"):
            await entity.get_entity_structure("../admin")

        with pytest.raises(ValueError, match="limit"):
            await entity.get_entity_records(
                "servers",
                limit=101,
            )

        with pytest.raises(ValueError, match="фильтра"):
            await entity.get_entity_records(
                "servers",
                filters={"cursor": "bad"},
            )


@pytest.mark.parametrize(
    "bad_url",
    [
        "http://example.org",
        "https://user:pass@example.org",
        "file:///tmp/x",
    ],
)
def test_unsafe_base_urls(bad_url):
    with pytest.raises(ValueError):
        TpuEntityClient(
            base_url=bad_url,
            routes=EntityRoutes.local_mock(),
        )


@pytest.mark.parametrize(
    "status",
    [302, 401, 403, 429, 500],
)
@pytest.mark.asyncio
async def test_remote_http_errors_do_not_leak_token(status):
    seen = []

    def handler(request):
        seen.append(
            request.headers.get("Authorization")
        )

        return httpx.Response(
            status,
            json={"detail": "secret internal traceback"},
        )

    http_client, entity = build_client(
        httpx.MockTransport(handler),
        token="top-secret",
    )

    async with http_client:
        with pytest.raises(EntityApiHttpError) as exc:
            await entity.get_entities()

    assert exc.value.status_code == status
    assert exc.value.retryable is (status in {429, 500})
    assert seen == ["Bearer top-secret"]

    assert "top-secret" not in str(exc.value)
    assert "traceback" not in str(exc.value)


@pytest.mark.parametrize(
    "content",
    [
        b"broken",
        b"[]",
        b'{"items":"wrong"}',
    ],
)
@pytest.mark.asyncio
async def test_invalid_json_or_schema(content):
    http_client, entity = build_client(
        httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                content=content,
            )
        )
    )

    async with http_client:
        with pytest.raises(EntityApiSchemaError):
            await entity.get_entities()


@pytest.mark.asyncio
async def test_rejects_oversized_response():
    http_client, entity = build_client(
        httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                content=b"a" * (512 * 1024 + 1),
            )
        )
    )

    async with http_client:
        with pytest.raises(
            EntityApiSchemaError,
            match="слишком большой",
        ):
            await entity.get_entities()


@pytest.mark.asyncio
async def test_timeout_is_controlled():
    def handler(request):
        raise httpx.ReadTimeout("private auth=bad")

    http_client, entity = build_client(
        httpx.MockTransport(handler)
    )

    async with http_client:
        with pytest.raises(EntityApiTransportError) as exc:
            await entity.get_entities()

    assert "auth=" not in str(exc.value)
