
"""Интеграционный тест HTTP sender и Mock API."""

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.config import AppSettings
from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)
from mock_api.main import create_mock_app
from services.resources import upsert_configuration
from services.sender import send_one_event
from services.status import apply_check_result
from shared.schemas import CheckResultIn, ResourceConfig


BASE = datetime(2026, 11, 2, 8, 0, tzinfo=timezone.utc)


@pytest.fixture
def engine(tmp_path):
    engine = make_engine(tmp_path / "integration.sqlite")
    initialize(engine)

    yield engine

    engine.dispose()


def create_event(engine):
    # Регистрируем ресурс
    with write_transaction(engine) as conn:
        version = upsert_configuration(
            conn,
            ResourceConfig(
                id="server-001",
                name="Test Server",
                target="https://example.org/health",
                check_type="HTTP",
            ),
            now=BASE,
        )

    # Две успешные проверки создают STATUS_CHANGED
    for seconds in (1, 2):
        with write_transaction(engine) as conn:
            apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id="server-001",
                    config_version_id=version,
                    checked_at=BASE + timedelta(seconds=seconds),
                    success=True,
                    response_time_ms=12.5,
                    http_status_code=200,
                ),
            )


def test_sender_delivers_event_to_mock_api(engine):
    create_event(engine)

    mock_settings = AppSettings(
        _env_file=None,
        event_token="integration-test-token",
    )

    sender_settings = AppSettings(
        _env_file=None,
        event_post_url="https://mock.example/api/v1/status/events",
        event_token="integration-test-token",
    )

    requests = []
    responses = []

    # Направляем HTTP-запрос отправителя в FastAPI.
    # Реальное сетевое соединение не требуется.
    with TestClient(create_mock_app(mock_settings)) as receiver:

        def forward_request(request):
            requests.append(request)

            reply = receiver.request(
                request.method,
                request.url.path,
                content=request.content,
                headers=dict(request.headers),
            )

            responses.append(reply.status_code)

            return httpx.Response(
                status_code=reply.status_code,
                content=reply.content,
                headers=dict(reply.headers),
            )

        transport = httpx.MockTransport(forward_request)

        with httpx.Client(transport=transport) as client:
            result = send_one_event(
                engine,
                sender_settings,
                worker_id="integration-worker",
                client=client,
                now=BASE + timedelta(seconds=5),
            )

        assert result == "SENT"
        assert responses == [202]
        assert len(requests) == 1

        payload = json.loads(requests[0].content)

        assert payload["resource_id"] == "server-001"
        assert payload["status"] == "UP"
        assert payload["event_type"] == "STATUS_CHANGED"

        # Повторяем точно такое же событие.
        # Получатель должен распознать дубликат.
        duplicate = receiver.post(
            "/api/v1/status/events",
            content=requests[0].content,
            headers={
                "Authorization": "Bearer integration-test-token",
                "Content-Type": "application/json",
            },
        )

        assert duplicate.status_code == 200
        assert duplicate.json()["result"] == "duplicate"

    # Проверяем, что отправитель зафиксировал доставку
    with read_transaction(engine) as conn:
        event = conn.execute(text("""
            SELECT delivery_status, attempts
            FROM outbox_events
        """)).mappings().one()

        attempt = conn.execute(text("""
            SELECT http_status_code, outcome
            FROM delivery_attempts
        """)).mappings().one()

    assert event["delivery_status"] == "SENT"
    assert event["attempts"] == 1

    assert attempt["http_status_code"] == 202
    assert attempt["outcome"] == "SENT"
