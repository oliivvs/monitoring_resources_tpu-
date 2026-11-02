
"""Тесты HTTP-отправки событий из Outbox."""
import asyncio

from services.events import enqueue_heartbeat_if_due
from services.sender_worker import OutboxSenderWorker
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy import text

from app.config import AppSettings
from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)
from services.resources import upsert_configuration
from services.sender import send_one_event
from services.status import apply_check_result
from shared.schemas import CheckResultIn, ResourceConfig


BASE = datetime(2026, 11, 2, 8, 0, tzinfo=timezone.utc)


@pytest.fixture
def engine(tmp_path):
    # Создаём отдельную тестовую базу данных
    db = make_engine(tmp_path / "sender_test.sqlite")
    initialize(db)

    yield db

    db.dispose()


def create_test_event(engine):
    # Регистрируем тестовый ресурс
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

    # Две успешные проверки переводят ресурс в UP
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


@pytest.mark.parametrize("status_code", [200, 202])
def test_successful_delivery(engine, status_code):
    create_test_event(engine)

    captured_requests = []

    # Имитируем сервер, принимающий события
    def handle_request(request):
        captured_requests.append(request)
        return httpx.Response(status_code=status_code)

    transport = httpx.MockTransport(handle_request)

    settings = AppSettings(
        event_post_url="https://mock.example/api/v1/status/events",
        event_token="test-secret-token",
    )

    with httpx.Client(transport=transport) as client:
        result = send_one_event(
            engine,
            settings,
            worker_id="test-worker",
            client=client,
            now=BASE + timedelta(seconds=5),
        )

    # Проверяем результат отправки
    assert result == "SENT"
    assert len(captured_requests) == 1

    request = captured_requests[0]

    assert request.method == "POST"
    assert request.url.path == "/api/v1/status/events"
    assert request.headers["Authorization"] == (
        "Bearer test-secret-token"
    )
    assert request.headers["Content-Type"] == "application/json"

    # Проверяем отправленный JSON
    payload = json.loads(request.content)

    assert payload["resource_id"] == "server-001"
    assert payload["status"] == "UP"
    assert payload["event_type"] == "STATUS_CHANGED"
    assert payload["event_id"]

    # Проверяем, что SQLite сохранила результат
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

    assert attempt["http_status_code"] == status_code
    assert attempt["outcome"] == "SENT"

@pytest.mark.parametrize(
    ("status_code", "expected_status", "expected_outcome"),
    [
        (400, "FAILED", "FAILED"),
        (429, "PENDING", "RETRY"),
        (500, "PENDING", "RETRY"),
    ],
)
def test_http_error_delivery(
    engine, status_code, expected_status, expected_outcome
):
    create_test_event(engine)

    # Имитируем ответ внешнего сервера
    def handle_request(request):
        headers = {}

        if status_code == 429:
            headers["Retry-After"] = "10"

        return httpx.Response(
            status_code=status_code,
            headers=headers,
        )

    transport = httpx.MockTransport(handle_request)

    settings = AppSettings(
        event_post_url="https://mock.example/api/v1/status/events",
        event_token="test-secret-token",
    )

    with httpx.Client(transport=transport) as client:
        result = send_one_event(
            engine,
            settings,
            worker_id="test-worker",
            client=client,
            now=BASE + timedelta(seconds=5),
        )

    assert result == expected_status

    # Проверяем сохранённое состояние Outbox
    with read_transaction(engine) as conn:
        event = conn.execute(text("""
            SELECT delivery_status, attempts,
                   next_attempt_at
            FROM outbox_events
        """)).mappings().one()

        attempt = conn.execute(text("""
            SELECT http_status_code, outcome
            FROM delivery_attempts
        """)).mappings().one()

    assert event["delivery_status"] == expected_status
    assert event["attempts"] == 1
    assert attempt["http_status_code"] == status_code
    assert attempt["outcome"] == expected_outcome

    if status_code == 400:
        # Постоянная ошибка: повторять не нужно
        assert event["next_attempt_at"] is None
    else:
        # Временная ошибка: назначена повторная попытка
        assert event["next_attempt_at"] is not None

        expected_delay = 10 if status_code == 429 else 2

        from shared.schemas import to_utc_iso

        expected_time = to_utc_iso(
            BASE + timedelta(seconds=5 + expected_delay)
        )
        assert event["next_attempt_at"] == expected_time


def test_network_timeout_retries(engine):
    create_test_event(engine)

    # Имитируем таймаут сетевого запроса
    def handle_request(request):
        raise httpx.ReadTimeout(
            "Request timed out",
            request=request,
        )

    transport = httpx.MockTransport(handle_request)

    settings = AppSettings(
        event_post_url="https://mock.example/api/v1/status/events",
        event_token="test-secret-token",
    )

    with httpx.Client(transport=transport) as client:
        result = send_one_event(
            engine,
            settings,
            worker_id="test-worker",
            client=client,
            now=BASE + timedelta(seconds=5),
        )

    # Таймаут не должен приводить к потере события
    assert result == "PENDING"

    with read_transaction(engine) as conn:
        event = conn.execute(text("""
            SELECT delivery_status, attempts, last_error
            FROM outbox_events
        """)).mappings().one()

        attempt = conn.execute(text("""
            SELECT http_status_code, outcome
            FROM delivery_attempts
        """)).mappings().one()

    assert event["delivery_status"] == "PENDING"
    assert event["attempts"] == 1
    assert event["last_error"] == "ReadTimeout"

    assert attempt["http_status_code"] is None
    assert attempt["outcome"] == "RETRY"



def test_retry_after_server_recovery(engine):
    create_test_event(engine)

    requests = []
    responses = [500, 200]

    # Сначала сервер возвращает ошибку, затем успех
    def handle_request(request):
        requests.append(request)
        return httpx.Response(
            status_code=responses.pop(0)
        )

    transport = httpx.MockTransport(handle_request)

    settings = AppSettings(
        event_post_url="https://mock.example/api/v1/status/events",
        event_token="test-secret-token",
    )

    with httpx.Client(transport=transport) as client:
        # Первая попытка: HTTP 500
        first = send_one_event(
            engine,
            settings,
            worker_id="test-worker",
            client=client,
            now=BASE + timedelta(seconds=5),
        )

        assert first == "PENDING"
        assert len(requests) == 1

        # До окончания задержки событие недоступно
        too_early = send_one_event(
            engine,
            settings,
            worker_id="test-worker",
            client=client,
            now=BASE + timedelta(seconds=6),
        )

        assert too_early is None
        assert len(requests) == 1

        # Через 2 секунды повторяем отправку
        second = send_one_event(
            engine,
            settings,
            worker_id="test-worker",
            client=client,
            now=BASE + timedelta(seconds=7),
        )

        assert second == "SENT"
        assert len(requests) == 2

    # В обеих попытках передавалось одно событие
    first_payload = json.loads(requests[0].content)
    second_payload = json.loads(requests[1].content)

    assert first_payload == second_payload
    assert first_payload["event_id"] == second_payload["event_id"]

    # Проверяем итоговое состояние в SQLite
    with read_transaction(engine) as conn:
        event = conn.execute(text("""
            SELECT delivery_status, attempts
            FROM outbox_events
        """)).mappings().one()

        attempts = conn.execute(text("""
            SELECT attempt_number, http_status_code, outcome
            FROM delivery_attempts
            ORDER BY attempt_number
        """)).mappings().all()

    assert event["delivery_status"] == "SENT"
    assert event["attempts"] == 2

    assert len(attempts) == 2

    assert attempts[0]["http_status_code"] == 500
    assert attempts[0]["outcome"] == "RETRY"

    assert attempts[1]["http_status_code"] == 200
    assert attempts[1]["outcome"] == "SENT"

@pytest.mark.asyncio
async def test_worker_sends_events_and_stops(engine):
    # Создаём первое событие STATUS_CHANGED
    create_test_event(engine)

    # Добавляем второе событие HEARTBEAT
    with write_transaction(engine) as conn:
        heartbeat_id = enqueue_heartbeat_if_due(
            conn,
            resource_id="server-001",
            now=BASE + timedelta(seconds=302),
        )

    assert heartbeat_id is not None

    received = []

    # Имитируем внешний сервер
    def handle_request(request):
        received.append(json.loads(request.content))
        return httpx.Response(status_code=200)

    transport = httpx.MockTransport(handle_request)

    settings = AppSettings(
        event_post_url="https://mock.example/api/v1/status/events",
        event_token="test-secret-token",
    )

    worker = OutboxSenderWorker(
        engine,
        settings,
        poll_seconds=0.05,
        worker_id="test-background-worker",
    )

    with httpx.Client(transport=transport) as client:
        # Запускаем обработчик в фоне
        task = asyncio.create_task(
            worker.run(client=client)
        )

        try:
            # Ждём, пока оба события станут SENT
            async def wait_for_delivery():
                while True:
                    with read_transaction(engine) as conn:
                        sent_count = conn.execute(text("""
                            SELECT COUNT(*)
                            FROM outbox_events
                            WHERE delivery_status = 'SENT'
                        """)).scalar_one()

                    if sent_count == 2:
                        return

                    await asyncio.sleep(0.02)

            await asyncio.wait_for(
                wait_for_delivery(),
                timeout=3,
            )

        finally:
            # Останавливаем обработчик даже при ошибке
            worker.stop()
            await asyncio.wait_for(task, timeout=3)

    # Получатель должен принять два события
    assert len(received) == 2

    event_types = {event["event_type"] for event in received}

    assert event_types == {
        "STATUS_CHANGED",
        "HEARTBEAT",
    }

    # Проверяем историю доставок
    with read_transaction(engine) as conn:
        statuses = conn.execute(text("""
            SELECT delivery_status, attempts
            FROM outbox_events
        """)).mappings().all()

        attempts_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM delivery_attempts
        """)).scalar_one()

    assert len(statuses) == 2
    assert all(row["delivery_status"] == "SENT" for row in statuses)
    assert all(row["attempts"] == 1 for row in statuses)
    assert attempts_count == 2

    # После stop() фоновая задача завершилась
    assert task.done()

@pytest.mark.parametrize(
    ("url", "token", "error"),
    [
        (None, "secret", "MONITORING_EVENT_POST_URL"),
        (
            "https://mock.example/events",
            None,
            "MONITORING_EVENT_TOKEN",
        ),
    ],
)
def test_missing_settings_does_not_claim_event(
    engine, url, token, error
):
    create_test_event(engine)

    settings = AppSettings(
        _env_file=None,
        event_post_url=url,
        event_token=token,
    )

    with pytest.raises(ValueError, match=error):
        send_one_event(
            engine,
            settings,
            worker_id="test-worker",
            now=BASE + timedelta(seconds=5),
        )

    # Событие не должно потеряться или заблокироваться
    with read_transaction(engine) as conn:
        event = conn.execute(text("""
            SELECT delivery_status, attempts, lease_owner
            FROM outbox_events
        """)).mappings().one()

    assert event["delivery_status"] == "PENDING"
    assert event["attempts"] == 0
    assert event["lease_owner"] is None


def test_http_request_does_not_lock_sqlite(engine):
    create_test_event(engine)

    def handle_request(request):
        # Во время HTTP-запроса SQLite должна быть доступна
        # для отдельной операции записи
        with write_transaction(engine) as conn:
            conn.execute(text("""
                UPDATE resources
                SET name = 'Updated Server'
                WHERE id = 'server-001'
            """))

        return httpx.Response(status_code=200)

    settings = AppSettings(
        event_post_url="https://mock.example/events",
        event_token="test-secret-token",
    )

    transport = httpx.MockTransport(handle_request)

    with httpx.Client(transport=transport) as client:
        result = send_one_event(
            engine,
            settings,
            worker_id="test-worker",
            client=client,
            now=BASE + timedelta(seconds=5),
        )

    assert result == "SENT"

    with read_transaction(engine) as conn:
        name = conn.execute(text("""
            SELECT name FROM resources
            WHERE id = 'server-001'
        """)).scalar_one()

    assert name == "Updated Server"


@pytest.mark.asyncio
async def test_worker_stops_with_empty_queue(engine):
    settings = AppSettings(
        event_post_url="https://mock.example/events",
        event_token="test-secret-token",
    )

    worker = OutboxSenderWorker(
        engine,
        settings,
        poll_seconds=60,
    )

    requests = []

    def handle_request(request):
        requests.append(request)
        return httpx.Response(status_code=200)

    transport = httpx.MockTransport(handle_request)

    with httpx.Client(transport=transport) as client:
        task = asyncio.create_task(worker.run(client=client))

        await asyncio.sleep(0.05)

        worker.stop()
        await asyncio.wait_for(task, timeout=3)

    assert task.done()
    assert requests == []
