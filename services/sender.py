
"""Отправка событий из Outbox во внешний HTTP API."""

import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx
from sqlalchemy.engine import Engine

from app.config import AppSettings
from db.session import write_transaction
from services.events import claim_one_event, finish_delivery


def parse_retry_after(value: str | None, now: datetime) -> int | None:
    """Преобразует Retry-After в количество секунд."""
    if value is None:
        return None

    value = value.strip()

    if value.isdecimal():
        return int(value)

    try:
        retry_at = parsedate_to_datetime(value)

        if retry_at.tzinfo is None:
            return None

        seconds = (retry_at - now).total_seconds()
        return max(0, math.ceil(seconds))
    except (TypeError, ValueError, OverflowError):
        return None


def send_one_event(
    engine: Engine,
    settings: AppSettings,
    *,
    worker_id: str,
    client: httpx.Client | None = None,
    now: datetime | None = None,
) -> str | None:
    """Отправляет одно ожидающее событие.

    Возвращает SENT, PENDING, FAILED или None,
    если событий для отправки нет.
    """

    # Проверяем настройки до резервирования события.
    if settings.event_post_url is None:
        raise ValueError("Не задан MONITORING_EVENT_POST_URL")

    if settings.event_token is None:
        raise ValueError("Не задан MONITORING_EVENT_TOKEN")

    token = settings.event_token.get_secret_value()

    if not token.strip():
        raise ValueError("Токен отправки событий пуст")

    started_at = now or datetime.now(timezone.utc)

    # Резервируем событие и завершаем транзакцию.
    lease_seconds = max(
        60,
        math.ceil(settings.event_timeout_seconds) + 30,
    )

    with write_transaction(engine) as conn:
        event = claim_one_event(
            conn,
            worker_id=worker_id,
            now=started_at,
            lease_seconds=lease_seconds,
        )

    if event is None:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    url = str(settings.event_post_url)
    http_status = None
    error_message = None
    retry_after = None

    def post_event(http_client: httpx.Client) -> httpx.Response:
        return http_client.post(
            url,
            content=event["payload_json"],
            headers=headers,
            timeout=settings.event_timeout_seconds,
            follow_redirects=False,
        )

    # Важно: во время HTTP-запроса транзакция БД закрыта.
    try:
        if client is None:
            with httpx.Client() as http_client:
                response = post_event(http_client)
        else:
            response = post_event(client)

        http_status = response.status_code

        if http_status not in (200, 202):
            error_message = f"HTTP {http_status}"

        if http_status == 429:
            retry_after = parse_retry_after(
                response.headers.get("Retry-After"),
                now or datetime.now(timezone.utc),
            )

    except httpx.RequestError as exc:
        # Не сохраняем текст исключения: он может содержать секреты.
        error_message = type(exc).__name__

    # После запроса отдельно фиксируем результат доставки.
    finished_at = now or datetime.now(timezone.utc)

    with write_transaction(engine) as conn:
        return finish_delivery(
            conn,
            event_id=event["event_id"],
            worker_id=worker_id,
            http_status_code=http_status,
            error_message=error_message,
            retry_after_seconds=retry_after,
            now=finished_at,
        )
