
"""Запуск фоновой отправки событий мониторинга ТПУ."""

import asyncio
import logging
import signal

from app.config import AppSettings
from app.database import get_engine
from services.sender_worker import OutboxSenderWorker


async def main():
    settings = AppSettings()

    # Проверяем конфигурацию до запуска.
    if settings.event_post_url is None:
        raise ValueError(
            "Укажите MONITORING_EVENT_POST_URL в .env"
        )

    if (
        settings.event_token is None
        or not settings.event_token.get_secret_value().strip()
    ):
        raise ValueError(
            "Укажите MONITORING_EVENT_TOKEN в .env"
        )

    logging.basicConfig(
        level=settings.app_log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Не выводим адреса HTTP-запросов в INFO-логах.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    worker = OutboxSenderWorker(
        engine=get_engine(),
        settings=settings,
        poll_seconds=1.0,
    )

    loop = asyncio.get_running_loop()

    # SIGINT (Ctrl+C) и SIGTERM останавливают обработчик
    # после завершения текущей отправки.
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)

    try:
        await worker.run()
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    asyncio.run(main())
