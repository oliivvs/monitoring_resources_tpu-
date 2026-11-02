
"""Фоновая обработка очереди HTTP-событий."""

import asyncio
import logging
from uuid import uuid4

import httpx
from sqlalchemy.engine import Engine

from app.config import AppSettings
from services.sender import send_one_event


logger = logging.getLogger(__name__)


class OutboxSenderWorker:
    """Периодически отправляет события из Outbox."""

    def __init__(
        self,
        engine: Engine,
        settings: AppSettings,
        *,
        poll_seconds: float = 1.0,
        worker_id: str | None = None,
    ):
        if not 0.05 <= poll_seconds <= 60:
            raise ValueError(
                "poll_seconds должен быть от 0.05 до 60"
            )

        self._engine = engine
        self._settings = settings
        self._poll_seconds = poll_seconds
        self._worker_id = worker_id or f"sender-{uuid4().hex}"

        self._stop = asyncio.Event()
        self._started = False

    def stop(self):
        """Запрашивает штатную остановку."""
        self._stop.set()

    async def _run_loop(self, client: httpx.Client):
        while not self._stop.is_set():
            result = None

            try:
                # Синхронная отправка выполняется в отдельном
                # потоке и не блокирует asyncio-планировщик.
                result = await asyncio.to_thread(
                    send_one_event,
                    self._engine,
                    self._settings,
                    worker_id=self._worker_id,
                    client=client,
                )

            except Exception as exc:
                # Не записываем текст ошибки: там могут
                # оказаться адреса и секретные данные.
                logger.error(
                    "Ошибка отправителя событий: %s",
                    type(exc).__name__,
                )

            if self._stop.is_set():
                break

            # Если событие отправлено или обработано,
            # проверяем очередь снова без задержки.
            if result is not None:
                continue

            # Если очередь пуста или произошла ошибка,
            # ждём следующего опроса.
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._poll_seconds,
                )
            except TimeoutError:
                pass

    async def run(
        self,
        *,
        client: httpx.Client | None = None,
    ):
        """Запускает обработку до вызова stop()."""

        if self._started:
            raise RuntimeError(
                "Повторный запуск worker запрещён"
            )

        if self._settings.event_post_url is None:
            raise ValueError(
                "Не задан MONITORING_EVENT_POST_URL"
            )

        token = self._settings.event_token
        if token is None or not token.get_secret_value().strip():
            raise ValueError(
                "Не задан MONITORING_EVENT_TOKEN"
            )

        self._started = True

        logger.info("Обработчик Outbox запущен")

        try:
            if client is None:
                with httpx.Client() as own_client:
                    await self._run_loop(own_client)
            else:
                await self._run_loop(client)
        finally:
            logger.info("Обработчик Outbox остановлен")
