
"""A21: автоматический цикл обработки Oracle Sync Inbox.

Не содержит бизнес-логики INSERT/UPDATE/DELETE.
Реальный обработчик передаётся при создании InboxLoop.
"""

from __future__ import annotations

import asyncio
import logging
import math

from sqlalchemy.engine import Engine

from services.inbox_worker import (
    InboxHandler,
    process_one_inbox_event,
)


logger = logging.getLogger(__name__)


class InboxLoop:
    """Периодически обрабатывает ожидающие события Inbox."""

    def __init__(
        self,
        engine: Engine,
        handler: InboxHandler,
        *,
        poll_seconds: float = 1.0,
        lease_seconds: int = 60,
        max_attempts: int = 5,
        retry_base_seconds: int = 5,
    ) -> None:

        if not callable(handler):
            raise ValueError(
                "Требуется обработчик событий Inbox"
            )

        if (
            isinstance(poll_seconds, bool)
            or not isinstance(poll_seconds, (int, float))
            or not math.isfinite(poll_seconds)
            or not 0.01 <= poll_seconds <= 60
        ):
            raise ValueError(
                "poll_seconds должен быть от 0.01 до 60"
            )

        if (
            type(lease_seconds) is not int
            or not 1 <= lease_seconds <= 3600
        ):
            raise ValueError(
                "lease_seconds должен быть от 1 до 3600"
            )

        if (
            type(max_attempts) is not int
            or not 1 <= max_attempts <= 32
        ):
            raise ValueError(
                "max_attempts должен быть от 1 до 32"
            )

        if (
            type(retry_base_seconds) is not int
            or not 1 <= retry_base_seconds <= 3600
        ):
            raise ValueError(
                "retry_base_seconds должен быть от 1 до 3600"
            )

        self._engine = engine
        self._handler = handler

        self._poll_seconds = float(poll_seconds)
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._retry_base_seconds = retry_base_seconds

        self._stop = asyncio.Event()
        self._started = False

    def stop(self) -> None:
        """Запрашивает штатную остановку."""
        self._stop.set()

    async def run(self) -> None:
        """Запускает цикл до вызова stop()."""

        if self._started:
            raise RuntimeError(
                "Повторный запуск InboxLoop запрещён"
            )

        self._started = True

        logger.info("Inbox Worker запущен")

        try:
            while not self._stop.is_set():

                result = None

                try:
                    # SQLite обрабатывается в отдельном
                    # потоке и не блокирует asyncio.
                    result = await asyncio.to_thread(
                        process_one_inbox_event,
                        self._engine,
                        self._handler,
                        lease_seconds=self._lease_seconds,
                        max_attempts=self._max_attempts,
                        retry_base_seconds=(
                            self._retry_base_seconds
                        ),
                    )

                except Exception as exc:
                    # Не выводим текст исключения:
                    # он может содержать секреты.
                    logger.error(
                        "Ошибка Inbox Worker: %s",
                        type(exc).__name__,
                    )

                if self._stop.is_set():
                    break

                # Если событие обработано, сразу
                # проверяем наличие следующего.
                if (
                    result is not None
                    and result.disposition != "STALE"
                ):
                    continue

                # Если очередь пуста или была ошибка,
                # ожидаем следующий цикл.
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self._poll_seconds,
                    )
                except TimeoutError:
                    pass

        finally:
            logger.info("Inbox Worker остановлен")
