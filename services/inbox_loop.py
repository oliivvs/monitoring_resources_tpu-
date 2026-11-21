
"""A21/A22: автоматический цикл обработки Oracle Sync Inbox."""

from __future__ import annotations

import asyncio
import logging
import math

from sqlalchemy.engine import Engine

from services.inbox_worker import (
    InboxHandler,
    InboxPreparer,
    InboxPreparedHandler,
    process_one_inbox_event,
    process_one_inbox_event_async,
)


logger = logging.getLogger(__name__)


class InboxLoop:
    """Обрабатывает очередь до штатной остановки."""

    def __init__(
        self,
        engine: Engine,
        handler: InboxHandler | None = None,
        *,
        prepare: InboxPreparer | None = None,
        apply: InboxPreparedHandler | None = None,
        poll_seconds: float = 1.0,
        lease_seconds: int = 60,
        max_attempts: int = 5,
        retry_base_seconds: int = 5,
    ) -> None:

        # Ровно один режим:
        # старый handler ИЛИ новая пара prepare + apply.
        if handler is not None:
            if (
                not callable(handler)
                or prepare is not None
                or apply is not None
            ):
                raise ValueError(
                    "Нельзя смешивать режимы Inbox Worker"
                )
        elif not callable(prepare) or not callable(apply):
            raise ValueError(
                "Требуются prepare и apply либо handler"
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
        self._prepare = prepare
        self._apply = apply

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
        """Запускает цикл обработки Inbox."""

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
                    if self._prepare is not None:
                        # Новый режим:
                        # подготовка вне транзакции.
                        result = (
                            await process_one_inbox_event_async(
                                self._engine,
                                self._prepare,
                                self._apply,
                                lease_seconds=(
                                    self._lease_seconds
                                ),
                                max_attempts=(
                                    self._max_attempts
                                ),
                                retry_base_seconds=(
                                    self._retry_base_seconds
                                ),
                            )
                        )
                    else:
                        # Старый синхронный режим A21.
                        result = await asyncio.to_thread(
                            process_one_inbox_event,
                            self._engine,
                            self._handler,
                            lease_seconds=(
                                self._lease_seconds
                            ),
                            max_attempts=(
                                self._max_attempts
                            ),
                            retry_base_seconds=(
                                self._retry_base_seconds
                            ),
                        )

                except Exception as exc:
                    logger.error(
                        "Ошибка Inbox Worker: %s",
                        type(exc).__name__,
                    )

                if self._stop.is_set():
                    break

                if (
                    result is not None
                    and result.disposition != "STALE"
                ):
                    continue

                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self._poll_seconds,
                    )
                except TimeoutError:
                    pass

        finally:
            logger.info("Inbox Worker остановлен")
