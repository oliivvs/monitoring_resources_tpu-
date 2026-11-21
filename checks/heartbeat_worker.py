
"""N22: периодическая постановка HEARTBEAT в существующий Outbox.

Worker не проверяет сеть и не отправляет HTTP POST.
Он использует services.events.enqueue_heartbeat_if_due(), которая
сохраняет время последней реальной проверки в checked_at.
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from db.session import write_transaction
from services.events import enqueue_heartbeat_if_due
from shared.schemas import to_utc_iso

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HeartbeatBatch:
    scanned: int
    queued: int
    event_ids: tuple[str, ...]


def enqueue_due_heartbeats(
    engine: Engine,
    *,
    now: datetime | None = None,
) -> HeartbeatBatch:
    """Один проход по активным ресурсам, атомарный относительно SQLite.

    Повторный вызов с тем же временем не создаёт новых HEARTBEAT.
    Ресурсы без реальных проверок и отключённые ресурсы пропускаются.
    """
    if now is not None:
        to_utc_iso(now)  # Запрещаем datetime без часового пояса.

    with write_transaction(engine) as conn:
        resource_ids = conn.execute(
            text("""
                SELECT r.id
                FROM resources AS r
                JOIN resource_states AS s ON s.resource_id = r.id
                JOIN resource_config_versions AS c
                  ON c.id = s.config_version_id
                WHERE r.active = 1
                  AND c.retired_at IS NULL
                  AND s.last_checked_at IS NOT NULL
                ORDER BY r.id
            """)
        ).scalars().all()

        created: list[str] = []
        for resource_id in resource_ids:
            event_id = enqueue_heartbeat_if_due(
                conn,
                resource_id=resource_id,
                now=now,
            )
            if event_id is not None:
                created.append(str(event_id))

    return HeartbeatBatch(
        scanned=len(resource_ids),
        queued=len(created),
        event_ids=tuple(created),
    )


class HeartbeatWorker:
    """Опрашивает SQLite и создаёт только наступившие HEARTBEAT."""

    def __init__(self, engine: Engine, *, poll_seconds: float = 10.0):
        if not math.isfinite(poll_seconds) or not 0.05 <= poll_seconds <= 60:
            raise ValueError("poll_seconds должен быть от 0.05 до 60")

        self._engine = engine
        self._poll_seconds = poll_seconds
        self._stop = asyncio.Event()
        self._started = False

    def stop(self) -> None:
        """Завершить цикл после текущего прохода."""
        self._stop.set()

    async def run_once(self, *, now: datetime | None = None) -> HeartbeatBatch:
        """Один проход; работа с SQLite вынесена из asyncio event loop."""
        return await asyncio.to_thread(
            enqueue_due_heartbeats,
            self._engine,
            now=now,
        )

    async def run(self) -> None:
        """Работать до stop(), не останавливаясь при ошибке одного прохода."""
        if self._started:
            raise RuntimeError("Повторный запуск HeartbeatWorker запрещён")

        self._started = True
        try:
            while not self._stop.is_set():
                try:
                    result = await self.run_once()
                except Exception as exc:
                    # В сообщении исключения могут быть секреты/адреса.
                    logger.error(
                        "Сбой постановки HEARTBEAT: %s",
                        type(exc).__name__,
                    )
                else:
                    logger.info(
                        "HEARTBEAT: проверено ресурсов=%d, добавлено событий=%d",
                        result.scanned,
                        result.queued,
                    )

                if self._stop.is_set():
                    break
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self._poll_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info("HeartbeatWorker остановлен")
