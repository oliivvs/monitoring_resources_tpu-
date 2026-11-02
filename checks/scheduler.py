
"""N08 + N09: расписание и ограничение параллельных проверок."""
from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from typing import Awaitable, Callable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from checks.runner import RunOutcome, check_and_store
from checks.security import NetworkPolicy
from db.session import read_transaction

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ResourceSchedule:
    resource_id: str
    config_version_id: int
    interval_seconds: int


@dataclass(slots=True)
class _Job:
    config_version_id: int
    interval_seconds: int
    next_due: float
    task: asyncio.Task[RunOutcome] | None = None
    running_version: int | None = None


def load_active_schedules(engine: Engine) -> list[ResourceSchedule]:
    """Читаем актуальное расписание активных ресурсов из SQLite."""
    with read_transaction(engine) as conn:
        rows = conn.execute(text("""
            SELECT r.id AS resource_id, c.id AS config_version_id,
                   c.interval_seconds
            FROM resources AS r
            JOIN resource_states AS s ON s.resource_id = r.id
            JOIN resource_config_versions AS c ON c.id = s.config_version_id
            WHERE r.active = 1 AND c.retired_at IS NULL
            ORDER BY r.id
        """)).mappings().all()

    return [ResourceSchedule(**row) for row in rows]


class ResourceScheduler:
    """Периодические проверки с ограничением числа запущенных задач."""

    def __init__(
        self,
        engine: Engine,
        policy: NetworkPolicy,
        *,
        refresh_seconds: float = 1.0,
        max_concurrent_checks: int = 5,
        shutdown_timeout_seconds: float = 65.0,
        checker: Callable[
            [Engine, str, NetworkPolicy],
            Awaitable[RunOutcome],
        ] = check_and_store,
    ) -> None:
        if not math.isfinite(refresh_seconds) or not 0.01 <= refresh_seconds <= 60:
            raise ValueError(
                "refresh_seconds должен быть от 0.01 до 60"
            )

        if (
            isinstance(max_concurrent_checks, bool)
            or not isinstance(max_concurrent_checks, int)
            or not 1 <= max_concurrent_checks <= 128
        ):
            raise ValueError(
                "max_concurrent_checks должен быть целым числом от 1 до 128"
            )

        if (
            not math.isfinite(shutdown_timeout_seconds)
            or not 0 < shutdown_timeout_seconds <= 300
        ):
            raise ValueError(
                "shutdown_timeout_seconds должен быть от 0 до 300"
            )

        self._engine = engine
        self._policy = policy
        self._refresh_seconds = refresh_seconds
        self._max_concurrent_checks = max_concurrent_checks
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._checker = checker

        self._stop = asyncio.Event()
        self._jobs: dict[str, _Job] = {}
        self._started = False

    def stop(self) -> None:
        """Запрещаем новые запуски, но не обрываем текущие проверки."""
        self._stop.set()

    @staticmethod
    def _consume_result(
        resource_id: str,
        task: asyncio.Task[RunOutcome],
    ) -> None:
        try:
            result = task.result()

        except asyncio.CancelledError:
            logger.info(
                "Проверка отменена: resource_id=%r",
                resource_id,
            )

        except Exception as exc:
            # В тексте исключения могут быть URL или другие секреты.
            logger.error(
                "Сбой проверки: resource_id=%r, type=%s",
                resource_id,
                type(exc).__name__,
            )

        else:
            logger.info(
                "Проверка завершена: resource_id=%r, disposition=%s",
                resource_id,
                result.disposition,
            )

    def _refresh_jobs(
        self,
        schedules: list[ResourceSchedule],
        now: float,
    ) -> None:
        active = {item.resource_id: item for item in schedules}

        # Сначала обрабатываем завершённые проверки.
        for rid, job in list(self._jobs.items()):
            if job.task is not None and job.task.done():
                self._consume_result(rid, job.task)
                job.task = None

                if rid not in active:
                    del self._jobs[rid]
                    continue

                if job.running_version == job.config_version_id:
                    # Интервал начинается после завершения проверки.
                    job.next_due = now + job.interval_seconds
                else:
                    # Конфигурация изменилась во время проверки.
                    job.next_due = now

                job.running_version = None

            elif rid not in active and job.task is None:
                # Отключённый ресурс больше не планируем.
                del self._jobs[rid]

        # Обновляем расписание по актуальным настройкам БД.
        for item in schedules:
            job = self._jobs.get(item.resource_id)

            if job is None:
                self._jobs[item.resource_id] = _Job(
                    item.config_version_id,
                    item.interval_seconds,
                    next_due=now,
                )

            elif (
                job.config_version_id != item.config_version_id
                or job.interval_seconds != item.interval_seconds
            ):
                job.config_version_id = item.config_version_id
                job.interval_seconds = item.interval_seconds
                job.next_due = now

        # N09: считаем все выполняющиеся задачи, в том числе
        # проверки ресурсов, которые успели отключить.
        running_count = sum(
            job.task is not None
            for job in self._jobs.values()
        )

        free_slots = self._max_concurrent_checks - running_count

        if free_slots <= 0:
            return

        # N09: старейшие ожидающие проверки запускаем первыми.
        # Не создаём asyncio.Task для каждого ожидающего ресурса.
        ordered = sorted(
            schedules,
            key=lambda item: (
                self._jobs[item.resource_id].next_due,
                item.resource_id,
            ),
        )

        for item in ordered:
            if free_slots == 0:
                break

            job = self._jobs[item.resource_id]

            # Не пересекаем проверки одного ресурса.
            if job.task is not None or now < job.next_due:
                continue

            job.running_version = job.config_version_id

            job.task = asyncio.create_task(
                self._checker(
                    self._engine,
                    item.resource_id,
                    self._policy,
                ),
                name=f"check:{item.resource_id}",
            )

            free_slots -= 1

    async def _finish_running_checks(self) -> None:
        """Даём проверкам время завершиться, затем отменяем оставшиеся."""
        tasks = {
            rid: job.task
            for rid, job in self._jobs.items()
            if job.task is not None
        }

        if not tasks:
            return

        # asyncio.wait не отменяет задачи при истечении timeout.
        _, pending = await asyncio.wait(
            set(tasks.values()),
            timeout=self._shutdown_timeout_seconds,
        )

        if pending:
            logger.warning(
                "Таймаут остановки: отменяем незавершённые проверки (%d)",
                len(pending),
            )

            for task in pending:
                task.cancel()

            # Собираем результаты отменённых задач, чтобы
            # не оставить необработанные исключения.
            await asyncio.gather(
                *pending,
                return_exceptions=True,
            )

        for rid, task in tasks.items():
            self._consume_result(rid, task)

    async def run(self) -> None:
        """Главный цикл планировщика."""
        if self._started:
            raise RuntimeError(
                "Этот экземпляр планировщика можно запускать только один раз"
            )

        self._started = True

        try:
            while not self._stop.is_set():
                try:
                    # Синхронный SQLite-запрос не блокирует event loop.
                    schedules = await asyncio.to_thread(
                        load_active_schedules,
                        self._engine,
                    )

                except Exception as exc:
                    logger.error(
                        "Не удалось прочитать расписание: %s",
                        type(exc).__name__,
                    )

                else:
                    if not self._stop.is_set():
                        self._refresh_jobs(
                            schedules,
                            asyncio.get_running_loop().time(),
                        )

                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self._refresh_seconds,
                    )
                except TimeoutError:
                    pass

        finally:
            try:
                # N09: при остановке ждём уже запущенные проверки.
                await self._finish_running_checks()
            finally:
                self._jobs.clear()
