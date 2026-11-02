
"""N08: периодический запуск проверок активных ресурсов SQLite."""
from __future__ import annotations

import asyncio
import logging
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
    """Читаем расписание только для активных ресурсов."""
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
    """Планировщик проверок. Общий лимит параллельности добавим в N09."""

    def __init__(
        self,
        engine: Engine,
        policy: NetworkPolicy,
        *,
        refresh_seconds: float = 1.0,
        checker: Callable[
            [Engine, str, NetworkPolicy],
            Awaitable[RunOutcome],
        ] = check_and_store,
    ) -> None:
        if not 0.01 <= refresh_seconds <= 60:
            raise ValueError("refresh_seconds должен быть от 0.01 до 60")

        self._engine = engine
        self._policy = policy
        self._refresh_seconds = refresh_seconds
        self._checker = checker
        self._stop = asyncio.Event()
        self._jobs: dict[str, _Job] = {}
        self._started = False

    def stop(self) -> None:
        """Просим цикл остановиться, не обрывая выполняемую проверку."""
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
            # Не пишем str(exc): ошибка может содержать секретные данные.
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
                    # Интервал отсчитывается от окончания проверки.
                    job.next_due = now + job.interval_seconds
                else:
                    # Конфигурация обновилась во время выполнения.
                    job.next_due = now

                job.running_version = None

            elif rid not in active and job.task is None:
                # Ресурс отключили: больше не планируем его проверки.
                del self._jobs[rid]

        # Обновляем расписание и запускаем проверки, время которых пришло.
        for item in schedules:
            rid = item.resource_id
            job = self._jobs.get(rid)

            if job is None:
                job = _Job(
                    item.config_version_id,
                    item.interval_seconds,
                    next_due=now,
                )
                self._jobs[rid] = job

            elif (
                job.config_version_id != item.config_version_id
                or job.interval_seconds != item.interval_seconds
            ):
                job.config_version_id = item.config_version_id
                job.interval_seconds = item.interval_seconds

                # При изменении настроек пересчитываем расписание.
                job.next_due = now

            if job.task is None and now >= job.next_due:
                job.running_version = job.config_version_id
                job.task = asyncio.create_task(
                    self._checker(self._engine, rid, self._policy),
                    name=f"check:{rid}",
                )

    async def run(self) -> None:
        """Запускаем цикл, пока не вызовут stop()."""
        if self._started:
            raise RuntimeError(
                "Этот экземпляр планировщика можно запускать только один раз"
            )

        self._started = True

        try:
            while not self._stop.is_set():
                try:
                    # Чтение SQLite переносим в отдельный поток.
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
            # При штатной остановке дожидаемся текущих проверок.
            pending = [
                (rid, job.task)
                for rid, job in self._jobs.items()
                if job.task is not None
            ]

            if pending:
                await asyncio.gather(
                    *(task for _, task in pending),
                    return_exceptions=True,
                )

                for rid, task in pending:
                    self._consume_result(rid, task)

            self._jobs.clear()
