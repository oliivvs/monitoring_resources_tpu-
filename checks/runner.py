
"""Интеграция сетевых проверок с транзакционным слоем v2.1."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from checks.probes import probe
from checks.security import NetworkPolicy, TargetNotAllowed
from db.session import read_transaction, write_transaction
from services.resources import get_active_check_snapshot
from services.status import (
    StaleCheckResult,
    StaleConfigurationResult,
    apply_check_result,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RunOutcome:
    """Итог выполнения одной проверки ресурса."""

    resource_id: str
    config_version_id: int | None

    # APPLIED — результат сохранён.
    # STALE — устаревший результат отклонён.
    # DENIED — проверка запрещена сетевой политикой.
    # INACTIVE — ресурс отсутствует или отключён.
    disposition: str

    check_id: int | None = None
    status: str | None = None
    event_id: str | None = None


async def check_and_store(
    engine: Engine,
    resource_id: str,
    policy: NetworkPolicy,
) -> RunOutcome:
    """Выполняем одну проверку и сохраняем результат.

    Порядок:
    1. Получить актуальную конфигурацию.
    2. Выполнить HTTP/TCP-проверку.
    3. Сохранить результат одной транзакцией.

    Операции SQLite переносим в отдельные потоки, чтобы
    не блокировать асинхронный цикл.
    """

    def read_snapshot():
        """Получаем снимок конфигурации до сетевого запроса."""
        with read_transaction(engine) as conn:
            return get_active_check_snapshot(
                conn,
                resource_id=resource_id,
            )

    try:
        snapshot = await asyncio.to_thread(read_snapshot)

    except (ValueError, LookupError) as exc:
        # Отсутствие ресурса не означает его недоступность.
        logger.warning(
            "Невозможно начать проверку: "
            "resource_id=%s, type=%s",
            resource_id,
            type(exc).__name__,
        )

        return RunOutcome(
            resource_id,
            None,
            "INACTIVE",
        )

    # Выполняем сетевой запрос по разрешённой конфигурации.
    try:
        result = await probe(snapshot, policy)

    except TargetNotAllowed:
        # Запрещённая проверка не должна менять статус на DOWN.
        logger.warning(
            "Проверка запрещена сетевой политикой: "
            "resource_id=%s",
            resource_id,
        )

        return RunOutcome(
            resource_id,
            snapshot.config_version_id,
            "DENIED",
        )

    def write_result():
        """Сохраняем проверку, статус и событие атомарно."""
        with write_transaction(engine) as conn:
            return apply_check_result(
                conn,
                result=result,
            )

    try:
        outcome = await asyncio.to_thread(write_result)

    except (StaleConfigurationResult, StaleCheckResult) as exc:
        # N13: различаем ошибки по типу исключения.
        # Не проверяем текст ошибки на русском языке.
        #
        # Устаревшая проверка не должна менять состояние БД.
        logger.info(
            "Отклонён устаревший результат: "
            "resource_id=%s version=%s reason=%s",
            resource_id,
            snapshot.config_version_id,
            type(exc).__name__,
        )

        return RunOutcome(
            resource_id,
            snapshot.config_version_id,
            "STALE",
        )

    # Результат применён к актуальной конфигурации ресурса.
    logger.info(
        "Применена проверка: "
        "resource_id=%s version=%s status=%s",
        resource_id,
        snapshot.config_version_id,
        outcome["current_status"],
    )

    return RunOutcome(
        resource_id=resource_id,
        config_version_id=snapshot.config_version_id,
        disposition="APPLIED",
        check_id=outcome["check_id"],
        status=outcome["current_status"],
        event_id=outcome["event_id"],
    )
