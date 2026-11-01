"""Интеграция проверок Никиты с общим транзакционным слоем v2.1."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from checks.probes import probe
from checks.security import NetworkPolicy, TargetNotAllowed
from db.session import read_transaction, write_transaction
from services.resources import get_active_check_snapshot
from services.status import StaleConfigurationResult, apply_check_result

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RunOutcome:
    resource_id: str
    config_version_id: int | None
    disposition: str  # APPLIED | STALE | DENIED | INACTIVE
    check_id: int | None = None
    status: str | None = None
    event_id: str | None = None


async def check_and_store(engine: Engine, resource_id: str, policy: NetworkPolicy) -> RunOutcome:
    """Один цикл: snapshot → сетевой запрос → атомарное применение результата.

    DB-операции выполняются в отдельных потоках, не блокируют event loop.
    Между snapshot и сохранением никакая транзакция БД не удерживается.
    """
    def read_snapshot():
        with read_transaction(engine) as conn:
            return get_active_check_snapshot(conn, resource_id=resource_id)

    try:
        snapshot = await asyncio.to_thread(read_snapshot)
    except (ValueError, LookupError) as exc:
        # Отсутствующий ресурс (LookupError) — ошибка настройки, не статус DOWN.
        logger.warning("Невозможно начать проверку resource_id=%s: %s", resource_id, type(exc).__name__)
        return RunOutcome(resource_id, None, "INACTIVE")
    try:
        result = await probe(snapshot, policy)
    except TargetNotAllowed:
        logger.warning("Проверка запрещена сетевой политикой: resource_id=%s", resource_id)
        return RunOutcome(resource_id, snapshot.config_version_id, "DENIED")

    def write_result():
        with write_transaction(engine) as conn:
            return apply_check_result(conn, result=result)

    try:
        outcome = await asyncio.to_thread(write_result)
    except StaleConfigurationResult:
        logger.info("Отклонён результат устаревшей конфигурации: resource_id=%s version=%s",
                    resource_id, snapshot.config_version_id)
        return RunOutcome(resource_id, snapshot.config_version_id, "STALE")
    except ValueError as exc:
        # Другой обработчик мог записать более свежий результат.
        if "Повторная или устаревшая проверка" in str(exc):
            return RunOutcome(resource_id, snapshot.config_version_id, "STALE")
        raise
    logger.info("Применена проверка resource_id=%s version=%s status=%s",
                resource_id, snapshot.config_version_id, outcome["current_status"])
    return RunOutcome(resource_id, snapshot.config_version_id, "APPLIED",
                      check_id=outcome["check_id"], status=outcome["current_status"],
                      event_id=outcome["event_id"])
