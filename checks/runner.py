
"""Запуск сетевой проверки и безопасное логирование её результата (N15)."""

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
    """Результат одной проверки для планировщика."""

    resource_id: str
    config_version_id: int | None
    disposition: str  # APPLIED | STALE | DENIED | INACTIVE
    check_id: int | None = None
    status: str | None = None
    event_id: str | None = None


async def check_and_store(
    engine: Engine,
    resource_id: str,
    policy: NetworkPolicy,
) -> RunOutcome:
    """Снимок конфигурации -> HTTP/TCP -> транзакционное сохранение.

    В логах нет URL, query-параметров, текстов сетевых ошибок
    и traceback: они могут содержать учётные данные.
    """

    def read_snapshot():
        with read_transaction(engine) as conn:
            return get_active_check_snapshot(conn, resource_id=resource_id)

    try:
        # Синхронный SQLite-запрос переносим из event loop в поток.
        snapshot = await asyncio.to_thread(read_snapshot)
    except (ValueError, LookupError) as exc:
        # Отсутствующий или отключённый ресурс — не состояние DOWN.
        logger.warning(
            "monitoring.check outcome=INACTIVE resource_id=%r error_type=%s",
            resource_id,
            type(exc).__name__,
        )
        return RunOutcome(resource_id, None, "INACTIVE")
    except Exception as exc:
        # Не скрываем непредвиденную ошибку чтения БД.
        logger.error(
            "monitoring.check outcome=ERROR phase=snapshot "
            "resource_id=%r error_type=%s",
            resource_id,
            type(exc).__name__,
        )
        raise

    try:
        result = await probe(snapshot, policy)
    except TargetNotAllowed:
        # Политика сети запретила проверку: не записываем её как отказ.
        logger.warning(
            "monitoring.check outcome=DENIED resource_id=%r version=%s",
            resource_id,
            snapshot.config_version_id,
        )
        return RunOutcome(resource_id, snapshot.config_version_id, "DENIED")
    except asyncio.CancelledError:
        # Отмену должен получить планировщик, не маскируем её.
        logger.info(
            "monitoring.check outcome=CANCELLED phase=probe "
            "resource_id=%r version=%s",
            resource_id,
            snapshot.config_version_id,
        )
        raise
    except Exception as exc:
        # Не логируем str(exc), target или error_message.
        logger.error(
            "monitoring.check outcome=ERROR phase=probe "
            "resource_id=%r version=%s error_type=%s",
            resource_id,
            snapshot.config_version_id,
            type(exc).__name__,
        )
        raise

    def write_result():
        with write_transaction(engine) as conn:
            return apply_check_result(conn, result=result)

    try:
        outcome = await asyncio.to_thread(write_result)
    except (StaleConfigurationResult, StaleCheckResult) as exc:
        # Просроченный результат не меняет состояние или историю.
        logger.info(
            "monitoring.check outcome=STALE resource_id=%r "
            "version=%s reason=%s",
            resource_id,
            snapshot.config_version_id,
            type(exc).__name__,
        )
        return RunOutcome(resource_id, snapshot.config_version_id, "STALE")
    except asyncio.CancelledError:
        # Важно: отмена await НЕ останавливает уже запущенный поток SQLite.
        logger.warning(
            "monitoring.check outcome=CANCELLED phase=persist "
            "resource_id=%r version=%s; database_write_may_continue=true",
            resource_id,
            snapshot.config_version_id,
        )
        raise
    except Exception as exc:
        # Ошибку БД не считаем сетевой недоступностью ресурса.
        logger.error(
            "monitoring.check outcome=ERROR phase=persist "
            "resource_id=%r version=%s error_type=%s",
            resource_id,
            snapshot.config_version_id,
            type(exc).__name__,
        )
        raise

    # Одна итоговая запись для применённой проверки, без тела ответа и URL.
    logger.log(
        logging.INFO if result.success else logging.WARNING,
        "monitoring.check outcome=APPLIED resource_id=%r version=%s "
        "check_id=%s success=%s status=%s http_code=%s "
        "duration_ms=%s event_id=%s",
        resource_id,
        snapshot.config_version_id,
        outcome["check_id"],
        result.success,
        outcome["current_status"],
        result.http_status_code,
        result.response_time_ms,
        outcome["event_id"],
    )

    return RunOutcome(
        resource_id=resource_id,
        config_version_id=snapshot.config_version_id,
        disposition="APPLIED",
        check_id=outcome["check_id"],
        status=outcome["current_status"],
        event_id=outcome["event_id"],
    )
