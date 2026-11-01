"""Единичные HTTP/TCP-проверки без работы с БД и без изменения статусов.

ТЗ: GET, проверка допустимого HTTP-кода, TCP connect, время ответа,
обработка таймаутов и сетевых ошибок, без следования редиректам.
"""
from __future__ import annotations

import asyncio
import socket
import time
from datetime import datetime, timezone

import httpx

from checks.security import NetworkPolicy, ResolutionFailure, authorize_destination
from checks.pinned_http import PinnedHTTPTransport
from shared.schemas import CheckResultIn, CheckSnapshot, CheckType


def _matches_http_status(status: int, snapshot: CheckSnapshot) -> bool:
    return any(r.start <= status <= r.end for r in snapshot.expected_http_codes)


async def probe(snapshot: CheckSnapshot, policy: NetworkPolicy) -> CheckResultIn:
    """Возвращает CheckResultIn с версией, захваченной перед сетевым I/O.

    TargetNotAllowed распространяется вызывающему коду и НЕ считается падением службы.
    Отмена задачи не перехватывается. Обычные сетевые ошибки возвращают success=False.
    """
    started = time.perf_counter()
    status_code: int | None = None
    error: str | None = None
    success = False
    try:
        # Один общий deadline покрывает DNS + connect + HTTP headers.
        async with asyncio.timeout(snapshot.timeout_seconds):
            target = await authorize_destination(snapshot, policy)
            if snapshot.check_type == CheckType.HTTP:
                transport = PinnedHTTPTransport(
                    hostname=target.host, port=target.port, ips=target.ips
                )
                async with httpx.AsyncClient(
                    transport=transport,
                    timeout=snapshot.timeout_seconds,
                    follow_redirects=False,
                    trust_env=False,
                ) as client:
                    # stream не скачивает тело GET-ответа, но корректно закрывает его.
                    async with client.stream('GET', snapshot.target, follow_redirects=False) as resp:
                        status_code = resp.status_code
                        success = _matches_http_status(status_code, snapshot)
                        if not success:
                            error = f"Недопустимый HTTP-код: {status_code}"
            elif snapshot.check_type == CheckType.TCP:
                last_failure = None
                for ip in target.ips:
                    try:
                        reader, writer = await asyncio.open_connection(ip, target.port)
                        break
                    except OSError as exc:
                        last_failure = exc
                else:
                    raise ConnectionError("All authorised IPs refused the connection") from last_failure
                try:
                    success = True
                finally:
                    writer.close()
                    await writer.wait_closed()
            else:
                # Не должно достигаться, тип CheckSnapshot проверяет Pydantic.
                raise ValueError("Тип проверки не реализован")
    except TimeoutError:
        error = "Превышено время ожидания"
    except ResolutionFailure:
        error = "Ошибка DNS"
    except httpx.TimeoutException:
        error = "Превышено время ожидания"
    except (httpx.RequestError, OSError, ConnectionError) as exc:
        # Не сохраняем URL, query, подробности TLS, секреты и содержимое ответа.
        error = f"Ошибка сети: {type(exc).__name__}"
    # Соединение могло открыться, но последующее закрытие/чтение завершиться
    # ошибкой. Не возвращаем противоречие success=True с error_message.
    if error is not None:
        success = False
    checked_at = datetime.now(timezone.utc)
    return CheckResultIn(
        resource_id=snapshot.resource_id,
        config_version_id=snapshot.config_version_id,
        checked_at=checked_at,
        success=success,
        response_time_ms=round((time.perf_counter() - started) * 1000, 3),
        http_status_code=status_code,
        error_message=error,
    )
