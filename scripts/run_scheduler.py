
"""Запуск планировщика мониторинга ресурсов ТПУ."""

import asyncio
import logging
import os
import signal

from app.config import AppSettings
from app.database import get_engine
from checks.scheduler import ResourceScheduler
from checks.security import NetworkPolicy


def load_network_policy() -> NetworkPolicy:
    """Получаем разрешённые адреса и порты из окружения."""

    hosts_raw = os.getenv("MONITORING_ALLOWED_HOSTS", "")
    ports_raw = os.getenv("MONITORING_ALLOWED_PORTS", "")

    hosts = frozenset(
        host.strip()
        for host in hosts_raw.split(",")
        if host.strip()
    )

    ports = frozenset(
        int(port.strip())
        for port in ports_raw.split(",")
        if port.strip()
    )

    # По умолчанию сетевые проверки запрещены.
    return NetworkPolicy(
        allowed_hosts=hosts,
        allowed_ports=ports,
        permit_loopback_for_tests=False,
    )


async def main() -> None:
    settings = AppSettings()

    logging.basicConfig(
        level=settings.app_log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Не выводим URL запросов в обычные логи.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    policy = load_network_policy()

    if not policy.allowed_hosts or not policy.allowed_ports:
        logging.warning(
            "Сетевые проверки запрещены: "
            "список разрешённых адресов или портов пуст"
        )

    engine = get_engine()

    scheduler = ResourceScheduler(
        engine=engine,
        policy=policy,
        refresh_seconds=1.0,
        max_concurrent_checks=5,
    )

    loop = asyncio.get_running_loop()

    # Корректная остановка контейнера.
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, scheduler.stop)

    logging.info("Планировщик мониторинга запущен")

    try:
        await scheduler.run()
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)

        engine.dispose()
        logging.info("Планировщик остановлен")


if __name__ == "__main__":
    asyncio.run(main())
