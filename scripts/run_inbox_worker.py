
"""Запуск отдельного процесса Oracle Sync Inbox Worker.

По умолчанию запуск запрещён.
Для включения требуется MONITORING_ORACLE_ENABLED=true.

Применяет события только из разрешённого источника
и только к разрешённым сетевым целям.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from pathlib import Path
from ipaddress import ip_address
from urllib.parse import urlsplit

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import text
from sqlalchemy.engine import Engine, make_url

from app.config import AppSettings
from app.database import get_engine
from checks.security import NetworkPolicy
from db.session import read_transaction

from integrations.tpu.entity_client import (
    EntityRoutes,
    TpuEntityClient,
)
from integrations.tpu.entity_mapper import (
    EntityMapper,
    load_mapping,
)
from integrations.tpu.oracle_inbox_handler import OracleInboxHandler

from services.inbox_loop import InboxLoop


logger = logging.getLogger(__name__)


class OracleWorkerSettings(BaseSettings):
    """Отдельные настройки фонового обработчика."""

    model_config = SettingsConfigDict(
        env_prefix="MONITORING_ORACLE_",
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    enabled: bool = False

    mapping_path: Path = Path(
        "config/tpu_oracle_mapping.yaml"
    )

    allowed_hosts: str = ""
    allowed_ports: str = ""

    # Snapshot API выключен по умолчанию.
    snapshot_enabled: bool = False
    snapshot_base_url: str | None = None
    snapshot_allowed_hosts: str = ""
    snapshot_token: SecretStr | None = None

    snapshot_timeout_seconds: float = Field(
        default=10.0,
        gt=0,
        le=30,
    )

    poll_seconds: float = Field(
        default=1.0,
        ge=0.01,
        le=60,
    )

    lease_seconds: int = Field(
        default=60,
        ge=1,
        le=3600,
    )

    prepare_timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        le=120,
    )

    max_attempts: int = Field(
        default=5,
        ge=1,
        le=32,
    )

    retry_base_seconds: int = Field(
        default=5,
        ge=1,
        le=3600,
    )


def build_network_policy(
    settings: OracleWorkerSettings,
) -> NetworkPolicy:
    """Формирует закрытую по умолчанию сетевую политику."""

    hosts = frozenset(
        host.strip()
        for host in settings.allowed_hosts.split(",")
        if host.strip()
    )

    raw_ports = settings.allowed_ports.split(",")

    ports = set()

    for value in raw_ports:
        value = value.strip()

        if not value:
            continue

        try:
            port = int(value)
        except ValueError:
            raise ValueError(
                "Некорректный MONITORING_ORACLE_ALLOWED_PORTS"
            ) from None

        ports.add(port)

    if not hosts or not ports:
        raise ValueError(
            "Нужно явно указать разрешённые hosts и ports"
        )

    return NetworkPolicy(
        allowed_hosts=hosts,
        allowed_ports=frozenset(ports),
        permit_loopback_for_tests=False,
    )


def build_snapshot_client(
    settings: OracleWorkerSettings,
) -> TpuEntityClient | None:
    """Создаёт явно разрешённый Snapshot-клиент.

    Пока используется только учебный маршрут Entity API.
    Промышленный контракт требует отдельного согласования.
    """

    if not settings.snapshot_enabled:
        return None

    if not settings.snapshot_base_url:
        raise ValueError(
            "Не указан Snapshot Base URL"
        )

    if (
        settings.snapshot_token is None
        or not settings.snapshot_token.get_secret_value().strip()
    ):
        raise ValueError(
            "Не настроен токен Snapshot API"
        )

    parsed = urlsplit(settings.snapshot_base_url)

    allowed = {
        host.strip().lower()
        for host in settings.snapshot_allowed_hosts.split(",")
        if host.strip()
    }

    hostname = (
        parsed.hostname.lower()
        if parsed.hostname is not None
        else None
    )

    if (
        parsed.scheme != "https"
        or hostname is None
        or hostname not in allowed
        or parsed.port not in (None, 443)
    ):
        raise ValueError(
            "Snapshot API требует HTTPS и разрешённый host"
        )

    # IP-адреса запрещены в этой конфигурации.
    # Предполагается явно согласованное DNS-имя API.
    try:
        ip_address(hostname)
    except ValueError:
        pass
    else:
        raise ValueError(
            "Snapshot API должен использовать DNS-имя"
        )

    return TpuEntityClient(
        base_url=settings.snapshot_base_url,
        routes=EntityRoutes.local_mock(),
        token=settings.snapshot_token,
        timeout_seconds=settings.snapshot_timeout_seconds,
    )


def validate_worker_timing(
    settings: OracleWorkerSettings,
) -> None:
    """Проверяет соотношение таймаутов и lease."""

    if (
        settings.prepare_timeout_seconds + 10
        > settings.lease_seconds
    ):
        raise ValueError(
            "lease_seconds должен оставлять минимум "
            "10 секунд после подготовки"
        )

    if (
        settings.snapshot_enabled
        and settings.snapshot_timeout_seconds
        >= settings.prepare_timeout_seconds
    ):
        raise ValueError(
            "Таймаут Snapshot API должен быть меньше "
            "таймаута подготовки"
        )


def build_worker(
    engine: Engine,
    settings: OracleWorkerSettings,
) -> InboxLoop:
    """Создаёт обработчик, но не запускает его."""

    if not settings.enabled:
        raise RuntimeError(
            "Oracle Worker выключен. "
            "Установите MONITORING_ORACLE_ENABLED=true"
        )

    validate_worker_timing(settings)

    mapping = load_mapping(settings.mapping_path)

    policy = build_network_policy(settings)

    mapper = EntityMapper(
        mapping,
        policy,
    )

    snapshot_client = build_snapshot_client(settings)

    bridge = OracleInboxHandler(
        mapper,
        snapshot_client=snapshot_client,
        prepare_timeout_seconds=settings.prepare_timeout_seconds,
    )

    return InboxLoop(
        engine,
        prepare=bridge.prepare,
        apply=bridge.apply,
        poll_seconds=settings.poll_seconds,
        lease_seconds=settings.lease_seconds,
        max_attempts=settings.max_attempts,
        retry_base_seconds=settings.retry_base_seconds,
    )


def verify_database(
    engine: Engine,
) -> None:
    """Не запускает Worker на старой схеме SQLite."""

    with read_transaction(engine) as conn:
        revision = conn.execute(
            text("""
                SELECT version_num
                FROM alembic_version
            """)
        ).scalar_one_or_none()

    if revision != "005_source_sequence":
        raise RuntimeError(
            "SQLite не соответствует миграции "
            "005_source_sequence"
        )


async def main() -> None:
    app_settings = AppSettings()
    worker_settings = OracleWorkerSettings()

    logging.basicConfig(
        level=app_settings.app_log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logging.getLogger("httpx").setLevel(logging.WARNING)

    # Сначала проверяем настройки, не открывая SQLite.
    if not worker_settings.enabled:
        raise RuntimeError(
            "Запуск Oracle Worker запрещён по умолчанию"
        )

    # Проверяем конфигурацию и allowlist заранее.
    if not worker_settings.mapping_path.is_file():
        raise FileNotFoundError(
            "Файл Oracle mapping не найден"
        )

    build_network_policy(worker_settings)

    database_url = make_url(app_settings.database_url)

    if (
        database_url.drivername != "sqlite"
        or not database_url.database
        or database_url.database == ":memory:"
    ):
        raise ValueError(
            "Oracle Worker требует файловую SQLite"
        )

    if not Path(database_url.database).is_file():
        raise FileNotFoundError(
            "Файл SQLite не найден. "
            "Сначала выполните миграции."
        )

    engine = get_engine()

    try:
        verify_database(engine)

        worker = build_worker(
            engine,
            worker_settings,
        )

        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, worker.stop)

        try:
            logger.info("Oracle Inbox Worker запускается")
            await worker.run()

        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    finally:
        engine.dispose()
        logger.info("Соединения Oracle Worker закрыты")


if __name__ == "__main__":
    asyncio.run(main())
