"""Строгая политика разрешённых сетевых проверок.

Разрешение на сетевую проверку задаёт оператор через явный allowlist.
По умолчанию все адреса запрещены. Для локальных тестов loopback включается отдельно.
Проверенные IP передаются в PinnedHTTPTransport и TCP open_connection.
Внешний egress firewall остаётся дополнительным уровнем защиты.
"""
from __future__ import annotations

import asyncio
import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit

from shared.schemas import CheckSnapshot, CheckType


class TargetNotAllowed(ValueError):
    """Цель нарушает сетевую политику; это ошибка конфигурации, не DOWN."""


class ResolutionFailure(Exception):
    """Разрешённое имя не удалось разрешить через DNS."""


@dataclass(frozen=True, slots=True)
class ApprovedTarget:
    host: str
    port: int
    ips: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class NetworkPolicy:
    allowed_hosts: frozenset[str] = frozenset()
    allowed_ports: frozenset[int] = frozenset()
    permit_loopback_for_tests: bool = False
    max_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.max_timeout_seconds <= 0 or self.max_timeout_seconds > 60:
            raise ValueError("Неверный верхний предел таймаута")
        if any(not 1 <= p <= 65535 for p in self.allowed_ports):
            raise ValueError("Разрешённые порты должны быть 1..65535")
        if not self.allowed_hosts or any(
            not isinstance(h, str) or not h or h.strip() != h or "*" in h
            for h in self.allowed_hosts
        ):
            # Пустой список допустим и означает полный запрет.
            if self.allowed_hosts:
                raise ValueError("Только точные имена и IP, без wildcard")


def parse_target(snapshot: CheckSnapshot) -> tuple[str, int]:
    if snapshot.check_type == CheckType.HTTP:
        url = urlsplit(snapshot.target)
        if url.scheme not in {"http", "https"} or not url.hostname:
            raise TargetNotAllowed("Некорректный URL")
        if url.username is not None or url.password is not None or url.fragment:
            raise TargetNotAllowed("Учетные данные и fragment в URL запрещены")
        try:
            port = url.port or (443 if url.scheme == "https" else 80)
        except ValueError as e:
            raise TargetNotAllowed("Некорректный HTTP-порт") from e
        return url.hostname.rstrip('.').casefold(), port
    if snapshot.check_type == CheckType.TCP:
        if snapshot.port is None or "//" in snapshot.target or "/" in snapshot.target:
            raise TargetNotAllowed("Некорректная TCP-цель")
        return snapshot.target.rstrip('.').casefold(), snapshot.port
    raise TargetNotAllowed("Неподдерживаемый тип проверки")


def _allowed_ip(address: str, policy: NetworkPolicy) -> bool:
    ip = ipaddress.ip_address(address.split('%', maxsplit=1)[0])
    # Тестовый loopback разрешён только специальной явной настройкой.
    if ip.is_loopback and policy.permit_loopback_for_tests:
        return True
    return ip.is_global


async def authorize_destination(snapshot: CheckSnapshot, policy: NetworkPolicy) -> ApprovedTarget:
    host, port = parse_target(snapshot)
    if host not in {h.rstrip('.').casefold() for h in policy.allowed_hosts}:
        raise TargetNotAllowed("Адрес отсутствует в списке разрешённых целей")
    if port not in policy.allowed_ports:
        raise TargetNotAllowed("Порт отсутствует в списке разрешённых портов")
    if snapshot.timeout_seconds > policy.max_timeout_seconds:
        raise TargetNotAllowed("Таймаут превышает административный лимит")
    try:
        # Получаем все IPv4/IPv6 адреса, не выбираем произвольно первый.
        addresses = await asyncio.get_running_loop().getaddrinfo(
            host, port, type=socket.SOCK_STREAM
        )
    except (socket.gaierror, UnicodeError, OSError) as exc:
        raise ResolutionFailure("Не удалось разрешить имя ресурса") from exc
    if not addresses:
        raise ResolutionFailure("DNS не вернул адреса ресурса")
    try:
        ips = {item[4][0] for item in addresses}
        validated = all(_allowed_ip(ip, policy) for ip in ips)
    except ValueError as exc:
        raise TargetNotAllowed("Некорректный сетевой адрес") from exc
    if not validated:
        raise TargetNotAllowed("DNS вернул запрещённый сетевой адрес")
    return ApprovedTarget(host=host, port=port, ips=tuple(sorted(ips)))
