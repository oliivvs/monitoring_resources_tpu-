"""Безопасное DNS-разрешение для Oracle Snapshot API."""

from __future__ import annotations

import asyncio
import socket
import ssl

from ipaddress import IPv6Address, ip_address, ip_network
from typing import Iterable

import httpx


class PinnedSnapshotTransport(httpx.AsyncBaseTransport):
    """Проверяет DNS и подключается к разрешённому IP."""

    def __init__(
        self,
        *,
        expected_host: str,
        allowed_cidrs: Iterable[str] = (),
        ssl_context: ssl.SSLContext | None = None,
        inner: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._expected_host = expected_host.lower()

        self._networks = tuple(
            ip_network(value, strict=True)
            for value in allowed_cidrs
        )

        self._inner = (
            inner
            if inner is not None
            else httpx.AsyncHTTPTransport(
                verify=ssl_context or ssl.create_default_context(),
                trust_env=False,
                retries=0,
            )
        )

    async def _resolve(
        self,
        host: str,
        port: int,
    ) -> list[str]:
        answers = await asyncio.to_thread(
            socket.getaddrinfo,
            host,
            port,
            type=socket.SOCK_STREAM,
        )

        return [item[4][0] for item in answers]

    def _allowed(self, address: str) -> bool:
        ip = ip_address(address)

        if isinstance(ip, IPv6Address) and ip.ipv4_mapped:
            ip = ip.ipv4_mapped

        if (
            ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_unspecified
            or ip.is_reserved
        ):
            return False

        return (
            ip.is_global
            or any(ip in net for net in self._networks)
        )

    async def handle_async_request(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        hostname = request.url.host

        if (
            request.url.scheme != "https"
            or hostname.lower() != self._expected_host
            or (request.url.port or 443) != 443
        ):
            raise httpx.ConnectError(
                "Snapshot API: неожиданное назначение",
                request=request,
            )

        try:
            addresses = await self._resolve(
                hostname,
                443,
            )

            # Если DNS содержит хотя бы один запрещённый
            # адрес, отклоняем весь результат.
            if not addresses or any(
                not self._allowed(address)
                for address in addresses
            ):
                raise ValueError(
                    "DNS содержит запрещённый адрес"
                )

            selected = ip_address(addresses[0])

            if (
                isinstance(selected, IPv6Address)
                and selected.ipv4_mapped
            ):
                selected = selected.ipv4_mapped

        except (OSError, ValueError):
            raise httpx.ConnectError(
                "Snapshot API: DNS недоступен или адрес запрещён",
                request=request,
            ) from None

        # URL направлен на проверенный IP.
        # Host и SNI сохраняют первоначальное DNS-имя.
        pinned_request = httpx.Request(
            request.method,
            request.url.copy_with(host=str(selected)),
            headers=request.headers,
            stream=request.stream,
            extensions={
                **request.extensions,
                "sni_hostname": hostname,
            },
        )

        return await self._inner.handle_async_request(
            pinned_request
        )

    async def aclose(self) -> None:
        await self._inner.aclose()
