"""HTTPX transport with DNS pinning at the actual TCP connection.

Uses the public HTTPX AsyncBaseTransport and HTTPCore AsyncConnectionPool interfaces.
The client URL, Host header, HTTPS SNI and certificate validation keep the
original hostname, while the socket opens only a previously authorised IP.
No proxy, redirect, or second DNS lookup is performed.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpcore
import httpx


class _PinnedBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, *, hostname: str, port: int, ips: tuple[str, ...]):
        self._hostname = hostname
        self._port = port
        self._ips = ips
        self._delegate = httpcore.AnyIOBackend()

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        if host.rstrip('.').casefold() != self._hostname or port != self._port:
            raise httpcore.ConnectError("Connection outside authorised destination")
        last_error = None
        for ip in self._ips:
            try:
                return await self._delegate.connect_tcp(
                    ip, port, timeout=timeout, local_address=local_address,
                    socket_options=socket_options,
                )
            except httpcore.ConnectError as error:
                last_error = error
        raise httpcore.ConnectError("All authorised addresses unavailable") from last_error

    async def connect_unix_socket(self, path, timeout=None, socket_options=None):
        raise httpcore.ConnectError("Unix domain sockets are disabled")

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class _ResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream):
        self._stream = stream

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self._stream:
                yield chunk
        except (httpcore.NetworkError, httpcore.ProtocolError, httpcore.ProxyError) as exc:
            raise httpx.RequestError("Network read failed") from exc

    async def aclose(self) -> None:
        await self._stream.aclose()


class PinnedHTTPTransport(httpx.AsyncBaseTransport):
    """One connection pool per probe; never reuse IP trust across checks."""

    def __init__(self, *, hostname: str, port: int, ips: tuple[str, ...]):
        import ssl
        self._pool = httpcore.AsyncConnectionPool(
            ssl_context=ssl.create_default_context(),
            network_backend=_PinnedBackend(hostname=hostname, port=port, ips=ips),
            max_connections=1, max_keepalive_connections=0,
            http2=False, retries=0,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        req = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        try:
            resp = await self._pool.handle_async_request(req)
        except httpcore.TimeoutException as exc:
            raise httpx.TimeoutException("Network timeout") from exc
        except (httpcore.NetworkError, httpcore.ProtocolError, httpcore.ProxyError) as exc:
            raise httpx.RequestError("Network request failed") from exc
        return httpx.Response(
            status_code=resp.status,
            headers=resp.headers,
            stream=_ResponseStream(resp.stream),
            extensions=resp.extensions,
        )

    async def aclose(self) -> None:
        await self._pool.aclose()
