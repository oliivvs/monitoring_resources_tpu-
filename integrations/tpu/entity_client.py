
"""Асинхронный read-only клиент для учебного Entity API ТПУ.

Production-маршруты сюда не зашиты.
Их согласуют по документации ТПУ.
"""

from __future__ import annotations

import json
import re
import ssl
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx
from pydantic import SecretStr, ValidationError

from integrations.tpu.snapshot_transport import PinnedSnapshotTransport

from integrations.tpu.entity_models import (
    EntitiesResponse,
    FiltersResponse,
    ParametersResponse,
    RecordsResponse,
    StructureResponse,
)


ENTITY_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class EntityApiError(RuntimeError):
    """Ошибка получения данных без раскрытия токенов."""


class EntityApiHttpError(EntityApiError):
    def __init__(self, status_code: int):
        self.status_code = status_code
        self.retryable = status_code == 429 or status_code >= 500

        super().__init__(
            f"Entity API вернул HTTP {status_code}"
        )


class EntityApiTransportError(EntityApiError):
    """Сетевая ошибка или таймаут."""


class EntityApiSchemaError(EntityApiError):
    """Неверный формат ответа внешнего API."""


@dataclass(frozen=True)
class EntityRoutes:
    """Маршруты API задаются в одном месте."""

    entities: str
    structure: str
    parameters: str
    filters: str
    records: str

    @classmethod
    def local_mock(cls) -> "EntityRoutes":
        prefix = "/mock/tpu/entity/v1/entities"

        return cls(
            entities=prefix,
            structure=prefix + "/{entity_id}/structure",
            parameters=prefix + "/{entity_id}/parameters",
            filters=prefix + "/{entity_id}/filters",
            records=prefix + "/{entity_id}/records",
        )

    def __post_init__(self) -> None:
        for name, path in vars(self).items():
            if (
                not path.startswith("/")
                or path.startswith("//")
                or ".." in path
                or any(
                    char in path
                    for char in ("?", "#", "\\", " ")
                )
            ):
                raise ValueError(
                    f"Недопустимый маршрут {name}"
                )

            expected = 0 if name == "entities" else 1

            if path.count("{entity_id}") != expected:
                raise ValueError(
                    f"Неверный шаблон маршрута {name}"
                )


class TpuEntityClient:
    def __init__(
        self,
        *,
        base_url: str,
        routes: EntityRoutes,
        token: SecretStr | None = None,
        timeout_seconds: float = 10.0,
        http_client: httpx.AsyncClient | None = None,
        max_response_bytes: int = 512 * 1024,
        pinned_allowed_cidrs: tuple[str, ...] | None = None,
    ) -> None:

        url = urlsplit(base_url)

        if url.scheme not in {"http", "https"} or not url.hostname:
            raise ValueError(
                "Неверный базовый URL Entity API"
            )

        if (
            url.username
            or url.password
            or url.query
            or url.fragment
            or url.path not in {"", "/"}
        ):
            raise ValueError(
                "base_url должен содержать только адрес сервера"
            )

        if (
            url.scheme == "http"
            and url.hostname not in {
                "localhost",
                "127.0.0.1",
                "::1",
            }
        ):
            raise ValueError(
                "Внешний Entity API разрешён только через HTTPS"
            )

        if not 0 < timeout_seconds <= 120:
            raise ValueError(
                "timeout_seconds должен быть от 0 до 120"
            )

        if not 1024 <= max_response_bytes <= 10 * 1024 * 1024:
            raise ValueError(
                "Недопустимый лимит размера ответа"
            )

        self._base_url = base_url.rstrip("/")
        self._routes = routes
        self._token = token
        self._timeout = timeout_seconds
        self._http_client = http_client
        self._max_response_bytes = max_response_bytes
        self._pinned_allowed_cidrs = pinned_allowed_cidrs

    @staticmethod
    def _valid_entity_id(entity_id: str) -> str:
        if not ENTITY_ID_PATTERN.fullmatch(entity_id):
            raise ValueError(
                "Некорректный entity_id"
            )

        return entity_id

    def _route(
        self,
        template: str,
        entity_id: str,
    ) -> str:
        return template.replace(
            "{entity_id}",
            self._valid_entity_id(entity_id),
        )

    async def _get_using(
        self,
        client: httpx.AsyncClient,
        path: str,
        *,
        params: dict | None = None,
    ) -> dict:

        headers = {
            "Accept": "application/json"
        }

        if self._token is not None:
            headers["Authorization"] = (
                f"Bearer {self._token.get_secret_value()}"
            )

        try:
            async with client.stream(
                "GET",
                self._base_url + path,
                headers=headers,
                params=params,
                timeout=self._timeout,
                follow_redirects=False,
            ) as response:

                if response.status_code != 200:
                    raise EntityApiHttpError(
                        response.status_code
                    )

                data = bytearray()

                async for part in response.aiter_bytes():
                    if (
                        len(data) + len(part)
                        > self._max_response_bytes
                    ):
                        raise EntityApiSchemaError(
                            "Entity API: ответ слишком большой"
                        )

                    data.extend(part)

        except httpx.RequestError:
            raise EntityApiTransportError(
                "Entity API: сетевая ошибка или таймаут"
            ) from None

        try:
            payload = json.loads(data)

        except (ValueError, UnicodeDecodeError):
            raise EntityApiSchemaError(
                "Entity API: неправильный JSON"
            ) from None

        if not isinstance(payload, dict):
            raise EntityApiSchemaError(
                "Entity API: ожидался JSON-объект"
            )

        return payload

    async def _get(
        self,
        path: str,
        *,
        params: dict | None = None,
    ) -> dict:

        if self._http_client is not None:
            return await self._get_using(
                self._http_client,
                path,
                params=params,
            )

        # Используем системные доверенные CA, но не
        # наследуем HTTP_PROXY/HTTPS_PROXY/ALL_PROXY.
        # ssl.create_default_context() также учитывает
        # стандартные OpenSSL-пути сертификатов.
        context = ssl.create_default_context()
        extra = {}

        if self._pinned_allowed_cidrs is not None:
            extra["transport"] = PinnedSnapshotTransport(
                expected_host=httpx.URL(self._base_url).host,
                allowed_cidrs=self._pinned_allowed_cidrs,
                ssl_context=context,
            )

        async with httpx.AsyncClient(
            timeout=self._timeout,
            follow_redirects=False,
            trust_env=False,
            verify=context,
            **extra,
        ) as client:
            return await self._get_using(
                client,
                path,
                params=params,
            )

    @staticmethod
    def _parse(model: type, payload: dict):
        try:
            return model.model_validate(payload)

        except ValidationError:
            raise EntityApiSchemaError(
                "Entity API: ответ не соответствует схеме"
            ) from None

    async def get_entities(self) -> EntitiesResponse:
        return self._parse(
            EntitiesResponse,
            await self._get(self._routes.entities),
        )

    async def get_entity_structure(
        self,
        entity_id: str,
    ) -> StructureResponse:
        return self._parse(
            StructureResponse,
            await self._get(
                self._route(
                    self._routes.structure,
                    entity_id,
                )
            ),
        )

    async def get_entity_parameters(
        self,
        entity_id: str,
    ) -> ParametersResponse:
        return self._parse(
            ParametersResponse,
            await self._get(
                self._route(
                    self._routes.parameters,
                    entity_id,
                )
            ),
        )

    async def get_entity_filters(
        self,
        entity_id: str,
    ) -> FiltersResponse:
        return self._parse(
            FiltersResponse,
            await self._get(
                self._route(
                    self._routes.filters,
                    entity_id,
                )
            ),
        )

    async def get_entity_records(
        self,
        entity_id: str,
        filters: dict[str, str | bool | int] | None = None,
        cursor: str | None = None,
        *,
        limit: int = 50,
    ) -> RecordsResponse:

        if not 1 <= limit <= 100:
            raise ValueError(
                "limit должен быть от 1 до 100"
            )

        if cursor is not None and (
            len(cursor) > 256
            or not cursor.isascii()
        ):
            raise ValueError(
                "Некорректный cursor"
            )

        params: dict[str, str | bool | int] = {
            "limit": limit
        }

        if cursor is not None:
            params["cursor"] = cursor

        if filters:
            for name in filters:
                if (
                    name in {"limit", "cursor"}
                    or not ENTITY_ID_PATTERN.fullmatch(name)
                ):
                    raise ValueError(
                        "Некорректное имя фильтра"
                    )

            params.update(filters)

        return self._parse(
            RecordsResponse,
            await self._get(
                self._route(
                    self._routes.records,
                    entity_id,
                ),
                params=params,
            ),
        )
