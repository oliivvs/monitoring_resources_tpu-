
"""N20: преобразование Entity API-записей в ResourceConfig.

Не выполняет HTTP/TCP-проверки и не пишет в БД.
Имена внешних полей задаются через mapping.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

from checks.security import (
    NetworkPolicy,
    ResolutionFailure,
    TargetNotAllowed,
    authorize_destination,
    parse_target,
)

from shared.schemas import (
    CheckSnapshot,
    CheckType,
    ResourceConfig,
)


_ALLOWED_FIELDS = frozenset({
    "source_record_id",
    "name",
    "target",
    "check_type",
    "active",
    "port",
    "interval_seconds",
    "timeout_seconds",
    "expected_http_codes",
    "up_after",
    "down_after",
    "heartbeat_interval_seconds",
})

_REQUIRED_FIELDS = frozenset({
    "source_record_id",
    "name",
    "target",
    "check_type",
    "active",
})

_OPTIONAL_FIELDS = _ALLOWED_FIELDS - _REQUIRED_FIELDS

_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]+$")
_SAFE_FIELD_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class EntityMappingError(ValueError):
    """Ошибка преобразования внешней записи."""


class MappingConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    source_system: str = Field(min_length=1)
    source_entity: str = Field(min_length=1)
    resource_id_prefix: str = Field(
        min_length=1,
        max_length=100,
    )

    fields: dict[str, str]
    defaults: dict[str, Any] = Field(
        default_factory=dict
    )

    @model_validator(mode="after")
    def check_mapping(self) -> "MappingConfig":

        if not _SAFE_ID.fullmatch(
            self.resource_id_prefix
        ):
            raise ValueError(
                "resource_id_prefix содержит запрещённые символы"
            )

        if not _REQUIRED_FIELDS.issubset(self.fields):
            missing = sorted(
                _REQUIRED_FIELDS - self.fields.keys()
            )
            raise ValueError(
                f"Не определены обязательные поля mapping: {missing}"
            )

        if self.fields.keys() - _ALLOWED_FIELDS:
            raise ValueError(
                "В mapping указаны неподдерживаемые поля"
            )

        if any(
            not _SAFE_FIELD_NAME.fullmatch(value)
            for value in self.fields.values()
        ):
            raise ValueError(
                "Неверное имя поля внешнего источника"
            )

        if len(set(self.fields.values())) != len(self.fields):
            raise ValueError(
                "Два внутренних поля ссылаются на одно внешнее"
            )

        if self.defaults.keys() - _OPTIONAL_FIELDS:
            raise ValueError(
                "defaults могут задавать только необязательные поля"
            )

        if self.defaults.keys() & self.fields.keys():
            raise ValueError(
                "Поле не может одновременно быть в fields и defaults"
            )

        return self


def load_mapping(path: str | Path) -> MappingConfig:
    """Загрузка локального YAML с ограничением размера."""

    with Path(path).open("rb") as source:
        raw = source.read(64 * 1024 + 1)

    if len(raw) > 64 * 1024:
        raise EntityMappingError(
            "Конфигурация mapping слишком большая"
        )

    class NoAliasesSafeLoader(yaml.SafeLoader):

        def compose_node(self, parent, index):
            if self.check_event(yaml.AliasEvent):
                raise EntityMappingError(
                    "YAML aliases запрещены"
                )
            return super().compose_node(parent, index)

        def construct_mapping(self, node, deep=False):
            obj = {}

            for key_node, val_node in node.value:
                key = self.construct_object(
                    key_node,
                    deep=deep,
                )

                if not isinstance(key, str) or key in obj:
                    raise EntityMappingError(
                        "Повторяющийся или нестроковый ключ YAML"
                    )

                obj[key] = self.construct_object(
                    val_node,
                    deep=deep,
                )

            return obj

    try:
        parsed = yaml.load(
            raw.decode("utf-8-sig"),
            Loader=NoAliasesSafeLoader,
        )

        return MappingConfig.model_validate(parsed)

    except (
        UnicodeError,
        yaml.YAMLError,
        ValidationError,
    ):
        raise EntityMappingError(
            "Неверная конфигурация mapping"
        ) from None


@dataclass(frozen=True, slots=True)
class MappedResource:
    source_record_id: str
    config: ResourceConfig
    ignored_fields: tuple[str, ...]


class EntityMapper:

    def __init__(
        self,
        mapping: MappingConfig,
        policy: NetworkPolicy,
    ):
        self.mapping = mapping
        self.policy = policy

    def map_record(
        self,
        record: dict[str, Any],
    ) -> MappedResource:

        if not isinstance(record, dict):
            raise EntityMappingError(
                "Запись источника должна быть объектом"
            )

        external = self.mapping.fields

        if any(
            name not in record
            for name in (
                external[k]
                for k in _REQUIRED_FIELDS
            )
        ):
            raise EntityMappingError(
                "В записи отсутствуют обязательные поля"
            )

        values = dict(self.mapping.defaults)

        for internal, external_name in external.items():
            if external_name in record:
                values[internal] = record[external_name]

        source_id = values.pop("source_record_id")

        if (
            not isinstance(source_id, str)
            or not _SAFE_ID.fullmatch(source_id)
            or len(source_id)
            > 128 - len(self.mapping.resource_id_prefix)
        ):
            raise EntityMappingError(
                "Некорректный source_record_id"
            )

        if not isinstance(values.get("active"), bool):
            raise EntityMappingError(
                "active должен быть boolean"
            )

        if (
            not isinstance(values.get("check_type"), str)
            or values["check_type"] not in {"HTTP", "TCP"}
        ):
            raise EntityMappingError(
                "check_type должен быть HTTP или TCP"
            )

        # Для TCP HTTP-коды не используются.
        # Для HTTP по умолчанию допустимы 200–299.
        values.setdefault(
            "expected_http_codes",
            (
                []
                if values["check_type"] == "TCP"
                else [{"start": 200, "end": 299}]
            ),
        )

        for name in (
            "port",
            "interval_seconds",
            "up_after",
            "down_after",
            "heartbeat_interval_seconds",
        ):
            if (
                name in values
                and values[name] is not None
                and (
                    not isinstance(values[name], int)
                    or isinstance(values[name], bool)
                )
            ):
                raise EntityMappingError(
                    f"{name} должен быть целым числом"
                )

        if (
            "timeout_seconds" in values
            and (
                isinstance(values["timeout_seconds"], bool)
                or not isinstance(
                    values["timeout_seconds"],
                    (int, float),
                )
            )
        ):
            raise EntityMappingError(
                "timeout_seconds должен быть числом"
            )

        try:
            config = ResourceConfig(
                id=self.mapping.resource_id_prefix + source_id,
                **values,
            )
        except ValidationError:
            raise EntityMappingError(
                "Поля записи не соответствуют ResourceConfig"
            ) from None

        # Проверяем синтаксис адреса без сетевого запроса.
        try:
            parse_target(self._snapshot(config))
        except (TargetNotAllowed, ValueError):
            raise EntityMappingError(
                "Неверный синтаксис target или запрещённые URL-компоненты"
            ) from None

        ignored = tuple(
            sorted(
                set(record) - set(external.values())
            )
        )

        return MappedResource(
            source_record_id=source_id,
            config=config,
            ignored_fields=ignored,
        )

    def map_records(
        self,
        records: list[dict[str, Any]],
    ) -> list[MappedResource]:

        seen: set[str] = set()
        mapped: list[MappedResource] = []

        for record in records:
            item = self.map_record(record)

            if item.source_record_id in seen:
                raise EntityMappingError(
                    "Повтор source_record_id в одной партии"
                )

            seen.add(item.source_record_id)
            mapped.append(item)

        return mapped

    @staticmethod
    def _snapshot(
        config: ResourceConfig,
    ) -> CheckSnapshot:

        return CheckSnapshot(
            resource_id=config.id,
            config_version_id=1,
            target=config.target,
            check_type=CheckType(config.check_type),
            port=config.port,
            interval_seconds=config.interval_seconds,
            timeout_seconds=config.timeout_seconds,
            expected_http_codes=tuple(
                config.expected_http_codes
            ),
        )

    async def validate_destination(
        self,
        item: MappedResource,
    ) -> None:
        """Проверка allowlist и DNS перед импортом."""

        try:
            await authorize_destination(
                self._snapshot(item.config),
                self.policy,
            )

        except (
            ResolutionFailure,
            TargetNotAllowed,
        ):
            raise EntityMappingError(
                "Цель запрещена сетевой политикой или недоступна через DNS"
            ) from None
