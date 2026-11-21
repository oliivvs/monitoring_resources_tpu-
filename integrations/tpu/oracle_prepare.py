
"""A22.3: подготовка данных Oracle Sync через EntityMapper.

Никаких изменений SQLite здесь нет.
Проверка адреса выполняется до транзакции записи.
"""

from __future__ import annotations

from integrations.tpu.entity_mapper import (
    EntityMapper,
    EntityMappingError,
    MappedResource,
)
from integrations.tpu.oracle_models import OracleSyncEvent
from services.source_sequence import MissingSourceSequence


class OraclePreparationError(ValueError):
    """Некорректные данные для применения события."""


class FullSnapshotRequired(OraclePreparationError):
    """Для UPDATE нужна полная конфигурация ресурса."""


FULL_UPDATE_FIELDS = frozenset({
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


async def prepare_oracle_resource(
    event: OracleSyncEvent,
    mapper: EntityMapper,
) -> MappedResource:
    """Проверяет INSERT/UPDATE и подготавливает ресурс.

    INSERT допускает значения по умолчанию EntityMapper.
    UPDATE требует полный набор параметров, чтобы
    отсутствующие поля не сбрасывали текущие настройки.

    DELETE обрабатывается отдельно, без EntityMapper.
    """

    if event.operation_code == 2:
        raise OraclePreparationError(
            "DELETE обрабатывается отдельным механизмом"
        )

    if event.source != mapper.mapping.source_system:
        raise OraclePreparationError(
            "Источник события не совпадает с mapping"
        )

    if event.entity != mapper.mapping.source_entity:
        raise OraclePreparationError(
            "Сущность события не совпадает с mapping"
        )

    if event.source_sequence is None:
        raise MissingSourceSequence(
            "Для применения требуется source_sequence"
        )

    fields = mapper.mapping.fields
    record = dict(event.data)

    identity_field = fields["source_record_id"]

    if (
        identity_field in record
        and record[identity_field] != event.record_id
    ):
        raise OraclePreparationError(
            "Идентификатор записи не совпадает с событием"
        )

    # Идентичность берём из проверенного поля event.record_id.
    record[identity_field] = event.record_id

    if event.operation_code == 3:
        if not FULL_UPDATE_FIELDS.issubset(fields):
            raise FullSnapshotRequired(
                "Mapping не содержит всех полей UPDATE"
            )

        missing = sorted(
            fields[name]
            for name in FULL_UPDATE_FIELDS
            if fields[name] not in record
        )

        if missing:
            raise FullSnapshotRequired(
                "UPDATE содержит неполную конфигурацию"
            )

    # Повторно используем строгую проверку Никиты.
    try:
        item = mapper.map_record(record)
    except EntityMappingError:
        raise OraclePreparationError(
            "Некорректная конфигурация ресурса"
        ) from None

    if item.source_record_id != event.record_id:
        raise OraclePreparationError(
            "Ошибка сопоставления идентификатора"
        )

    # Проверка allowlist и DNS вне транзакции SQLite.
    await mapper.validate_destination(item)

    return item
