"""A23.2.1: получение полного снимка Oracle Sync.

Используется существующий read-only TpuEntityClient.
SQLite не изменяется, сетевые запросы выполняются
вне транзакции записи.

Учебный контракт:
- поиск по source_record_id;
- одна запись в результате;
- source_sequence совпадает с событием;
- присутствуют все поля конфигурации.
"""

from __future__ import annotations

from typing import Any

from integrations.tpu.entity_client import TpuEntityClient
from integrations.tpu.entity_mapper import MappingConfig
from integrations.tpu.oracle_models import OracleSyncEvent
from integrations.tpu.oracle_prepare import FULL_UPDATE_FIELDS


class OracleSnapshotError(Exception):
    """Базовая ошибка получения снимка."""


class OracleSnapshotNotReady(OracleSnapshotError):
    """Снимок ещё недоступен или версия не совпадает."""


class OracleSnapshotContractError(OracleSnapshotError):
    """Ответ не соответствует контракту."""


async def fetch_full_oracle_snapshot(
    client: TpuEntityClient,
    event: OracleSyncEvent,
    mapping: MappingConfig,
) -> dict[str, Any]:
    """Получает полный снимок для частичного UPDATE."""

    if event.operation_code != 3:
        raise OracleSnapshotContractError(
            "Получение снимка разрешено только для UPDATE"
        )

    if (
        event.source != mapping.source_system
        or event.entity != mapping.source_entity
    ):
        raise OracleSnapshotContractError(
            "Источник события не совпадает с mapping"
        )

    if event.source_sequence is None:
        raise OracleSnapshotContractError(
            "Отсутствует source_sequence события"
        )

    if not FULL_UPDATE_FIELDS.issubset(mapping.fields):
        raise OracleSnapshotContractError(
            "Mapping не содержит полей полной конфигурации"
        )

    identity_field = mapping.fields["source_record_id"]

    response = await client.get_entity_records(
        event.entity,
        filters={
            identity_field: event.record_id,
        },
        limit=2,
    )

    if response.next_cursor is not None:
        raise OracleSnapshotNotReady(
            "Невозможно подтвердить единственность записи"
        )

    if not response.items:
        raise OracleSnapshotNotReady(
            "Запись ещё недоступна в Entity API"
        )

    if len(response.items) != 1:
        raise OracleSnapshotContractError(
            "Entity API вернул несколько записей"
        )

    record = dict(response.items[0])

    if record.get(identity_field) != event.record_id:
        raise OracleSnapshotContractError(
            "Идентификатор записи не совпадает"
        )

    snapshot_sequence = record.get("source_sequence")

    if (
        type(snapshot_sequence) is not int
        or snapshot_sequence < 0
    ):
        raise OracleSnapshotContractError(
            "Отсутствует корректная версия снимка"
        )

    if snapshot_sequence != event.source_sequence:
        raise OracleSnapshotNotReady(
            "Версия снимка не совпадает с событием"
        )

    required_fields = {
        mapping.fields[name]
        for name in FULL_UPDATE_FIELDS
    }

    missing = required_fields - record.keys()

    if missing:
        raise OracleSnapshotContractError(
            "Entity API вернул неполную конфигурацию"
        )

    return record
