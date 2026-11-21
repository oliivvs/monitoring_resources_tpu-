
"""A22.4: атомарное применение INSERT/UPDATE Oracle Sync.

На вход поступает уже проверенный MappedResource.
Проверка DNS и allowlist должна быть выполнена ДО вызова.

Функция не открывает собственную транзакцию:
она работает внутри write_transaction() Inbox Worker.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Connection

from integrations.tpu.entity_mapper import MappedResource
from integrations.tpu.oracle_models import (
    OPERATION_NAMES,
    OracleSyncEvent,
)
from services.inbox_worker import InboxClaim
from services.resources import upsert_configuration
from services.source_mapping import (
    SourceMappingConflict,
    link_existing_resource,
    lookup_resource_id,
)
from services.source_sequence import (
    check_and_advance_sequence,
)


class OracleApplyError(ValueError):
    """Ошибка применения события к ресурсу."""


class UnknownSourceResource(OracleApplyError):
    """UPDATE пришёл раньше создания сопоставления."""


class ExistingResourceCollision(OracleApplyError):
    """Источник пытается занять существующий resource_id."""


@dataclass(frozen=True)
class OracleApplyResult:
    action: Literal[
        "CREATED",
        "UPDATED",
        "UNCHANGED",
        "STALE",
        "DUPLICATE",
    ]
    resource_id: str
    config_version_id: int | None


def apply_prepared_oracle_resource(
    conn: Connection,
    claim: InboxClaim,
    event: OracleSyncEvent,
    prepared: MappedResource,
    *,
    resource_id_prefix: str,
    now: datetime | None = None,
) -> OracleApplyResult:
    """Применяет INSERT/UPDATE в общей транзакции Worker.

    Контроль sequence, изменение ресурса и сопоставление
    откатываются вместе при любой ошибке.
    """

    if (
        not conn.in_transaction()
        or not conn.info.get("write_mode")
    ):
        raise RuntimeError(
            "Нужна активная write_transaction()"
        )

    if event.operation_code not in (1, 3):
        raise OracleApplyError(
            "В A22.4 поддержаны только INSERT и UPDATE"
        )

    try:
        stored = OracleSyncEvent.model_validate_json(
            claim.payload_json
        )
    except ValidationError:
        raise OracleApplyError(
            "Сохранённое событие повреждено"
        ) from None

    # Не позволяем применить подготовленную конфигурацию
    # к другому событию, случайно взятому из очереди.
    if stored != event:
        raise OracleApplyError(
            "Подготовленное событие не совпадает с Inbox"
        )

    if (
        claim.event_id != str(event.event_id)
        or claim.source_system != event.source
        or claim.source_entity != event.entity
        or claim.source_record_id != event.record_id
        or claim.source_sequence != event.source_sequence
        or claim.operation != OPERATION_NAMES[event.operation_code]
    ):
        raise OracleApplyError(
            "Поля события и Inbox не совпадают"
        )

    expected_resource_id = (
        resource_id_prefix + event.record_id
    )

    if (
        prepared.source_record_id != event.record_id
        or prepared.config.id != expected_resource_id
    ):
        raise OracleApplyError(
            "Идентификатор подготовленного ресурса неверен"
        )

    timestamp = now or datetime.now(timezone.utc)

    if timestamp.utcoffset() is None:
        raise ValueError(
            "Требуется timezone-aware datetime"
        )

    decision = check_and_advance_sequence(
        conn,
        source_system=event.source,
        source_entity=event.entity,
        source_record_id=event.record_id,
        event_id=str(event.event_id),
        source_sequence=event.source_sequence,
        operation=OPERATION_NAMES[event.operation_code],
        now=timestamp,
    )

    if decision != "APPLY":
        return OracleApplyResult(
            action=decision,
            resource_id=prepared.config.id,
            config_version_id=None,
        )

    mapped_id = lookup_resource_id(
        conn,
        source_system=event.source,
        source_entity=event.entity,
        source_record_id=event.record_id,
    )

    resource_id = prepared.config.id

    if mapped_id is None:
        # UPDATE не должен создавать неизвестный ресурс.
        if event.operation_code == 3:
            raise UnknownSourceResource(
                "UPDATE получен до INSERT"
            )

        # Не разрешаем Oracle Sync захватывать ресурс,
        # который был создан вручную или другим источником.
        occupied = conn.execute(
            text("""
                SELECT 1
                FROM resources
                WHERE id=:resource_id
            """),
            {"resource_id": resource_id},
        ).scalar_one_or_none()

        if occupied is not None:
            raise ExistingResourceCollision(
                "resource_id уже существует без сопоставления"
            )

        version_id = upsert_configuration(
            conn,
            prepared.config,
            now=timestamp,
        )

        link_existing_resource(
            conn,
            source_system=event.source,
            source_entity=event.entity,
            source_record_id=event.record_id,
            resource_id=resource_id,
            now=timestamp,
        )

        return OracleApplyResult(
            action="CREATED",
            resource_id=resource_id,
            config_version_id=version_id,
        )

    if mapped_id != resource_id:
        raise SourceMappingConflict(
            "Сопоставление указывает на другой resource_id"
        )

    before = conn.execute(
        text("""
            SELECT
                r.name,
                r.active,
                s.config_version_id
            FROM resources AS r
            JOIN resource_states AS s
              ON s.resource_id = r.id
            WHERE r.id=:resource_id
        """),
        {"resource_id": resource_id},
    ).mappings().one_or_none()

    if before is None:
        raise OracleApplyError(
            "Сопоставленный ресурс не имеет состояния"
        )

    version_id = upsert_configuration(
        conn,
        prepared.config,
        now=timestamp,
    )

    changed = (
        before["name"] != prepared.config.name
        or bool(before["active"]) != prepared.config.active
        or before["config_version_id"] != version_id
    )

    return OracleApplyResult(
        action="UPDATED" if changed else "UNCHANGED",
        resource_id=resource_id,
        config_version_id=version_id,
    )
