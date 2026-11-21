
"""A22.5: безопасный DELETE из Oracle Sync.

DELETE отключает ресурс, но сохраняет:
- сам ресурс;
- историю проверок;
- версии конфигурации;
- сопоставление с внешней записью;
- последний source_sequence.

Вызывать только внутри write_transaction() Worker.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Connection

from integrations.tpu.oracle_models import OracleSyncEvent
from services.inbox_worker import InboxClaim
from services.resources import upsert_configuration
from services.source_mapping import lookup_resource_id
from services.source_sequence import check_and_advance_sequence
from shared.schemas import ResourceConfig


class OracleDeleteError(ValueError):
    """Ошибка применения удаления Oracle Sync."""


@dataclass(frozen=True)
class OracleDeleteResult:
    action: Literal[
        "DEACTIVATED",
        "ALREADY_INACTIVE",
        "TOMBSTONED",
        "STALE",
        "DUPLICATE",
    ]
    resource_id: str | None
    config_version_id: int | None


def apply_oracle_delete(
    conn: Connection,
    claim: InboxClaim,
    event: OracleSyncEvent,
    *,
    expected_source: str,
    expected_entity: str,
    now: datetime | None = None,
) -> OracleDeleteResult:
    """Применяет DELETE в транзакции Inbox Worker."""

    if (
        not conn.in_transaction()
        or not conn.info.get("write_mode")
    ):
        raise RuntimeError(
            "Требуется write_transaction()"
        )

    if event.operation_code != 2:
        raise OracleDeleteError(
            "Ожидалась операция DELETE"
        )

    if (
        event.source != expected_source
        or event.entity != expected_entity
    ):
        raise OracleDeleteError(
            "Источник события не соответствует настройкам"
        )

    try:
        stored = OracleSyncEvent.model_validate_json(
            claim.payload_json
        )
    except ValidationError:
        raise OracleDeleteError(
            "Повреждённое событие Inbox"
        ) from None

    if stored != event:
        raise OracleDeleteError(
            "Событие не совпадает с сохранённым Inbox"
        )

    if (
        claim.event_id != str(event.event_id)
        or claim.source_system != event.source
        or claim.source_entity != event.entity
        or claim.source_record_id != event.record_id
        or claim.operation != "DELETE"
        or claim.source_sequence != event.source_sequence
    ):
        raise OracleDeleteError(
            "Поля Inbox и DELETE не совпадают"
        )

    timestamp = now or datetime.now(timezone.utc)

    if timestamp.utcoffset() is None:
        raise ValueError(
            "Требуется timezone-aware datetime"
        )

    # Сначала защищаемся от устаревших событий.
    # Запись sequence откатится вместе с DELETE,
    # если что-либо ниже завершится ошибкой.
    decision = check_and_advance_sequence(
        conn,
        source_system=event.source,
        source_entity=event.entity,
        source_record_id=event.record_id,
        event_id=str(event.event_id),
        source_sequence=event.source_sequence,
        operation="DELETE",
        now=timestamp,
    )

    if decision != "APPLY":
        return OracleDeleteResult(
            action=decision,
            resource_id=None,
            config_version_id=None,
        )

    resource_id = lookup_resource_id(
        conn,
        source_system=event.source,
        source_entity=event.entity,
        source_record_id=event.record_id,
    )

    if resource_id is None:
        # Ресурс ещё не создавался.
        # Но sequence запоминаем, чтобы старый INSERT
        # впоследствии не восстановил удалённую запись.
        return OracleDeleteResult(
            action="TOMBSTONED",
            resource_id=None,
            config_version_id=None,
        )

    row = conn.execute(
        text("""
            SELECT
                r.name,
                r.active,
                c.id AS version_id,
                c.target,
                c.check_type,
                c.port,
                c.interval_seconds,
                c.timeout_seconds,
                c.up_after,
                c.down_after,
                c.heartbeat_interval_seconds
            FROM resources AS r
            JOIN resource_states AS s
              ON s.resource_id = r.id
            JOIN resource_config_versions AS c
              ON c.id = s.config_version_id
            WHERE r.id = :resource_id
              AND c.retired_at IS NULL
        """),
        {"resource_id": resource_id},
    ).mappings().one_or_none()

    if row is None:
        raise OracleDeleteError(
            "Сопоставленный ресурс не имеет активной конфигурации"
        )

    if not bool(row["active"]):
        return OracleDeleteResult(
            action="ALREADY_INACTIVE",
            resource_id=resource_id,
            config_version_id=row["version_id"],
        )

    ranges = conn.execute(
        text("""
            SELECT
                code_start AS start,
                code_end AS end
            FROM http_expected_ranges
            WHERE config_version_id = :version_id
            ORDER BY code_start, code_end
        """),
        {"version_id": row["version_id"]},
    ).mappings().all()

    # Используем существующую логику Никиты,
    # чтобы не обходить версионирование конфигураций.
    disabled_config = ResourceConfig(
        id=resource_id,
        name=row["name"],
        target=row["target"],
        check_type=row["check_type"],
        port=row["port"],
        interval_seconds=row["interval_seconds"],
        timeout_seconds=row["timeout_seconds"],
        expected_http_codes=[
            dict(item) for item in ranges
        ],
        active=False,
        up_after=row["up_after"],
        down_after=row["down_after"],
        heartbeat_interval_seconds=(
            row["heartbeat_interval_seconds"]
        ),
    )

    version_id = upsert_configuration(
        conn,
        disabled_config,
        now=timestamp,
    )

    return OracleDeleteResult(
        action="DEACTIVATED",
        resource_id=resource_id,
        config_version_id=version_id,
    )
