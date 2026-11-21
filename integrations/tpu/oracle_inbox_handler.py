
"""A22.6: соединение Oracle Sync с Inbox Worker.

prepare() работает вне SQLite write_transaction.
apply() работает внутри write_transaction.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import ValidationError
from sqlalchemy.engine import Connection

from integrations.tpu.entity_mapper import (
    EntityMapper,
    MappedResource,
)
from integrations.tpu.oracle_models import (
    OPERATION_NAMES,
    OracleSyncEvent,
)
from integrations.tpu.oracle_prepare import (
    prepare_oracle_resource,
)

from services.inbox_worker import InboxClaim
from services.oracle_apply import apply_prepared_oracle_resource
from services.oracle_delete import apply_oracle_delete


class OracleInboxError(ValueError):
    """Некорректное событие Oracle Sync."""


@dataclass(frozen=True)
class PreparedOracleEvent:
    event: OracleSyncEvent
    resource: MappedResource | None


class OracleInboxHandler:
    """Адаптер бизнес-логики к Inbox Worker."""

    def __init__(self, mapper: EntityMapper) -> None:
        self._mapper = mapper

    async def prepare(
        self,
        claim: InboxClaim,
    ) -> PreparedOracleEvent:
        """Проверяет событие без транзакции записи."""

        try:
            event = OracleSyncEvent.model_validate_json(
                claim.payload_json
            )
        except (ValidationError, ValueError):
            raise OracleInboxError(
                "Событие Inbox не соответствует контракту"
            ) from None

        if (
            claim.event_id != str(event.event_id)
            or claim.source_system != event.source
            or claim.source_entity != event.entity
            or claim.source_record_id != event.record_id
            or claim.source_sequence != event.source_sequence
            or claim.operation != (
                OPERATION_NAMES[event.operation_code]
            )
        ):
            raise OracleInboxError(
                "Данные Inbox не совпадают с событием"
            )

        if (
            event.source != self._mapper.mapping.source_system
            or event.entity != self._mapper.mapping.source_entity
        ):
            raise OracleInboxError(
                "Источник или сущность не поддерживается"
            )

        if event.operation_code == 2:
            # DELETE не требует DNS или получения target.
            return PreparedOracleEvent(
                event=event,
                resource=None,
            )

        # INSERT / UPDATE:
        # проверка mapping, адреса, allowlist и DNS.
        resource = await prepare_oracle_resource(
            event,
            self._mapper,
        )

        return PreparedOracleEvent(
            event=event,
            resource=resource,
        )

    def apply(
        self,
        conn: Connection,
        claim: InboxClaim,
        prepared: PreparedOracleEvent,
    ) -> None:
        """Применяет изменения только внутри SQLite."""

        event = prepared.event

        if event.operation_code == 2:
            apply_oracle_delete(
                conn,
                claim,
                event,
                expected_source=(
                    self._mapper.mapping.source_system
                ),
                expected_entity=(
                    self._mapper.mapping.source_entity
                ),
            )
            return

        if prepared.resource is None:
            raise OracleInboxError(
                "Не подготовлена конфигурация ресурса"
            )

        apply_prepared_oracle_resource(
            conn,
            claim,
            event,
            prepared.resource,
            resource_id_prefix=(
                self._mapper.mapping.resource_id_prefix
            ),
        )
