
"""A23.1: классификация ошибок Oracle Sync Inbox.

Постоянные ошибки прекращают обработку сразу.
Временные ошибки остаются в механизме retry.

Сетевые проверки выполняются вне SQLite-транзакции.
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
    FullSnapshotRequired,
    OraclePreparationError,
    prepare_oracle_resource,
)

from services.inbox_worker import (
    InboxClaim,
    PermanentInboxError,
)
from services.oracle_apply import (
    OracleApplyError,
    UnknownSourceResource,
    apply_prepared_oracle_resource,
)
from services.oracle_delete import (
    OracleDeleteError,
    apply_oracle_delete,
)
from services.source_mapping import SourceMappingError
from services.source_sequence import (
    MissingSourceSequence,
    SourceSequenceConflict,
)


class OracleInboxError(PermanentInboxError):
    """Постоянная ошибка события Oracle Sync."""


@dataclass(frozen=True)
class PreparedOracleEvent:
    event: OracleSyncEvent
    resource: MappedResource | None


class OracleInboxHandler:
    """Обработчик Oracle Sync для двухэтапного Worker."""

    def __init__(self, mapper: EntityMapper) -> None:
        self._mapper = mapper

    async def prepare(
        self,
        claim: InboxClaim,
    ) -> PreparedOracleEvent:
        """Подготавливает событие без write_transaction."""

        try:
            event = OracleSyncEvent.model_validate_json(
                claim.payload_json
            )

        except (ValidationError, ValueError):
            raise OracleInboxError(
                "Повреждённое событие Inbox"
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
                "Поля события не совпадают с Inbox"
            )

        if (
            event.source != self._mapper.mapping.source_system
            or event.entity != self._mapper.mapping.source_entity
        ):
            raise OracleInboxError(
                "Неподдерживаемый источник или сущность"
            )

        if event.operation_code == 2:
            return PreparedOracleEvent(
                event=event,
                resource=None,
            )

        try:
            resource = await prepare_oracle_resource(
                event,
                self._mapper,
            )

        except FullSnapshotRequired:
            # До A23.2 оставляем возможность retry.
            raise

        except (
            OraclePreparationError,
            MissingSourceSequence,
        ):
            raise OracleInboxError(
                "Некорректные поля события"
            ) from None

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
        """Применяет событие атомарно внутри SQLite."""

        event = prepared.event

        if event.operation_code == 2:
            try:
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

            except (
                OracleDeleteError,
                SourceSequenceConflict,
                MissingSourceSequence,
                SourceMappingError,
            ):
                raise OracleInboxError(
                    "Постоянная ошибка DELETE"
                ) from None

            return

        if prepared.resource is None:
            raise OracleInboxError(
                "Отсутствует подготовленный ресурс"
            )

        try:
            apply_prepared_oracle_resource(
                conn,
                claim,
                event,
                prepared.resource,
                resource_id_prefix=(
                    self._mapper.mapping.resource_id_prefix
                ),
            )

        except UnknownSourceResource:
            # UPDATE мог прийти раньше INSERT.
            # Сохраняем возможность повторной обработки.
            raise

        except (
            OracleApplyError,
            SourceMappingError,
            SourceSequenceConflict,
            MissingSourceSequence,
        ):
            raise OracleInboxError(
                "Постоянная ошибка INSERT/UPDATE"
            ) from None
