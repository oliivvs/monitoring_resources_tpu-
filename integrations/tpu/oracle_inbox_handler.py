"""A23.2.2: обработка Oracle Sync с поддержкой частичных UPDATE.

prepare():
- проверяет событие;
- при частичном UPDATE получает полный снимок;
- проверяет снимок через EntityMapper;
- не выполняет запись в SQLite.

apply():
- атомарно применяет подготовленную конфигурацию.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass

from pydantic import ValidationError
from sqlalchemy.engine import Connection

from integrations.tpu.entity_client import (
    EntityApiHttpError,
    EntityApiSchemaError,
    TpuEntityClient,
)
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
from integrations.tpu.oracle_snapshot import (
    OracleSnapshotContractError,
    fetch_full_oracle_snapshot,
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
    """Адаптер Oracle Sync к двухэтапному Inbox Worker."""

    def __init__(
        self,
        mapper: EntityMapper,
        *,
        snapshot_client: TpuEntityClient | None = None,
        prepare_timeout_seconds: float | None = None,
    ) -> None:
        if prepare_timeout_seconds is not None:
            if (
                isinstance(prepare_timeout_seconds, bool)
                or not isinstance(prepare_timeout_seconds, (int, float))
                or not math.isfinite(prepare_timeout_seconds)
                or not 0 < prepare_timeout_seconds <= 120
            ):
                raise ValueError("Некорректный таймаут подготовки")

        self._mapper = mapper
        self._snapshot_client = snapshot_client
        self._prepare_timeout_seconds = prepare_timeout_seconds

    async def _prepare_partial_update(
        self,
        event: OracleSyncEvent,
    ) -> MappedResource:
        """Получает и проверяет полный снимок UPDATE."""

        if self._snapshot_client is None:
            # Старое поведение: пока нет Entity API,
            # событие остаётся доступным для retry.
            raise FullSnapshotRequired(
                "Получение полного снимка не настроено"
            )

        try:
            snapshot = await fetch_full_oracle_snapshot(
                self._snapshot_client,
                event,
                self._mapper.mapping,
            )

        except OracleSnapshotContractError:
            raise OracleInboxError(
                "Неверный контракт полного снимка"
            ) from None

        except EntityApiHttpError as exc:
            if not exc.retryable:
                raise OracleInboxError(
                    "Entity API отклонил запрос"
                ) from None

            # HTTP 429 и 5xx: повторная попытка.
            raise

        except EntityApiSchemaError:
            raise OracleInboxError(
                "Entity API вернул неверную схему"
            ) from None

        # Не позволяем API подменить значения полей,
        # уже присутствующих в уведомлении.
        # Неизвестные поля уведомления не сравниваем:
        # они могут содержать служебные метаданные.
        known_fields = set(
            self._mapper.mapping.fields.values()
        )

        for name, value in event.data.items():
            if (
                name in known_fields
                and snapshot.get(name) != value
            ):
                raise OracleInboxError(
                    "Данные уведомления и снимка не совпадают"
                )

        # Подменяем data только у временного экземпляра,
        # используемого для валидации и подготовки.
        # Исходный event и сохранённый JSON не изменяются.
        snapshot_event = event.model_copy(
            update={"data": snapshot}
        )

        try:
            return await prepare_oracle_resource(
                snapshot_event,
                self._mapper,
            )

        except (
            OraclePreparationError,
            MissingSourceSequence,
        ):
            raise OracleInboxError(
                "Полный снимок содержит некорректные поля"
            ) from None

    async def prepare(
        self,
        claim: InboxClaim,
    ) -> PreparedOracleEvent:
        """Подготавливает событие с ограничением времени."""

        if self._prepare_timeout_seconds is None:
            return await self._prepare_unbounded(claim)

        async with asyncio.timeout(self._prepare_timeout_seconds):
            return await self._prepare_unbounded(claim)

    async def _prepare_unbounded(
        self,
        claim: InboxClaim,
    ) -> PreparedOracleEvent:
        """Проверяет событие вне транзакции SQLite."""

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
            if event.operation_code != 3:
                raise

            resource = await self._prepare_partial_update(
                event
            )

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
        """Атомарно применяет событие внутри SQLite."""

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
