
"""Контроль порядка событий Oracle Sync.

Функция вызывается внутри write_transaction().

Важно: проверка последовательности и изменение ресурса
должны выполняться в ОДНОЙ транзакции SQLite.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

from shared.schemas import to_utc_iso


SequenceDecision = Literal[
    "APPLY",
    "STALE",
    "DUPLICATE",
]


class SourceSequenceConflict(ValueError):
    """Один sequence принадлежит разным событиям."""


class MissingSourceSequence(ValueError):
    """Невозможно гарантировать порядок событий."""


def check_and_advance_sequence(
    conn: Connection,
    *,
    source_system: str,
    source_entity: str,
    source_record_id: str,
    event_id: str,
    source_sequence: int | None,
    operation: str,
    now: datetime | None = None,
) -> SequenceDecision:
    """Регистрирует новую версию внешней записи.

    APPLY: событие новое, вызывающий код должен
           применить изменение ресурса.

    STALE: пришло устаревшее событие.
    DUPLICATE: тот же event_id и sequence.

    Вызывать до изменения ресурса, но в той же
    транзакции. При ошибке транзакция откатит всё.
    """

    if (
        not conn.in_transaction()
        or not conn.info.get("write_mode")
    ):
        raise RuntimeError(
            "Требуется write_transaction()"
        )

    if (
        type(source_sequence) is not int
        or source_sequence < 0
    ):
        raise MissingSourceSequence(
            "Требуется корректный source_sequence"
        )

    for value in (
        source_system,
        source_entity,
        source_record_id,
        event_id,
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                "Идентификаторы не могут быть пустыми"
            )

    if operation not in (
        "INSERT",
        "UPDATE",
        "DELETE",
    ):
        raise ValueError(
            "Неподдерживаемая операция"
        )

    params = {
        "source": source_system,
        "entity": source_entity,
        "record_id": source_record_id,
    }

    previous = conn.execute(
        text("""
            SELECT last_sequence,
                   last_event_id,
                   last_operation
            FROM resource_source_sequences
            WHERE source_system=:source
              AND source_entity=:entity
              AND source_record_id=:record_id
        """),
        params,
    ).mappings().first()

    if previous is not None:
        old_sequence = previous["last_sequence"]

        if source_sequence < old_sequence:
            return "STALE"

        if source_sequence == old_sequence:
            if (
                previous["last_event_id"] == event_id
                and previous["last_operation"] == operation
            ):
                return "DUPLICATE"

            raise SourceSequenceConflict(
                "Разные события имеют одинаковый sequence"
            )

    timestamp = now or datetime.now(timezone.utc)

    if timestamp.utcoffset() is None:
        raise ValueError(
            "Требуется timezone-aware datetime"
        )

    values = {
        **params,
        "sequence": source_sequence,
        "event_id": event_id,
        "operation": operation,
        "updated_at": to_utc_iso(timestamp),
    }

    if previous is None:
        conn.execute(
            text("""
                INSERT INTO resource_source_sequences (
                    source_system,
                    source_entity,
                    source_record_id,
                    last_sequence,
                    last_event_id,
                    last_operation,
                    updated_at
                )
                VALUES (
                    :source,
                    :entity,
                    :record_id,
                    :sequence,
                    :event_id,
                    :operation,
                    :updated_at
                )
            """),
            values,
        )
    else:
        conn.execute(
            text("""
                UPDATE resource_source_sequences
                SET last_sequence=:sequence,
                    last_event_id=:event_id,
                    last_operation=:operation,
                    updated_at=:updated_at
                WHERE source_system=:source
                  AND source_entity=:entity
                  AND source_record_id=:record_id
            """),
            values,
        )

    return "APPLY"
