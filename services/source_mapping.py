
"""A22.2: сопоставление записей Oracle Sync и ресурсов ТПУ.

Все изменения выполняются внутри write_transaction().
Ресурс должен быть предварительно создан и проверен.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

from shared.schemas import to_utc_iso


class SourceMappingError(ValueError):
    """Ошибка сопоставления внешнего ресурса."""


class SourceMappingConflict(SourceMappingError):
    """Конфликт двух разных сопоставлений."""


class SourceMappingAmbiguous(SourceMappingError):
    """Одна внешняя запись связана с несколькими ресурсами."""


class SourceResourceMissing(SourceMappingError):
    """Внутренний ресурс не существует."""


def _validate_key(
    source_system: str,
    source_entity: str,
    source_record_id: str,
) -> dict[str, str]:

    fields = {
        "source": (source_system, 128),
        "entity": (source_entity, 128),
        "record_id": (source_record_id, 255),
    }

    result = {}

    for name, (value, maximum) in fields.items():
        if (
            not isinstance(value, str)
            or not value
            or value != value.strip()
            or len(value) > maximum
        ):
            raise ValueError(
                f"Некорректный идентификатор: {name}"
            )

        result[name] = value

    return result


def lookup_resource_id(
    conn: Connection,
    *,
    source_system: str,
    source_entity: str,
    source_record_id: str,
) -> str | None:
    """Возвращает внутренний resource_id или None.

    Если найдены несколько соответствий, вызывается ошибка.
    Выбирать произвольный ресурс запрещено.
    """

    params = _validate_key(
        source_system,
        source_entity,
        source_record_id,
    )

    rows = conn.execute(
        text("""
            SELECT resource_id
            FROM resource_source_mappings
            WHERE source_system=:source
              AND source_entity=:entity
              AND source_record_id=:record_id
            ORDER BY resource_id
            LIMIT 2
        """),
        params,
    ).scalars().all()

    if len(rows) > 1:
        raise SourceMappingAmbiguous(
            "Внешняя запись имеет несколько сопоставлений"
        )

    return rows[0] if rows else None


def link_existing_resource(
    conn: Connection,
    *,
    source_system: str,
    source_entity: str,
    source_record_id: str,
    resource_id: str,
    now: datetime | None = None,
) -> Literal["CREATED", "UNCHANGED"]:
    """Привязывает внешнюю запись к существующему ресурсу.

    Повтор одинаковой связи безопасен.
    Конфликтующие связи запрещены.

    Вставка выполняется в общей транзакции с созданием
    ресурса и обновлением source_sequence.
    """

    if (
        not conn.in_transaction()
        or not conn.info.get("write_mode")
    ):
        raise RuntimeError(
            "Требуется write_transaction()"
        )

    params = _validate_key(
        source_system,
        source_entity,
        source_record_id,
    )

    if (
        not isinstance(resource_id, str)
        or not resource_id
        or resource_id != resource_id.strip()
        or len(resource_id) > 128
    ):
        raise ValueError("Некорректный resource_id")

    existing = lookup_resource_id(
        conn,
        source_system=source_system,
        source_entity=source_entity,
        source_record_id=source_record_id,
    )

    if existing is not None:
        if existing == resource_id:
            return "UNCHANGED"

        raise SourceMappingConflict(
            "Внешняя запись уже связана с другим ресурсом"
        )

    # Защищаемся от ситуации, когда два независимых
    # внешних идентификатора пытаются управлять
    # одним внутренним ресурсом.
    already_linked = conn.execute(
        text("""
            SELECT 1
            FROM resource_source_mappings
            WHERE resource_id=:resource_id
            LIMIT 1
        """),
        {"resource_id": resource_id},
    ).scalar_one_or_none()

    if already_linked is not None:
        raise SourceMappingConflict(
            "Ресурс уже связан с другой внешней записью"
        )

    exists = conn.execute(
        text("""
            SELECT 1
            FROM resources
            WHERE id=:resource_id
        """),
        {"resource_id": resource_id},
    ).scalar_one_or_none()

    if exists is None:
        raise SourceResourceMissing(
            "Внутренний ресурс не найден"
        )

    timestamp = now or datetime.now(timezone.utc)

    if timestamp.utcoffset() is None:
        raise ValueError(
            "Требуется timezone-aware datetime"
        )

    now_s = to_utc_iso(timestamp)

    conn.execute(
        text("""
            INSERT INTO resource_source_mappings (
                source_system,
                source_entity,
                source_record_id,
                resource_id,
                created_at,
                updated_at
            ) VALUES (
                :source,
                :entity,
                :record_id,
                :resource_id,
                :created_at,
                :updated_at
            )
        """),
        {
            **params,
            "resource_id": resource_id,
            "created_at": now_s,
            "updated_at": now_s,
        },
    )

    return "CREATED"
