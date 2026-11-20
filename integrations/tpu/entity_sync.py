
"""N21: безопасная синхронизация Entity API с SQLite."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from db.session import write_transaction

from integrations.tpu.entity_client import TpuEntityClient
from integrations.tpu.entity_mapper import EntityMapper, MappedResource

from services.config_import import ImportItem, ImportReport
from services.resources import upsert_configuration


class EntitySyncError(ValueError):
    """Ошибка получения или проверки партии данных."""


async def fetch_validated_resources(
    client: TpuEntityClient,
    mapper: EntityMapper,
    *,
    entity_id: str = "servers",
    page_size: int = 50,
    max_pages: int = 20,
    max_records: int = 1000,
) -> tuple[MappedResource, ...]:

    if entity_id != mapper.mapping.source_entity:
        raise EntitySyncError(
            "Сущность не совпадает с конфигурацией mapping"
        )

    if not 1 <= page_size <= 100:
        raise ValueError("page_size должен быть от 1 до 100")

    if not 1 <= max_pages <= 1000:
        raise ValueError("max_pages должен быть от 1 до 1000")

    if not 1 <= max_records <= 10000:
        raise ValueError("max_records должен быть от 1 до 10000")

    found: list[MappedResource] = []

    source_ids: set[str] = set()
    resource_ids: set[str] = set()
    cursors: set[str] = set()

    cursor: str | None = None

    for _ in range(max_pages):

        page = await client.get_entity_records(
            entity_id,
            cursor=cursor,
            limit=page_size,
        )

        mapped_page = mapper.map_records(page.items)

        if len(found) + len(mapped_page) > max_records:
            raise EntitySyncError(
                "Превышено допустимое число записей за синхронизацию"
            )

        for item in mapped_page:

            if (
                item.source_record_id in source_ids
                or item.config.id in resource_ids
            ):
                raise EntitySyncError(
                    "Повтор идентификатора между страницами API"
                )

            source_ids.add(item.source_record_id)
            resource_ids.add(item.config.id)

            # Проверяем адрес перед записью в SQLite.
            await mapper.validate_destination(item)

            found.append(item)

        if page.next_cursor is None:
            return tuple(found)

        if (
            page.next_cursor == cursor
            or page.next_cursor in cursors
        ):
            raise EntitySyncError(
                "Entity API вернул повторяющийся курсор"
            )

        cursors.add(page.next_cursor)
        cursor = page.next_cursor

    raise EntitySyncError(
        "Entity API превысил максимальное число страниц"
    )


def persist_validated_resources(
    engine: Engine,
    items: Sequence[MappedResource],
) -> ImportReport:
    """Сохраняет заранее проверенные ресурсы одной транзакцией."""

    # Вызывать только после fetch_validated_resources().
    # Сырые данные внешнего API сюда передавать нельзя.

    unique_ids = [
        item.config.id
        for item in items
    ]

    if len(unique_ids) != len(set(unique_ids)):
        raise EntitySyncError(
            "Дубли resource_id в партии импорта"
        )

    result: list[ImportItem] = []

    with write_transaction(engine) as conn:

        for item in items:

            config = item.config

            previous_version = conn.execute(
                text("""
                    SELECT s.config_version_id
                    FROM resource_states AS s
                    JOIN resource_config_versions AS c
                      ON c.id = s.config_version_id
                    WHERE s.resource_id = :resource_id
                      AND c.retired_at IS NULL
                """),
                {"resource_id": config.id},
            ).scalar_one_or_none()

            # Используем готовую логику работы с ресурсами.
            version_id = upsert_configuration(
                conn,
                config,
            )

            if previous_version is None:
                action = "CREATED"

            elif previous_version == version_id:
                action = "UNCHANGED"

            else:
                action = "UPDATED"

            result.append(
                ImportItem(
                    config.id,
                    version_id,
                    action,
                )
            )

    return ImportReport(items=tuple(result))


async def sync_entity_resources(
    engine: Engine,
    client: TpuEntityClient,
    mapper: EntityMapper,
    *,
    entity_id: str = "servers",
    page_size: int = 50,
    max_pages: int = 20,
    max_records: int = 1000,
) -> ImportReport:
    """Полный цикл синхронизации Entity API с SQLite."""

    items = await fetch_validated_resources(
        client,
        mapper,
        entity_id=entity_id,
        page_size=page_size,
        max_pages=max_pages,
        max_records=max_records,
    )

    # Операции SQLite выполняем в отдельном потоке,
    # чтобы не блокировать цикл asyncio.
    return await asyncio.to_thread(
        persist_validated_resources,
        engine,
        items,
    )
