
"""N10: атомарный импорт ресурсов из JSON/YAML в общую SQLite."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from sqlalchemy import text
from sqlalchemy.engine import Engine

from checks.config_loader import load_resources
from db.session import write_transaction
from services.resources import upsert_configuration


@dataclass(frozen=True, slots=True)
class ImportItem:
    resource_id: str
    version_id: int
    action: Literal["CREATED", "UPDATED", "UNCHANGED"]


@dataclass(frozen=True, slots=True)
class ImportReport:
    items: tuple[ImportItem, ...]

    @property
    def total(self) -> int:
        return len(self.items)

    def count(
        self,
        action: Literal["CREATED", "UPDATED", "UNCHANGED"],
    ) -> int:
        return sum(item.action == action for item in self.items)


def import_resources_file(
    engine: Engine,
    path: str | Path,
) -> ImportReport:
    """Импортируем весь файл в одной транзакции.

    Если один ресурс содержит ошибку, откатывается вся партия.
    Сетевые HTTP/TCP-запросы здесь не выполняются.
    """
    # Сначала полностью проверяем файл, не изменяя SQLite.
    resource_file = load_resources(path)
    imported: list[ImportItem] = []

    with write_transaction(engine) as conn:
        for resource in resource_file.resources:
            # Запоминаем активную версию до изменения ресурса.
            previous_version = conn.execute(
                text("""
                    SELECT s.config_version_id
                    FROM resource_states AS s
                    JOIN resource_config_versions AS c
                      ON c.id = s.config_version_id
                    WHERE s.resource_id = :resource_id
                      AND c.retired_at IS NULL
                """),
                {"resource_id": resource.id},
            ).scalar_one_or_none()

            # Повторно используем общую логику версионирования v2.1.
            version_id = upsert_configuration(conn, resource)

            if previous_version is None:
                action = "CREATED"
            elif previous_version == version_id:
                action = "UNCHANGED"
            else:
                action = "UPDATED"

            imported.append(
                ImportItem(resource.id, version_id, action)
            )

    return ImportReport(items=tuple(imported))
