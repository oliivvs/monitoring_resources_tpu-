
"""DTO учебного Entity API ТПУ. Реальные поля могут отличаться."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EntityDto(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EntityInfo(EntityDto):
    entity_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=255)


class EntitiesResponse(EntityDto):
    items: list[EntityInfo]


class EntityField(EntityDto):
    name: str = Field(min_length=1)
    type: str = Field(min_length=1)
    required: bool


class StructureResponse(EntityDto):
    entity_id: str
    fields: list[EntityField]


class ParametersResponse(EntityDto):
    entity_id: str
    parameters: list[EntityField]


class FiltersResponse(EntityDto):
    entity_id: str
    filters: list[EntityField]


class RecordsResponse(EntityDto):
    items: list[dict[str, Any]]
    next_cursor: str | None = Field(default=None, max_length=256)

    @field_validator("items")
    @classmethod
    def validate_items(
        cls,
        items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if len(items) > 100 or any(not record for record in items):
            raise ValueError(
                "Некорректная страница записей Entity API"
            )

        return items
