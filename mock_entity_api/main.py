
"""Локальная имитация Entity API ТПУ."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel


app = FastAPI(
    title="Mock Entity API ТПУ",
    description="Тестовый сервер для разработки адаптера мониторинга",
    version="1.0.0",
)

PREFIX = "/mock/tpu/entity/v1"
ENTITY_ID = "servers"


# Модели ответов API

class EntityInfo(BaseModel):
    entity_id: str
    name: str


class EntitiesResponse(BaseModel):
    items: list[EntityInfo]


class FieldInfo(BaseModel):
    name: str
    type: str
    required: bool


class StructureResponse(BaseModel):
    entity_id: str
    fields: list[FieldInfo]


class ParametersResponse(BaseModel):
    entity_id: str
    parameters: list[FieldInfo]


class FiltersResponse(BaseModel):
    entity_id: str
    filters: list[FieldInfo]


class RecordsResponse(BaseModel):
    items: list[dict[str, Any]]
    next_cursor: str | None = None


# Тестовые данные

TEST_RECORDS = [
    {
        "source_record_id": "tpu-demo-001",
        "name": "Демонстрационный HTTP-сервис",
        "target": "http://127.0.0.1:18080/health",
        "check_type": "HTTP",
        "active": True,
    },
    {
        "source_record_id": "tpu-demo-002",
        "name": "Резервный тестовый HTTP-сервис",
        "target": "http://127.0.0.1:18080/health",
        "check_type": "HTTP",
        "active": False,
    },
    {
        "source_record_id": "tpu-demo-003",
        "name": "Дополнительный тестовый HTTP-сервис",
        "target": "http://127.0.0.1:18080/health",
        "check_type": "HTTP",
        "active": True,
    },
]


def require_entity(entity_id: str) -> None:
    """Проверяет существование сущности."""
    if entity_id != ENTITY_ID:
        raise HTTPException(
            status_code=404,
            detail="Сущность не найдена",
        )


# 1. Список сущностей

@app.get(
    f"{PREFIX}/entities",
    response_model=EntitiesResponse,
    tags=["Entity API"],
)
def get_entities() -> EntitiesResponse:
    return EntitiesResponse(
        items=[
            EntityInfo(
                entity_id=ENTITY_ID,
                name="Тестовые серверы",
            )
        ]
    )


# 2. Структура сущности

@app.get(
    f"{PREFIX}/entities/{{entity_id}}/structure",
    response_model=StructureResponse,
    tags=["Entity API"],
)
def get_entity_structure(entity_id: str) -> StructureResponse:
    require_entity(entity_id)

    return StructureResponse(
        entity_id=entity_id,
        fields=[
            FieldInfo(
                name="source_record_id",
                type="string",
                required=True,
            ),
            FieldInfo(
                name="name",
                type="string",
                required=True,
            ),
            FieldInfo(
                name="target",
                type="string",
                required=True,
            ),
            FieldInfo(
                name="check_type",
                type="string",
                required=True,
            ),
            FieldInfo(
                name="active",
                type="boolean",
                required=True,
            ),
        ],
    )


# 3. Параметры чтения записей

@app.get(
    f"{PREFIX}/entities/{{entity_id}}/parameters",
    response_model=ParametersResponse,
    tags=["Entity API"],
)
def get_entity_parameters(entity_id: str) -> ParametersResponse:
    require_entity(entity_id)

    return ParametersResponse(
        entity_id=entity_id,
        parameters=[
            FieldInfo(
                name="limit",
                type="integer",
                required=False,
            ),
            FieldInfo(
                name="cursor",
                type="string",
                required=False,
            ),
        ],
    )


# 4. Доступные фильтры

@app.get(
    f"{PREFIX}/entities/{{entity_id}}/filters",
    response_model=FiltersResponse,
    tags=["Entity API"],
)
def get_entity_filters(entity_id: str) -> FiltersResponse:
    require_entity(entity_id)

    return FiltersResponse(
        entity_id=entity_id,
        filters=[
            FieldInfo(
                name="active",
                type="boolean",
                required=False,
            )
        ],
    )


# 5. Получение записей с пагинацией

@app.get(
    f"{PREFIX}/entities/{{entity_id}}/records",
    response_model=RecordsResponse,
    tags=["Entity API"],
)
def get_entity_records(
    entity_id: str,
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = Query(default=None),
    active: bool | None = Query(default=None),
) -> RecordsResponse:
    require_entity(entity_id)

    if cursor is None:
        start = 0
    elif not cursor.isdecimal() or len(cursor) > 9:
        raise HTTPException(
            status_code=422,
            detail="Некорректный cursor",
        )
    else:
        start = int(cursor)

    records = TEST_RECORDS

    if active is not None:
        records = [
            record
            for record in records
            if record["active"] is active
        ]

    end = start + limit

    return RecordsResponse(
        items=records[start:end],
        next_cursor=(
            str(end)
            if end < len(records)
            else None
        ),
    )
