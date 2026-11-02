
"""REST API для просмотра ресурсов мониторинга."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.database import get_read_connection
from shared.schemas import (
    CheckResultRead,
    ResourceRead,
    ResourceStatusRead,
)


router = APIRouter(
    prefix="/api/v1/resources",
    tags=["Ресурсы"],
)


# Общий SQL-запрос для получения ресурсов.
RESOURCE_SELECT = """
    SELECT
        r.id,
        r.name,
        r.active,
        c.id AS config_version_id,
        c.version_number AS config_version,
        c.target,
        c.check_type,
        c.port,
        c.interval_seconds,
        c.timeout_seconds,
        c.up_after,
        c.down_after,
        c.heartbeat_interval_seconds,
        s.current_status,
        s.ok_count,
        s.fail_count,
        s.last_checked_at,
        s.status_changed_at
    FROM resources AS r
    JOIN resource_states AS s
        ON s.resource_id = r.id
    JOIN resource_config_versions AS c
        ON c.id = s.config_version_id
    WHERE c.retired_at IS NULL
"""


def build_resource(conn: Connection, row) -> ResourceRead:
    """Преобразует запись SQLite в модель ресурса."""

    data = dict(row)
    version_id = data.pop("config_version_id")

    # Получаем ожидаемые диапазоны HTTP-кодов.
    ranges = conn.execute(
        text("""
            SELECT
                code_start AS start,
                code_end AS end
            FROM http_expected_ranges
            WHERE config_version_id = :version_id
            ORDER BY code_start, code_end
        """),
        {"version_id": version_id},
    ).mappings().all()

    data["active"] = bool(data["active"])
    data["expected_http_codes"] = [
        dict(item) for item in ranges
    ]

    return ResourceRead.model_validate(data)


@router.get(
    "",
    response_model=list[ResourceRead],
    summary="Получить список ресурсов",
)
def list_resources(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    conn: Connection = Depends(get_read_connection),
) -> list[ResourceRead]:
    """Возвращает список ресурсов с текущими статусами."""

    rows = conn.execute(
        text(
            RESOURCE_SELECT
            + " ORDER BY r.id LIMIT :limit OFFSET :offset"
        ),
        {
            "limit": limit,
            "offset": offset,
        },
    ).mappings().all()

    return [
        build_resource(conn, row)
        for row in rows
    ]


@router.get(
    "/{resource_id}",
    response_model=ResourceRead,
    summary="Получить ресурс по идентификатору",
)
def get_resource(
    resource_id: str,
    conn: Connection = Depends(get_read_connection),
) -> ResourceRead:
    """Возвращает данные одного ресурса."""

    row = conn.execute(
        text(
            RESOURCE_SELECT
            + " AND r.id = :resource_id"
        ),
        {"resource_id": resource_id},
    ).mappings().first()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Ресурс не найден",
        )

    return build_resource(conn, row)


@router.get(
    "/{resource_id}/status",
    response_model=ResourceStatusRead,
    summary="Получить текущий статус ресурса",
)
def get_resource_status(
    resource_id: str,
    conn: Connection = Depends(get_read_connection),
) -> ResourceStatusRead:
    """Возвращает текущий статус и последнюю ошибку."""

    row = conn.execute(
        text("""
            SELECT
                r.id AS resource_id,
                s.current_status AS status,
                s.last_checked_at AS checked_at,
                (
                    SELECT cr.error_message
                    FROM check_results AS cr
                    WHERE cr.resource_id = r.id
                    ORDER BY cr.checked_at DESC, cr.id DESC
                    LIMIT 1
                ) AS error
            FROM resources AS r
            JOIN resource_states AS s
                ON s.resource_id = r.id
            WHERE r.id = :resource_id
        """),
        {"resource_id": resource_id},
    ).mappings().first()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Ресурс не найден",
        )

    return ResourceStatusRead.model_validate(dict(row))


@router.get(
    "/{resource_id}/checks",
    response_model=list[CheckResultRead],
    summary="Получить историю проверок ресурса",
)
def get_resource_checks(
    resource_id: str,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    conn: Connection = Depends(get_read_connection),
) -> list[CheckResultRead]:
    """Возвращает историю проверок от новых к старым."""

    # Сначала проверяем существование ресурса.
    exists = conn.execute(
        text("""
            SELECT 1
            FROM resources
            WHERE id = :resource_id
        """),
        {"resource_id": resource_id},
    ).first()

    if exists is None:
        raise HTTPException(
            status_code=404,
            detail="Ресурс не найден",
        )

    rows = conn.execute(
        text("""
            SELECT
                id,
                resource_id,
                config_version_id,
                checked_at,
                success,
                response_time_ms,
                http_status_code,
                error_message,
                resulting_status
            FROM check_results
            WHERE resource_id = :resource_id
            ORDER BY checked_at DESC, id DESC
            LIMIT :limit OFFSET :offset
        """),
        {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        },
    ).mappings().all()

    result = []

    for row in rows:
        data = dict(row)
        data["success"] = bool(data["success"])

        result.append(
            CheckResultRead.model_validate(data)
        )

    return result
