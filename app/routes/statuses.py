
"""Защищённый Pull-снимок текущих статусов для внешних клиентов."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.database import get_read_connection
from app.security_tokens import require_read_token
from shared.schemas import CheckType, ResourceStatus


class CurrentStatusItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_id: str
    resource_name: str
    check_type: CheckType
    status: ResourceStatus
    active: bool
    checked_at: AwareDatetime | None
    status_changed_at: AwareDatetime | None


class StatusesPage(BaseModel):
    items: list[CurrentStatusItem]
    limit: int = Field(ge=1, le=100)
    offset: int = Field(ge=0)
    has_more: bool


router = APIRouter(prefix="/api/v1", tags=["Внешний API статусов"])


@router.get(
    "/statuses",
    response_model=StatusesPage,
    summary="Текущие статусы ресурсов (READ_TOKEN)",
    dependencies=[Depends(require_read_token)],
)
def get_statuses(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    conn: Connection = Depends(get_read_connection),
) -> StatusesPage:
    """Возвращает ВСЕ ресурсы, в т.ч. inactive, но без target/IP/error."""
    rows = conn.execute(
        text("""
            SELECT r.id AS resource_id, r.name AS resource_name,
                   c.check_type, s.current_status AS status,
                   r.active, s.last_checked_at AS checked_at,
                   s.status_changed_at
            FROM resources AS r
            JOIN resource_states AS s ON s.resource_id = r.id
            JOIN resource_config_versions AS c ON c.id = s.config_version_id
            WHERE c.retired_at IS NULL
            ORDER BY r.id
            LIMIT :limit_plus_one OFFSET :offset
        """),
        {"limit_plus_one": limit + 1, "offset": offset},
    ).mappings().all()

    items = [
        CurrentStatusItem.model_validate({**row, "active": bool(row["active"])})
        for row in rows[:limit]
    ]
    return StatusesPage(
        items=items,
        limit=limit,
        offset=offset,
        has_more=len(rows) > limit,
    )
