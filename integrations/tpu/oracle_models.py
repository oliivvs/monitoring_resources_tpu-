
"""Нормализованный тестовый контракт уведомления Oracle Sync.

Фактические коды/поля production Oracle Sync должны подтверждаться командой ТПУ.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


OPERATION_NAMES = {1: "INSERT", 2: "DELETE", 3: "UPDATE"}


class OracleSyncEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    source: str = Field(min_length=1, max_length=128)
    entity: str = Field(min_length=1, max_length=128)
    record_id: str = Field(min_length=1, max_length=255)
    operation_code: Literal[1, 2, 3]
    source_sequence: int | None = Field(default=None, ge=0)
    occurred_at: AwareDatetime
    data: dict[str, Any]

    @field_validator("source", "entity", "record_id")
    @classmethod
    def non_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Поле не может быть пустым")
        return value.strip()

    @field_validator("occurred_at")
    @classmethod
    def utc_only(cls, value: AwareDatetime) -> AwareDatetime:
        if value.utcoffset() != timedelta(0):
            raise ValueError("occurred_at должен быть временем UTC")
        return value


class OracleSyncReceipt(BaseModel):
    event_id: UUID
    result: Literal["accepted", "duplicate"]
