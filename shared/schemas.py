"""Общий контракт Pydantic: одна служба = один resource_id."""
from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Annotated
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class CheckType(StrEnum):
    HTTP = "HTTP"
    TCP = "TCP"
    # ICMP не включён до реализации необязательной сетевой проверки.


class ResourceStatus(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    UNKNOWN = "UNKNOWN"


class EventType(StrEnum):
    STATUS_CHANGED = "STATUS_CHANGED"
    HEARTBEAT = "HEARTBEAT"


class DeliveryStatus(StrEnum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"


class ExpectedHttpRange(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    start: int = Field(ge=100, le=599)
    end: int = Field(ge=100, le=599)

    @model_validator(mode="after")
    def check_range(self):
        if self.end < self.start:
            raise ValueError("Конец диапазона меньше начала")
        return self


class ResourceConfig(StrictModel):
    id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=255)
    target: str = Field(min_length=1)
    check_type: CheckType
    port: int | None = Field(default=None, ge=1, le=65535)
    interval_seconds: int = Field(default=60, ge=1)
    timeout_seconds: float = Field(default=5.0, gt=0)
    expected_http_codes: list[ExpectedHttpRange] = Field(
        default_factory=lambda: [ExpectedHttpRange(start=200, end=299)]
    )
    active: bool = True
    up_after: int = Field(default=2, ge=1)
    down_after: int = Field(default=3, ge=1)
    heartbeat_interval_seconds: int = Field(default=300, ge=1)

    @field_validator("id", "name", "target")
    @classmethod
    def strip(cls, val: str):
        val = val.strip()
        if not val:
            raise ValueError("Пустые значения не допускаются")
        return val

    @model_validator(mode="after")
    def validate_target(self):
        if self.check_type == CheckType.HTTP:
            parsed = urlsplit(self.target)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                raise ValueError("HTTP target должен быть http(s) URL")
            if self.port is not None:
                raise ValueError("Для HTTP порт указывается внутри URL")
            if not self.expected_http_codes:
                raise ValueError("Нужен хотя бы один ожидаемый HTTP-код или диапазон")
        else:
            if self.port is None:
                raise ValueError("Для TCP обязателен порт")
            if "://" in self.target or "/" in self.target:
                raise ValueError("Для TCP нужен только хост или IP")
            if self.expected_http_codes:
                raise ValueError("Коды HTTP не используются для TCP")
        return self


class ResourceRead(ResourceConfig):
    config_version: int = Field(ge=1)
    current_status: ResourceStatus
    ok_count: int = Field(ge=0)
    fail_count: int = Field(ge=0)
    last_checked_at: AwareDatetime | None
    status_changed_at: AwareDatetime | None


class CheckSnapshot(StrictModel):
    """Неизменяемые параметры одной запущенной проверки, прочитанные до сетевого I/O."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    resource_id: str = Field(min_length=1)
    config_version_id: int = Field(ge=1)
    target: str
    check_type: CheckType
    port: int | None
    interval_seconds: int = Field(ge=1)
    timeout_seconds: float = Field(gt=0)
    expected_http_codes: tuple[ExpectedHttpRange, ...]


class CheckResultIn(StrictModel):
    resource_id: str = Field(min_length=1)
    # Обязателен ID версии из CheckSnapshot, полученный ДО выполнения HTTP/TCP.
    # Нельзя подставлять актуальный ID при сохранении результата.
    config_version_id: int = Field(ge=1)
    checked_at: AwareDatetime
    success: bool
    response_time_ms: float | None = Field(default=None, ge=0)
    http_status_code: int | None = Field(default=None, ge=100, le=599)
    error_message: str | None = None


class CheckResultRead(CheckResultIn):
    id: int = Field(ge=1)
    resulting_status: ResourceStatus


class StatusEventContext(StrictModel):
    resource_id: str
    resource_name: str
    status: ResourceStatus
    check_type: CheckType
    checked_at: AwareDatetime
    response_time_ms: float | None = Field(ge=0)
    error: str | None = None


class StatusEventPayload(StatusEventContext):
    event_id: UUID
    event_type: EventType


class ResourceStatusRead(StrictModel):
    resource_id: str
    status: ResourceStatus
    checked_at: AwareDatetime | None
    error: str | None


class OutboxEventRead(StrictModel):
    event_id: UUID
    resource_id: str
    event_type: EventType
    status: ResourceStatus
    checked_at: AwareDatetime
    payload_json: str
    delivery_status: DeliveryStatus
    attempts: int = Field(ge=0)
    next_attempt_at: AwareDatetime | None
    delivered_at: AwareDatetime | None
    last_error: str | None
    created_at: AwareDatetime


def to_utc_iso(date: datetime) -> str:
    if date.utcoffset() is None:
        raise ValueError("Требуется timezone-aware datetime")
    return date.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def from_utc_iso(text: str) -> datetime:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.utcoffset() is None:
        raise ValueError("Ожидался ISO 8601 UTC")
    return dt.astimezone(timezone.utc)
