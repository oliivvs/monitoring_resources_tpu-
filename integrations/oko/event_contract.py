"""N23. Исходящий JSON-контракт Push-интеграции с ОКО.

Локальная проверка формата до сохранения в Outbox. Никакой сети,
схемы БД, секретов или production-URL этот модуль не содержит.
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime
from uuid import UUID

from shared.schemas import EventType, StatusEventContext, to_utc_iso

MAX_EVENT_BYTES = 16 * 1024
MAX_RESOURCE_ID = 255
MAX_RESOURCE_NAME = 500
MAX_ERROR_LENGTH = 4096

# Превентивный лимит наших диагностических сообщений,
# ниже максимума ОКО.
OUTGOING_ERROR_LENGTH = 1024

FIELDS = frozenset({
    "event_id",
    "event_type",
    "resource_id",
    "resource_name",
    "status",
    "check_type",
    "checked_at",
    "response_time_ms",
    "error",
})

_UTC_ISO = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)

_BEARER = re.compile(r"(?i)\bbearer\s+\S+")
_URL = re.compile(r"""(?i)https?://[^\s<>'"]+""")
_CONTROL = re.compile(r"[\x00-\x1f\x7f]+")


class OkoContractError(ValueError):
    """Некорректное событие без раскрытия исходного JSON."""


def _no_duplicate_fields(
    pairs: list[tuple[str, object]],
) -> dict:
    result = {}

    for key, value in pairs:
        if key in result:
            raise OkoContractError(
                "Повторяющийся ключ JSON"
            )
        result[key] = value

    return result


def _invalid_constant(_: str) -> None:
    raise OkoContractError(
        "JSON содержит недопустимое числовое значение"
    )


def validate_oko_payload(payload: str | bytes) -> bytes:
    """Проверяет девять полей и возвращает исходные байты.

    Возврат исходных байтов важен для at-least-once доставки:
    повторная попытка не должна менять JSON или event_id.
    """

    if not isinstance(payload, (str, bytes)):
        raise OkoContractError(
            "Ожидается строковый JSON"
        )

    try:
        raw = (
            payload.encode("utf-8")
            if isinstance(payload, str)
            else payload
        )
    except UnicodeError:
        raise OkoContractError(
            "JSON не кодируется в UTF-8"
        ) from None

    if len(raw) > MAX_EVENT_BYTES:
        raise OkoContractError(
            "Размер события превышает 16 КБ"
        )

    try:
        obj = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_no_duplicate_fields,
            parse_constant=_invalid_constant,
        )
    except (UnicodeError, json.JSONDecodeError):
        raise OkoContractError(
            "Некорректный JSON"
        ) from None

    if not isinstance(obj, dict) or set(obj) != FIELDS:
        raise OkoContractError(
            "Ожидаются ровно девять полей события"
        )

    if not isinstance(obj["event_id"], str):
        raise OkoContractError(
            "event_id должен быть UUID"
        )

    try:
        UUID(obj["event_id"])
    except ValueError:
        raise OkoContractError(
            "event_id должен быть UUID"
        ) from None

    if obj["event_type"] not in (
        "STATUS_CHANGED",
        "HEARTBEAT",
    ):
        raise OkoContractError(
            "Недопустимый event_type"
        )

    if obj["status"] not in (
        "UP",
        "DOWN",
        "UNKNOWN",
    ):
        raise OkoContractError(
            "Недопустимый status"
        )

    if obj["check_type"] not in ("HTTP", "TCP"):
        raise OkoContractError(
            "Недопустимый check_type"
        )

    for key, maximum in (
        ("resource_id", MAX_RESOURCE_ID),
        ("resource_name", MAX_RESOURCE_NAME),
    ):
        value = obj[key]

        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > maximum
        ):
            raise OkoContractError(
                f"Неверное поле {key}"
            )

    checked = obj["checked_at"]

    if (
        not isinstance(checked, str)
        or not _UTC_ISO.fullmatch(checked)
    ):
        raise OkoContractError(
            "checked_at должен быть в UTC с суффиксом Z"
        )

    try:
        datetime.fromisoformat(
            checked.replace("Z", "+00:00")
        )
    except ValueError:
        raise OkoContractError(
            "Неверное значение checked_at"
        ) from None

    latency = obj["response_time_ms"]

    try:
        valid_latency = (
            latency is None
            or (
                not isinstance(latency, bool)
                and isinstance(latency, (int, float))
                and math.isfinite(latency)
                and latency >= 0
            )
        )
    except (OverflowError, ValueError):
        valid_latency = False

    if not valid_latency:
        raise OkoContractError(
            "response_time_ms должен быть "
            "неотрицательным числом или null"
        )

    error = obj["error"]

    if error is not None and (
        not isinstance(error, str)
        or len(error) > MAX_ERROR_LENGTH
    ):
        raise OkoContractError(
            "error должен быть строкой "
            "до 4096 символов или null"
        )

    return raw


def _safe_error(error: str | None) -> str | None:
    """Убирает типовые секреты и сокращает диагностику."""

    if error is None:
        return None

    value = _CONTROL.sub(" ", error)
    value = _BEARER.sub("Bearer [скрыто]", value)
    value = _URL.sub("[URL скрыт]", value)

    return value[:OUTGOING_ERROR_LENGTH]


def build_oko_payload(
    context: StatusEventContext,
    kind: EventType,
    event_id: UUID,
) -> str:
    """Формирует один проверенный JSON перед записью в Outbox."""

    body = {
        "event_id": str(event_id),
        "event_type": kind.value,
        "resource_id": context.resource_id,
        "resource_name": context.resource_name,
        "status": context.status.value,
        "check_type": context.check_type.value,
        "checked_at": to_utc_iso(context.checked_at),
        "response_time_ms": context.response_time_ms,
        "error": _safe_error(context.error),
    }

    result = json.dumps(
        body,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    validate_oko_payload(result)

    return result