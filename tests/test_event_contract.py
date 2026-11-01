
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from shared.schemas import StatusEventPayload
import json

# Образец события из технического задания
EXAMPLE = {
    "event_id": "7d51a70e-6bea-4d1c-9805-d0157ddc0d90",
    "resource_id": "server-001",
    "resource_name": "Test Web Server",
    "status": "DOWN",
    "check_type": "HTTP",
    "checked_at": "2026-10-31T08:00:00Z",
    "response_time_ms": None,
    "error": "Connection timeout",
    "event_type": "STATUS_CHANGED",
}


def test_event_matches_specification():
    event = StatusEventPayload.model_validate(EXAMPLE)
    result = event.model_dump(mode="json")

    assert set(result) == set(EXAMPLE)
    assert result["event_id"] == EXAMPLE["event_id"]
    assert result["status"] == "DOWN"
    assert result["event_type"] == "STATUS_CHANGED"
    assert result["checked_at"].endswith("Z")


def test_heartbeat_uses_same_contract():
    data = {**EXAMPLE, "event_type": "HEARTBEAT"}
    event = StatusEventPayload.model_validate(data)

    assert event.event_type.value == "HEARTBEAT"


def test_missing_event_id_rejected():
    data = {key: value for key, value in EXAMPLE.items()
            if key != "event_id"}

    with pytest.raises(ValidationError):
        StatusEventPayload.model_validate(data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "ERROR"),
        ("event_type", "UNKNOWN_EVENT"),
        ("check_type", "UDP"),
        ("response_time_ms", -10),
        ("event_id", "not-a-uuid"),
        ("checked_at", "2026-10-31T08:00:00"),
        ("extra_field", "unexpected"),
    ],
)
def test_invalid_event_rejected(field, value):
    data = {**EXAMPLE, field: value}

    with pytest.raises(ValidationError):
        StatusEventPayload.model_validate(data)

def test_event_json_serialization():
    # Создаём событие из подготовленных данных
    event = StatusEventPayload.model_validate(EXAMPLE)

    # Преобразуем событие в JSON для HTTP-запроса
    json_body = event.model_dump_json()
    data = json.loads(json_body)

    # Проверяем формат передаваемых данных
    assert isinstance(data["event_id"], str)
    assert isinstance(data["checked_at"], str)
    assert data["status"] == "DOWN"
    assert data["event_type"] == "STATUS_CHANGED"

    # Проверяем, что JSON можно восстановить в модель
    restored = StatusEventPayload.model_validate_json(json_body)

    assert restored == event
