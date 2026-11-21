"""N23: контракт ОКО и цикл Outbox -> sender без внешней сети."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text

from app.config import AppSettings
from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)
from integrations.oko.event_contract import (
    FIELDS,
    MAX_EVENT_BYTES,
    OkoContractError,
    build_oko_payload,
    validate_oko_payload,
)
from services.events import enqueue_heartbeat_if_due
from services.resources import upsert_configuration
from services.sender import send_one_event
from services.status import apply_check_result
from shared.schemas import (
    CheckResultIn,
    EventType,
    ResourceConfig,
    StatusEventContext,
)


BASE = datetime(
    2026, 11, 21, 8, 0,
    tzinfo=timezone.utc,
)


@pytest.fixture
def engine(tmp_path):
    db = make_engine(tmp_path / "n23.sqlite")
    initialize(db)

    yield db

    db.dispose()


def sample_json(**updates):
    obj = {
        "event_id": str(uuid4()),
        "event_type": "STATUS_CHANGED",
        "resource_id": "tpu-test-http",
        "resource_name": "Тестовый HTTP-сервис",
        "status": "DOWN",
        "check_type": "HTTP",
        "checked_at": "2026-11-21T08:00:01.000000Z",
        "response_time_ms": None,
        "error": None,
    }

    obj.update(updates)

    return json.dumps(
        obj,
        ensure_ascii=False,
    )


def test_exact_nine_fields_and_nullable_fields():
    text_json = sample_json()

    assert set(json.loads(text_json)) == FIELDS

    assert validate_oko_payload(
        text_json
    ) == text_json.encode("utf-8")


@pytest.mark.parametrize(
    "changes",
    [
        {"event_type": "OTHER"},
        {"status": "ERROR"},
        {"check_type": "ICMP"},
        {"event_id": "not-uuid"},
        {"resource_id": ""},
        {"resource_id": "r" * 256},
        {"resource_name": "n" * 501},
        {"error": "x" * 4097},
        {"response_time_ms": -1},
        {"response_time_ms": True},
        {"checked_at": "2026-11-21T08:00:01+00:00"},
        {"checked_at": "2026-02-31T08:00:01Z"},
    ],
)
def test_rejects_invalid_fields(changes):
    with pytest.raises(OkoContractError):
        validate_oko_payload(
            sample_json(**changes)
        )


def test_rejects_missing_extra_and_duplicate_fields():
    data = json.loads(sample_json())

    data.pop("error")

    with pytest.raises(OkoContractError):
        validate_oko_payload(
            json.dumps(data)
        )

    data["error"] = None
    data["debug"] = "secret"

    with pytest.raises(OkoContractError):
        validate_oko_payload(
            json.dumps(data)
        )

    duplicate = (
        sample_json()[:-1]
        + ',"status":"UP"}'
    )

    with pytest.raises(
        OkoContractError,
        match="Повторяющийся",
    ):
        validate_oko_payload(duplicate)


def test_size_is_limited_by_utf8_bytes_not_characters():
    oversized = sample_json(
        error="🚀" * 4096
    )

    assert len(oversized) < MAX_EVENT_BYTES

    with pytest.raises(
        OkoContractError,
        match="16 КБ",
    ):
        validate_oko_payload(oversized)


def test_nonfinite_numbers_and_bad_json():
    for raw in (
        sample_json(response_time_ms=float("nan")),
        sample_json(response_time_ms=float("inf")),
        "not json",
        b"\xff",
    ):
        with pytest.raises(OkoContractError):
            validate_oko_payload(raw)


def test_builder_redacts_url_and_token_and_limits_error():
    event = StatusEventContext(
        resource_id="test-http",
        resource_name="Тестовый ресурс",
        status="DOWN",
        check_type="HTTP",
        checked_at=BASE,
        response_time_ms=None,
        error=(
            "ReadTimeout https://private.example/secret "
            "Bearer very-secret "
            + "Z" * 9000
        ),
    )

    raw = build_oko_payload(
        event,
        EventType.STATUS_CHANGED,
        uuid4(),
    )

    obj = json.loads(
        validate_oko_payload(raw)
    )

    assert len(obj["error"]) <= 1024
    assert "private.example" not in obj["error"]
    assert "very-secret" not in obj["error"]
    assert obj["checked_at"].endswith("Z")


def create_status_change(engine, *, error=None):
    with write_transaction(engine) as conn:
        version = upsert_configuration(
            conn,
            ResourceConfig(
                id="n23-http",
                name="N23 Тестовый сервис",
                target="https://example.org/health",
                check_type="HTTP",
                up_after=1,
                down_after=1,
                heartbeat_interval_seconds=75,
            ),
            now=BASE,
        )

    with write_transaction(engine) as conn:
        return apply_check_result(
            conn,
            result=CheckResultIn(
                resource_id="n23-http",
                config_version_id=version,
                checked_at=BASE + timedelta(seconds=1),
                success=error is None,
                response_time_ms=(
                    None if error else 15.5
                ),
                http_status_code=(
                    503 if error else 200
                ),
                error_message=error,
            ),
        )


def get_payloads(engine):
    with read_transaction(engine) as conn:
        return conn.execute(
            text("""
                SELECT
                    event_id,
                    event_type,
                    payload_json,
                    checked_at
                FROM outbox_events
                ORDER BY created_at, event_id
            """)
        ).mappings().all()


def test_generated_status_change_has_contract(engine):
    create_status_change(engine)

    rows = get_payloads(engine)

    assert len(rows) == 1

    obj = json.loads(
        validate_oko_payload(
            rows[0]["payload_json"]
        )
    )

    assert obj["event_type"] == "STATUS_CHANGED"
    assert obj["status"] == "UP"
    assert obj["event_id"] == rows[0]["event_id"]
    assert obj["checked_at"] == rows[0]["checked_at"]


def test_long_error_does_not_break_status_or_outbox(engine):
    create_status_change(
        engine,
        error=(
            "Timeout https://private.example "
            + "X" * 12000
        ),
    )

    rows = get_payloads(engine)

    assert len(rows) == 1

    obj = json.loads(
        validate_oko_payload(
            rows[0]["payload_json"]
        )
    )

    assert obj["status"] == "DOWN"
    assert len(obj["error"]) <= 1024

    assert (
        len(rows[0]["payload_json"].encode("utf-8"))
        <= MAX_EVENT_BYTES
    )

    assert (
        "private.example"
        not in rows[0]["payload_json"]
    )


def test_heartbeat_uses_last_real_check_time(engine):
    create_status_change(engine)

    with write_transaction(engine) as conn:
        eid = enqueue_heartbeat_if_due(
            conn,
            resource_id="n23-http",
            now=BASE + timedelta(seconds=76),
        )

    assert eid is not None

    rows = get_payloads(engine)

    assert len(rows) == 2

    by_type = {
        row["event_type"]: json.loads(
            validate_oko_payload(
                row["payload_json"]
            )
        )
        for row in rows
    }

    assert (
        by_type["HEARTBEAT"]["checked_at"]
        == by_type["STATUS_CHANGED"]["checked_at"]
    )

    assert (
        by_type["HEARTBEAT"]["event_id"]
        != by_type["STATUS_CHANGED"]["event_id"]
    )


@pytest.mark.parametrize(
    "http_code, expected",
    [
        (200, "SENT"),
        (202, "SENT"),
        (400, "FAILED"),
        (401, "FAILED"),
        (409, "FAILED"),
        (429, "PENDING"),
        (503, "PENDING"),
    ],
)
def test_sender_http_classification(
    engine,
    http_code,
    expected,
):
    create_status_change(engine)

    sent = []

    def endpoint(request):
        sent.append(request.content)

        validate_oko_payload(request.content)

        assert (
            request.headers["Authorization"]
            == "Bearer test-token"
        )

        return httpx.Response(http_code)

    settings = AppSettings(
        _env_file=None,
        event_post_url=(
            "https://oko.example/api/v1/status/events"
        ),
        event_token="test-token",
    )

    with httpx.Client(
        transport=httpx.MockTransport(endpoint)
    ) as client:
        result = send_one_event(
            engine,
            settings,
            worker_id="n23-worker",
            client=client,
            now=BASE + timedelta(seconds=5),
        )

    assert result == expected
    assert len(sent) == 1


def test_retry_preserves_exact_bytes_and_event_id(engine):
    create_status_change(engine)

    sent = []
    responses = [503, 202]

    def endpoint(request):
        sent.append(request.content)

        return httpx.Response(
            responses.pop(0)
        )

    settings = AppSettings(
        _env_file=None,
        event_post_url=(
            "https://oko.example/api/v1/status/events"
        ),
        event_token="test-token",
    )

    with httpx.Client(
        transport=httpx.MockTransport(endpoint)
    ) as client:
        assert send_one_event(
            engine,
            settings,
            worker_id="n23-worker",
            client=client,
            now=BASE + timedelta(seconds=5),
        ) == "PENDING"

        assert send_one_event(
            engine,
            settings,
            worker_id="n23-worker",
            client=client,
            now=BASE + timedelta(seconds=7),
        ) == "SENT"

    assert sent[0] == sent[1]

    assert (
        json.loads(sent[0])["event_id"]
        == get_payloads(engine)[0]["event_id"]
    )