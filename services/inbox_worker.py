
"""A21: надёжное извлечение событий Oracle Sync из SQLite Inbox.

Этот модуль пока не изменяет ресурсы. Реальный обработчик
INSERT/UPDATE/DELETE подключим в A22.

Обработчик должен выполнять только операции SQLite через
переданную Connection, без HTTP, DNS и самостоятельного commit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Literal
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from db.session import write_transaction
from shared.schemas import to_utc_iso


@dataclass(frozen=True)
class InboxClaim:
    event_id: str
    source_system: str
    source_entity: str
    source_record_id: str
    operation: str
    source_sequence: int | None
    payload_json: str
    attempts: int
    claim_token: str


@dataclass(frozen=True)
class ProcessResult:
    event_id: str
    disposition: Literal[
        "PROCESSED", "RETRY", "FAILED", "STALE"
    ]


InboxHandler = Callable[[Connection, InboxClaim], None]


def _clock(now: datetime | None) -> datetime:
    value = (
        now if now is not None
        else datetime.now(timezone.utc)
    )

    if value.utcoffset() is None:
        raise ValueError(
            "Требуется timezone-aware время"
        )

    return value.astimezone(timezone.utc)


def _limits(
    *,
    lease_seconds: int,
    max_attempts: int,
) -> None:
    if (
        type(lease_seconds) is not int
        or not 1 <= lease_seconds <= 3600
    ):
        raise ValueError(
            "lease_seconds должен быть от 1 до 3600"
        )

    if (
        type(max_attempts) is not int
        or not 1 <= max_attempts <= 32
    ):
        raise ValueError(
            "max_attempts должен быть от 1 до 32"
        )


def claim_inbox_event(
    engine: Engine,
    *,
    now: datetime | None = None,
    lease_seconds: int = 60,
    max_attempts: int = 5,
) -> InboxClaim | None:
    """Атомарно захватывает готовое событие.

    BEGIN IMMEDIATE сериализует операции SQLite.
    Истёкший захват может получить другой Worker.
    """

    _limits(
        lease_seconds=lease_seconds,
        max_attempts=max_attempts,
    )

    timestamp = _clock(now)
    now_s = to_utc_iso(timestamp)
    token = uuid4().hex

    until_s = to_utc_iso(
        timestamp + timedelta(seconds=lease_seconds)
    )

    with write_transaction(engine) as conn:
        # Истёкшие события с исчерпанными попытками.
        conn.execute(
            text("""
                UPDATE inbox_events
                SET processing_status='FAILED',
                    lease_expires_at=NULL,
                    claim_token=NULL,
                    next_attempt_at=NULL,
                    last_error='LeaseExpired'
                WHERE processing_status='PROCESSING'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= :now
                  AND attempts >= :max_attempts
            """),
            {
                "now": now_s,
                "max_attempts": max_attempts,
            },
        )

        row = conn.execute(
            text("""
                SELECT
                    event_id,
                    source_system,
                    source_entity,
                    source_record_id,
                    operation,
                    source_sequence,
                    payload_json,
                    attempts
                FROM inbox_events
                WHERE (
                    processing_status='PENDING'
                    AND (
                        next_attempt_at IS NULL
                        OR next_attempt_at <= :now
                    )
                    AND attempts < :max_attempts
                )
                OR (
                    processing_status='PROCESSING'
                    AND lease_expires_at IS NOT NULL
                    AND lease_expires_at <= :now
                    AND attempts < :max_attempts
                )
                ORDER BY received_at, event_id
                LIMIT 1
            """),
            {
                "now": now_s,
                "max_attempts": max_attempts,
            },
        ).mappings().first()

        if row is None:
            return None

        conn.execute(
            text("""
                UPDATE inbox_events
                SET processing_status='PROCESSING',
                    attempts=attempts+1,
                    claim_token=:token,
                    lease_expires_at=:until,
                    next_attempt_at=NULL
                WHERE event_id=:event_id
            """),
            {
                "token": token,
                "until": until_s,
                "event_id": row["event_id"],
            },
        )

    return InboxClaim(
        event_id=row["event_id"],
        source_system=row["source_system"],
        source_entity=row["source_entity"],
        source_record_id=row["source_record_id"],
        operation=row["operation"],
        source_sequence=row["source_sequence"],
        payload_json=row["payload_json"],
        attempts=row["attempts"] + 1,
        claim_token=token,
    )


def process_one_inbox_event(
    engine: Engine,
    handler: InboxHandler,
    *,
    now: datetime | None = None,
    lease_seconds: int = 60,
    max_attempts: int = 5,
    retry_base_seconds: int = 5,
) -> ProcessResult | None:
    """Обрабатывает одно событие с атомарным сохранением.

    handler вызывается внутри SQLite-транзакции.
    Нельзя выполнять HTTP, DNS или commit внутри handler.
    """

    if not callable(handler):
        raise ValueError(
            "Требуется реальный обработчик события"
        )

    if (
        type(retry_base_seconds) is not int
        or not 1 <= retry_base_seconds <= 3600
    ):
        raise ValueError(
            "retry_base_seconds должен быть от 1 до 3600"
        )

    timestamp = _clock(now)

    claim = claim_inbox_event(
        engine,
        now=timestamp,
        lease_seconds=lease_seconds,
        max_attempts=max_attempts,
    )

    if claim is None:
        return None

    try:
        with write_transaction(engine) as conn:
            # Только актуальный владелец может
            # применить изменение к базе.
            owned = conn.execute(
                text("""
                    SELECT 1
                    FROM inbox_events
                    WHERE event_id=:id
                      AND claim_token=:token
                      AND processing_status='PROCESSING'
                """),
                {
                    "id": claim.event_id,
                    "token": claim.claim_token,
                },
            ).scalar_one_or_none()

            if owned is None:
                return ProcessResult(
                    claim.event_id,
                    "STALE",
                )

            # Здесь позже подключим бизнес-логику A22.
            handler(conn, claim)

            conn.execute(
                text("""
                    UPDATE inbox_events
                    SET processing_status='PROCESSED',
                        processed_at=:at,
                        claim_token=NULL,
                        lease_expires_at=NULL,
                        next_attempt_at=NULL,
                        last_error=NULL
                    WHERE event_id=:id
                      AND claim_token=:token
                      AND processing_status='PROCESSING'
                """),
                {
                    "id": claim.event_id,
                    "token": claim.claim_token,
                    "at": to_utc_iso(
                        _clock(None)
                        if now is None
                        else timestamp
                    ),
                },
            )

        return ProcessResult(
            claim.event_id,
            "PROCESSED",
        )

    except Exception as exc:
        # Не сохраняем текст исключения:
        # он может содержать секреты.
        error_code = type(exc).__name__[:100]

        retry = claim.attempts < max_attempts

        delay = min(
            retry_base_seconds
            * 2 ** (claim.attempts - 1),
            3600,
        )

        retry_at = (
            to_utc_iso(
                timestamp + timedelta(seconds=delay)
            )
            if retry
            else None
        )

        with write_transaction(engine) as conn:
            row_count = conn.execute(
                text("""
                    UPDATE inbox_events
                    SET processing_status=:status,
                        claim_token=NULL,
                        lease_expires_at=NULL,
                        next_attempt_at=:retry_at,
                        last_error=:error
                    WHERE event_id=:id
                      AND claim_token=:token
                      AND processing_status='PROCESSING'
                """),
                {
                    "id": claim.event_id,
                    "token": claim.claim_token,
                    "status": (
                        "PENDING" if retry else "FAILED"
                    ),
                    "retry_at": retry_at,
                    "error": error_code,
                },
            ).rowcount

        if not row_count:
            return ProcessResult(
                claim.event_id,
                "STALE",
            )

        return ProcessResult(
            claim.event_id,
            "RETRY" if retry else "FAILED",
        )
