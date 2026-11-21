
"""A21/A22: надёжная обработка событий Oracle Sync Inbox.

Синхронный режим предназначен для обработчиков SQLite.

Асинхронный режим разделяет обработку на этапы:
1. Захват события.
2. Подготовка и сетевая проверка БЕЗ транзакции.
3. Атомарное применение к SQLite и отметка PROCESSED.

Нельзя выполнять HTTP, DNS или commit внутри обработчика SQLite.
"""

from __future__ import annotations

import asyncio

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Literal
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from db.session import write_transaction
from shared.schemas import to_utc_iso


class PermanentInboxError(ValueError):
    """Ошибка, которую повторная обработка не исправит."""


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

InboxPreparer = Callable[
    [InboxClaim],
    Awaitable[Any],
]

InboxPreparedHandler = Callable[
    [Connection, InboxClaim, Any],
    None,
]


def _clock(now: datetime | None) -> datetime:
    value = (
        now
        if now is not None
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


def _retry_limit(retry_base_seconds: int) -> None:
    if (
        type(retry_base_seconds) is not int
        or not 1 <= retry_base_seconds <= 3600
    ):
        raise ValueError(
            "retry_base_seconds должен быть от 1 до 3600"
        )


def claim_inbox_event(
    engine: Engine,
    *,
    now: datetime | None = None,
    lease_seconds: int = 60,
    max_attempts: int = 5,
) -> InboxClaim | None:
    """Атомарно захватывает готовое событие."""

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


def _apply_claim(
    engine: Engine,
    claim: InboxClaim,
    handler: InboxHandler,
    *,
    now: datetime | None = None,
    check_lease: bool = False,
) -> ProcessResult:
    """Атомарно применяет событие и помечает PROCESSED."""

    with write_transaction(engine) as conn:
        timestamp = _clock(now)
        params = {
            "id": claim.event_id,
            "token": claim.claim_token,
            "at": to_utc_iso(timestamp),
        }

        lease_condition = (
            "AND lease_expires_at > :at"
            if check_lease
            else ""
        )

        owned = conn.execute(
            text(f"""
                SELECT 1
                FROM inbox_events
                WHERE event_id=:id
                  AND claim_token=:token
                  AND processing_status='PROCESSING'
                  {lease_condition}
            """),
            params,
        ).scalar_one_or_none()

        if owned is None:
            return ProcessResult(
                claim.event_id,
                "STALE",
            )

        handler(conn, claim)

        changed = conn.execute(
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
            params,
        ).rowcount

        if changed != 1:
            raise RuntimeError(
                "Не удалось завершить захват Inbox"
            )

    return ProcessResult(
        claim.event_id,
        "PROCESSED",
    )


def _fail_claim(
    engine: Engine,
    claim: InboxClaim,
    error_code: str,
    *,
    now: datetime | None = None,
    max_attempts: int = 5,
    retry_base_seconds: int = 5,
    check_lease: bool = False,
    permanent: bool = False,
) -> ProcessResult:
    """Освобождает захват: RETRY или окончательный FAILED."""

    timestamp = _clock(now)
    retry = not permanent and claim.attempts < max_attempts

    delay = min(
        retry_base_seconds * 2 ** (claim.attempts - 1),
        3600,
    )

    retry_at = (
        to_utc_iso(
            timestamp + timedelta(seconds=delay)
        )
        if retry
        else None
    )

    params = {
        "id": claim.event_id,
        "token": claim.claim_token,
        "status": "PENDING" if retry else "FAILED",
        "retry_at": retry_at,
        "error": error_code[:100],
        "at": to_utc_iso(timestamp),
    }

    lease_condition = (
        "AND lease_expires_at > :at"
        if check_lease
        else ""
    )

    with write_transaction(engine) as conn:
        changed = conn.execute(
            text(f"""
                UPDATE inbox_events
                SET processing_status=:status,
                    claim_token=NULL,
                    lease_expires_at=NULL,
                    next_attempt_at=:retry_at,
                    last_error=:error
                WHERE event_id=:id
                  AND claim_token=:token
                  AND processing_status='PROCESSING'
                  {lease_condition}
            """),
            params,
        ).rowcount

    if changed != 1:
        return ProcessResult(
            claim.event_id,
            "STALE",
        )

    return ProcessResult(
        claim.event_id,
        "RETRY" if retry else "FAILED",
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
    """Существующий синхронный режим A21."""

    if not callable(handler):
        raise ValueError(
            "Требуется реальный обработчик события"
        )

    _retry_limit(retry_base_seconds)

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
        return _apply_claim(
            engine,
            claim,
            handler,
            now=now,
        )

    except Exception as exc:
        return _fail_claim(
            engine,
            claim,
            type(exc).__name__,
            now=now,
            permanent=isinstance(exc, PermanentInboxError),
            max_attempts=max_attempts,
            retry_base_seconds=retry_base_seconds,
        )


async def process_one_inbox_event_async(
    engine: Engine,
    prepare: InboxPreparer,
    handler: InboxPreparedHandler,
    *,
    now: datetime | None = None,
    lease_seconds: int = 60,
    max_attempts: int = 5,
    retry_base_seconds: int = 5,
) -> ProcessResult | None:
    """Новый двухэтапный режим Oracle Sync.

    prepare(claim):
        Асинхронная подготовка без write_transaction.

    handler(conn, claim, prepared):
        Только операции SQLite. Запускается внутри
        write_transaction вместе с отметкой PROCESSED.

    Если подготовка не удалась, событие возвращается
    в PENDING или получает статус FAILED.
    """

    if not callable(prepare) or not callable(handler):
        raise ValueError(
            "Нужны функции подготовки и применения"
        )

    _limits(
        lease_seconds=lease_seconds,
        max_attempts=max_attempts,
    )
    _retry_limit(retry_base_seconds)

    claim = await asyncio.to_thread(
        claim_inbox_event,
        engine,
        now=now,
        lease_seconds=lease_seconds,
        max_attempts=max_attempts,
    )

    if claim is None:
        return None

    try:
        # ВАЖНО: здесь транзакция SQLite не открыта.
        prepared = await prepare(claim)

        def apply_inside_transaction(
            conn: Connection,
            selected: InboxClaim,
        ) -> None:
            handler(conn, selected, prepared)

        # Проверяем lease перед применением.
        return await asyncio.to_thread(
            _apply_claim,
            engine,
            claim,
            apply_inside_transaction,
            now=now,
            check_lease=True,
        )

    except Exception as exc:
        # Сохраняем только тип исключения,
        # не раскрывая адреса и содержимое события.
        return await asyncio.to_thread(
            _fail_claim,
            engine,
            claim,
            type(exc).__name__,
            now=now,
            permanent=isinstance(exc, PermanentInboxError),
            max_attempts=max_attempts,
            retry_base_seconds=retry_base_seconds,
            check_lease=True,
        )
