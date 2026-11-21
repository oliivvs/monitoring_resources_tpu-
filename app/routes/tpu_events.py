
"""Oracle Sync Push: принимаем событие только ПОСЛЕ долговременного сохранения.

На этом этапе события остаются PENDING. Применение ресурсов выполняет будущий
Inbox Worker, который будет согласован с EntityMapper Никиты.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from starlette.concurrency import run_in_threadpool

from app.database import get_engine
from app.security_tokens import require_tpu_sync_token
from db.session import write_transaction
from integrations.tpu.oracle_models import (
    OPERATION_NAMES,
    OracleSyncEvent,
    OracleSyncReceipt,
)
from shared.schemas import to_utc_iso

MAX_BODY_BYTES = 64 * 1024  # защита входящего API; отдельно от 16-КБ лимита ОКО
router = APIRouter(prefix="/api/v1/integrations/tpu", tags=["Oracle Sync"])


def _reject_non_json_constant(value: str):
    raise ValueError(f"Недопустимая константа JSON: {value}")


def _persist_inbox_event(engine: Engine, event: OracleSyncEvent, raw: str, digest: str) -> bool:
    """True = новое событие, False = точно такой же повтор.

    BEGIN IMMEDIATE сериализует конкурирующих писателей SQLite, поэтому проверка
    event_id и INSERT атомарны. Никакого HTTP внутри транзакции.
    """
    with write_transaction(engine) as conn:
        previous = conn.execute(
            text("SELECT payload_hash FROM inbox_events WHERE event_id=:id"),
            {"id": str(event.event_id)},
        ).scalar_one_or_none()

        if previous is not None:
            if previous != digest:
                raise HTTPException(
                    status_code=409,
                    detail="event_id уже существует с другим содержимым",
                )
            return False

        conn.execute(
            text("""
                INSERT INTO inbox_events (
                    event_id, source_system, source_entity, source_record_id,
                    operation, source_sequence, occurred_at, payload_json,
                    payload_hash, received_at, processing_status, attempts
                ) VALUES (
                    :event_id, :source, :entity, :record_id, :operation,
                    :sequence, :occurred_at, :payload, :hash,
                    :received_at, 'PENDING', 0
                )
            """),
            {
                "event_id": str(event.event_id),
                "source": event.source,
                "entity": event.entity,
                "record_id": event.record_id,
                "operation": OPERATION_NAMES[event.operation_code],
                "sequence": event.source_sequence,
                "occurred_at": to_utc_iso(event.occurred_at),
                "payload": raw,
                "hash": digest,
                "received_at": to_utc_iso(datetime.now(timezone.utc)),
            },
        )
        return True


@router.post(
    "/resource-events",
    status_code=202,
    response_model=OracleSyncReceipt,
    summary="Принять тестовое событие Oracle Sync (TPU_SYNC_TOKEN)",
    dependencies=[Depends(require_tpu_sync_token)],
    responses={
        200: {"description": "Тот же event_id и содержимое (duplicate)"},
        401: {"description": "Отсутствует или неверен TPU_SYNC_TOKEN"},
        409: {"description": "Тот же event_id с другим содержимым"},
        413: {"description": "Слишком большой JSON"},
        415: {"description": "Нужен Content-Type: application/json"},
        422: {"description": "JSON не соответствует Pydantic-схеме"},
        500: {"description": "Ошибка записи события в Inbox"},
    },
)
async def receive_tpu_resource_event(
    request: Request,
    engine: Engine = Depends(get_engine),
) -> JSONResponse:
    media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if media_type != "application/json":
        raise HTTPException(status_code=415, detail="Ожидается application/json")

    size = 0
    chunks: list[bytes] = []
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            raise HTTPException(status_code=413, detail="Превышен размер JSON (64 КБ)")
        chunks.append(chunk)
    try:
        raw = b"".join(chunks).decode("utf-8")
        parsed = json.loads(raw, parse_constant=_reject_non_json_constant)
        event = OracleSyncEvent.model_validate(parsed)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, ValueError):
        raise HTTPException(status_code=422, detail="Некорректное событие Oracle Sync") from None

    # Порядок ключей JSON и отсутствие необязательного null не влияют на hash.
    canonical = json.dumps(
        event.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    try:
        accepted = await run_in_threadpool(_persist_inbox_event, engine, event, raw, digest)
    except HTTPException:
        raise
    except SQLAlchemyError:
        # Не публикуем текст SQL-ошибки или исходные данные в ответе/логе.
        raise HTTPException(status_code=500, detail="Не удалось сохранить событие") from None

    result = OracleSyncReceipt(
        event_id=event.event_id,
        result="accepted" if accepted else "duplicate",
    )
    return JSONResponse(status_code=202 if accepted else 200, content=result.model_dump(mode="json"))
