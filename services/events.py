"""Создание STATUS_CHANGED / HEARTBEAT и управление очередью доставки."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import Connection

from shared.schemas import (
    EventType, ResourceStatus, StatusEventContext, StatusEventPayload,
    from_utc_iso, to_utc_iso,
)


def _payload(context: StatusEventContext, kind: EventType) -> tuple[str, str]:
    eid = uuid4()
    data = StatusEventPayload(**context.model_dump(), event_id=eid, event_type=kind)
    json_data = data.model_dump(mode="json")
    json_data["checked_at"] = to_utc_iso(context.checked_at)
    return str(eid), json.dumps(json_data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def record_status_change(
    conn: Connection, *, previous_status: ResourceStatus,
    context: StatusEventContext, check_result_id: int,
) -> UUID | None:
    """Вызвать из транзакции обработчика проверки; НЕ делает commit и HTTP.

    Одна смена статуса создаёт один переход и один неизменяемый JSON-снимок.
    """
    if previous_status == context.status:
        return None
    at = to_utc_iso(context.checked_at)
    tid = conn.execute(text("""
        INSERT INTO status_transitions
          (resource_id, check_result_id, previous_status, new_status, changed_at)
        VALUES (:rid,:cid,:old,:new,:at) RETURNING id
    """), {"rid":context.resource_id,"cid":check_result_id,"old":previous_status.value,
           "new":context.status.value,"at":at}).scalar_one()
    eid, payload = _payload(context, EventType.STATUS_CHANGED)
    conn.execute(text("""
        INSERT INTO outbox_events
          (event_id,resource_id,status_transition_id,event_type,status,checked_at,payload_json,
           delivery_status,attempts,next_attempt_at,created_at)
        VALUES (:eid,:rid,:transition,'STATUS_CHANGED',:status,:at,:payload,'PENDING',0,:at,:at)
    """), {"eid":eid,"rid":context.resource_id,"transition":tid,
           "status":context.status.value,"at":at,"payload":payload})
    return UUID(eid)


def enqueue_heartbeat_if_due(conn: Connection, *, resource_id: str,
                             now: datetime | None = None) -> UUID | None:
    """HEARTBEAT не чаще установленного интервала; сериализовано BEGIN IMMEDIATE."""
    now = now or datetime.now(timezone.utc)
    now_s = to_utc_iso(now)
    state = conn.execute(text("""
        SELECT r.id, r.name, r.active, s.current_status, s.last_heartbeat_enqueued_at,
               s.status_changed_at, c.check_type, c.heartbeat_interval_seconds
        FROM resources r
        JOIN resource_states s ON s.resource_id=r.id
        JOIN resource_config_versions c ON c.id=s.config_version_id
        WHERE r.id=:rid AND c.retired_at IS NULL
    """), {"rid":resource_id}).mappings().first()
    if not state or not state["active"]:
        return None
    last_check = conn.execute(text("""
       SELECT checked_at,response_time_ms,error_message
       FROM check_results WHERE resource_id=:rid
         AND config_version_id=(SELECT config_version_id FROM resource_states WHERE resource_id=:rid)
       ORDER BY checked_at DESC,id DESC LIMIT 1
    """), {"rid":resource_id}).mappings().first()
    if not last_check:
        return None
    last_event_at = state["last_heartbeat_enqueued_at"] or state["status_changed_at"] or last_check["checked_at"]
    if now < from_utc_iso(last_event_at) + timedelta(seconds=state["heartbeat_interval_seconds"]):
        return None
    context = StatusEventContext(
        resource_id=resource_id, resource_name=state["name"],
        status=ResourceStatus(state["current_status"]), check_type=state["check_type"],
        checked_at=from_utc_iso(last_check["checked_at"]),
        response_time_ms=last_check["response_time_ms"], error=last_check["error_message"],
    )
    eid, payload = _payload(context, EventType.HEARTBEAT)
    conn.execute(text("""
        INSERT INTO outbox_events
          (event_id,resource_id,event_type,status,checked_at,payload_json,
           delivery_status,attempts,next_attempt_at,created_at)
        VALUES (:eid,:rid,'HEARTBEAT',:status,:checked,:payload,'PENDING',0,:created,:created)
    """), {"eid":eid,"rid":resource_id,"status":context.status.value,
           "checked":to_utc_iso(context.checked_at),"payload":payload,"created":now_s})
    conn.execute(text("""
        UPDATE resource_states SET last_heartbeat_enqueued_at=:at,revision=revision+1,
          updated_at=:at WHERE resource_id=:rid
    """), {"at":now_s,"rid":resource_id})
    return UUID(eid)


def claim_one_event(conn: Connection, *, worker_id: str, now: datetime | None = None,
                    lease_seconds: int = 60) -> dict | None:
    """Одна атомарная операция резервирования; внешний POST выполняется ПОСЛЕ commit."""
    if not worker_id or lease_seconds <= 0:
        raise ValueError("Некорректный идентификатор воркера или срок аренды")
    now = now or datetime.now(timezone.utc)
    at = to_utc_iso(now)
    until = to_utc_iso(now + timedelta(seconds=lease_seconds))
    row = conn.execute(text("""
        UPDATE outbox_events
        SET lease_owner=:worker,lease_until=:until
        WHERE event_id = (
          SELECT event_id FROM outbox_events
          WHERE delivery_status='PENDING'
            AND (next_attempt_at IS NULL OR next_attempt_at<=:at)
            AND (lease_until IS NULL OR lease_until<=:at)
          ORDER BY created_at,event_id LIMIT 1
        )
        RETURNING event_id,payload_json,attempts,lease_until
    """), {"worker":worker_id,"until":until,"at":at}).mappings().first()
    return dict(row) if row else None


def finish_delivery(
    conn: Connection, *, event_id: str, worker_id: str,
    http_status_code: int | None, error_message: str | None = None,
    retry_after_seconds: int | None = None, now: datetime | None = None,
) -> str:
    """Записывает ровно один результат завершённой попытки и обновляет outbox.

    Должна выполняться внутри write_transaction. Не принимает токены/секреты.
    """
    now = now or datetime.now(timezone.utc)
    at = to_utc_iso(now)
    row = conn.execute(text("""
      SELECT attempts,delivery_status,lease_owner,lease_until
      FROM outbox_events WHERE event_id=:eid
    """), {"eid":event_id}).mappings().first()
    if not row or row["delivery_status"] != "PENDING" or row["lease_owner"] != worker_id:
        raise ValueError("Событие не зарезервировано этим воркером")
    if row["lease_until"] < at:
        raise ValueError("Резервирование события истекло")
    attempt_no = row["attempts"] + 1
    if http_status_code in (200, 202):
        outcome, status, next_at, delivered = "SENT", "SENT", None, at
    elif http_status_code is None or http_status_code == 429 or 500 <= http_status_code <= 599:
        outcome, status, delivered = "RETRY", "PENDING", None
        seconds = retry_after_seconds if http_status_code == 429 and retry_after_seconds is not None and retry_after_seconds >= 0 else min(2 ** min(attempt_no, 8), 300)
        next_at = to_utc_iso(now + timedelta(seconds=seconds))
    else:
        outcome, status, next_at, delivered = "FAILED", "FAILED", None, None

    conn.execute(text("""
        INSERT INTO delivery_attempts
          (event_id,attempt_number,attempted_at,http_status_code,outcome,error_message)
        VALUES (:eid,:num,:at,:http,:outcome,:error)
    """), {"eid":event_id,"num":attempt_no,"at":at,"http":http_status_code,
           "outcome":outcome,"error":error_message})
    conn.execute(text("""
        UPDATE outbox_events SET attempts=:num,delivery_status=:status,
          next_attempt_at=:next_at, delivered_at=:delivered,
          last_error=:error,lease_owner=NULL,lease_until=NULL
        WHERE event_id=:eid
    """), {"eid":event_id,"num":attempt_no,"status":status,
           "next_at":next_at,"delivered":delivered,"error":error_message})
    return status
