"""Сохранение проверки, статус и outbox выполняются одной транзакцией."""
import logging

logger = logging.getLogger(__name__)
from sqlalchemy import text
from sqlalchemy.engine import Connection

from services.events import record_status_change
from shared.schemas import (
    CheckResultIn, CheckType, ResourceStatus, StatusEventContext,
    from_utc_iso, to_utc_iso,
)


class StaleConfigurationResult(ValueError):
    """Запрос выполнен по версии, более не действующей для данного ресурса."""


def apply_check_result(conn: Connection, *, result: CheckResultIn) -> dict:
    """Требует write_transaction; не выполняет commit/сетевых обращений."""
    obj = conn.execute(text("""
        SELECT r.name,r.active,c.id AS config_id,c.check_type,c.up_after,c.down_after,
               s.current_status,s.ok_count,s.fail_count,s.last_checked_at
        FROM resources r JOIN resource_states s ON r.id=s.resource_id
        JOIN resource_config_versions c ON c.id=s.config_version_id
        WHERE r.id=:rid AND c.retired_at IS NULL
    """), {"rid":result.resource_id}).mappings().first()
    if not obj:
        raise LookupError("Ресурс не найден")
    # Проверять под BEGIN IMMEDIATE и ДО любых INSERT/UPDATE.
    # Это исключает гонку "снимок v1 -> смена конфигурации -> ответ v1".
    if result.config_version_id != obj["config_id"]:
        logger.info(
            "Отброшен результат устаревшей конфигурации: resource_id=%s, "
            "check_config_version_id=%s, active_config_version_id=%s",
            result.resource_id, result.config_version_id, obj["config_id"],
        )
        raise StaleConfigurationResult(
            f"Устаревшая версия конфигурации ресурса {result.resource_id}: "
            f"проверка={result.config_version_id}, активная={obj['config_id']}"
        )
    if not obj["active"]:
        raise ValueError("Проверки выключенного ресурса запрещены")
    at = to_utc_iso(result.checked_at)
    if obj["last_checked_at"] is not None and at <= obj["last_checked_at"]:
        raise ValueError("Повторная или устаревшая проверка")
    if obj["check_type"] == CheckType.TCP and result.http_status_code is not None:
        raise ValueError("TCP не возвращает HTTP-код")
    if result.success and result.error_message:
        raise ValueError("Успешная проверка не должна содержать ошибку")
    if result.success and obj["check_type"] == CheckType.HTTP:
        if result.http_status_code is None:
            raise ValueError("Успешная HTTP-проверка должна иметь HTTP-код")
        allowed = conn.execute(text("""
            SELECT 1 FROM http_expected_ranges
            WHERE config_version_id=:version AND :code BETWEEN code_start AND code_end LIMIT 1
        """), {"version":result.config_version_id,"code":result.http_status_code}).first()
        if not allowed:
            raise ValueError("HTTP-код не допускается конфигурацией")

    old = ResourceStatus(obj["current_status"])
    ok_count = (obj["ok_count"] + 1) if result.success else 0
    fail_count = 0 if result.success else (obj["fail_count"] + 1)
    new = old
    if result.success and ok_count >= obj["up_after"]:
        new = ResourceStatus.UP
    elif not result.success and fail_count >= obj["down_after"]:
        new = ResourceStatus.DOWN
    changed = new != old

    check_id = conn.execute(text("""
       INSERT INTO check_results
         (resource_id,config_version_id,checked_at,success,response_time_ms,
          http_status_code,error_message,resulting_status)
       VALUES (:rid,:version,:at,:success,:ms,:http,:error,:status) RETURNING id
    """), {"rid":result.resource_id,"version":result.config_version_id,"at":at,
           "success":int(result.success),"ms":result.response_time_ms,
           "http":result.http_status_code,"error":result.error_message,
           "status":new.value}).scalar_one()
    conn.execute(text("""
       UPDATE resource_states
       SET current_status=:status,ok_count=:ok,fail_count=:fail,last_checked_at=:at,
           status_changed_at=CASE WHEN :changed=1 THEN :at ELSE status_changed_at END,
           updated_at=:at,revision=revision+1
       WHERE resource_id=:rid
    """), {"rid":result.resource_id,"status":new.value,"ok":ok_count,
           "fail":fail_count,"at":at,"changed":int(changed)})

    event_id = None
    if changed:
        context = StatusEventContext(
            resource_id=result.resource_id,resource_name=obj["name"],
            status=new, check_type=obj["check_type"], checked_at=result.checked_at,
            response_time_ms=result.response_time_ms,error=result.error_message,
        )
        event_id = record_status_change(
            conn, previous_status=old,context=context,check_result_id=check_id
        )
    return {"check_id":check_id,"previous_status":old.value,
            "current_status":new.value,"status_changed":changed,
            "event_id":str(event_id) if event_id else None}
