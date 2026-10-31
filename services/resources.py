"""Управление ресурсами: одна активная версия конфигурации."""
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.engine import Connection

from shared.schemas import (
    ResourceConfig, CheckSnapshot, ExpectedHttpRange, CheckType, to_utc_iso,
)


def upsert_configuration(conn: Connection, config: ResourceConfig, *, now: datetime | None = None) -> int:
    """Добавляет ресурс или создаёт новую версию конфигурации.

    Повторная загрузка тех же параметров не меняет версию или статус.
    Изменение target/check_type/HTTP-кодов требует нового ID ресурса:
    это новая проверяемая служба, а не изменение расписания.
    Пороговые/временные параметры версионируются, счётчики обнуляются,
    подтверждённый последний статус остаётся до новой проверки.
    История прежних конфигураций и проверок сохраняется.
    Вызывается внутри write_transaction(); commit здесь запрещён.
    """
    now_s = to_utc_iso(now or datetime.now(timezone.utc))
    row = conn.execute(text("SELECT id, active FROM resources WHERE id=:id"), {"id": config.id}).mappings().first()
    if not row:
        conn.execute(text("""INSERT INTO resources (id,name,active,created_at,updated_at)
                                 VALUES (:id,:name,:active,:at,:at)"""),
                     {"id":config.id,"name":config.name,"active":int(config.active),"at":now_s})
        version = 1
    else:
        existing = conn.execute(text("""
            SELECT id,version_number,target,check_type,port,interval_seconds,
                   timeout_seconds,up_after,down_after,heartbeat_interval_seconds,effective_at
            FROM resource_config_versions WHERE resource_id=:rid AND retired_at IS NULL
        """), {"rid":config.id}).mappings().one()
        ranges = list(conn.execute(text("""
            SELECT code_start,code_end FROM http_expected_ranges
            WHERE config_version_id=:cid ORDER BY code_start,code_end
        """), {"cid":existing["id"]}))
        expected_ranges = sorted((r.start, r.end) for r in config.expected_http_codes)
        if (existing["target"] != config.target or existing["check_type"] != config.check_type.value
            or existing["port"] != config.port or ranges != expected_ranges):
            raise ValueError("Изменение цели или критерия проверки требует нового resource_id; "
                             "оно меняет идентичность мониторируемой службы")
        conn.execute(text("UPDATE resources SET name=:name, active=:active, updated_at=:at WHERE id=:id"),
                     {"id":config.id,"name":config.name,"active":int(config.active),"at":now_s})
        changes = any((
            bool(row["active"]) != config.active,
            existing["interval_seconds"] != config.interval_seconds,
            existing["timeout_seconds"] != config.timeout_seconds,
            existing["up_after"] != config.up_after,
            existing["down_after"] != config.down_after,
            existing["heartbeat_interval_seconds"] != config.heartbeat_interval_seconds,
        ))
        if not changes:
            # Идемпотентная загрузка файла: перезапуск не сбрасывает статус.
            return existing["id"]
        if now_s <= existing["effective_at"]:
            raise ValueError("Новая версия должна иметь время позже предыдущей")
        version = existing["version_number"] + 1
        conn.execute(text("UPDATE resource_config_versions SET retired_at=:at WHERE id=:id"),
                     {"at":now_s,"id":existing["id"]})

    v_id = conn.execute(text("""
        INSERT INTO resource_config_versions (
          resource_id,version_number,target,check_type,port,
          interval_seconds,timeout_seconds,up_after,down_after,heartbeat_interval_seconds,effective_at)
        VALUES (:id,:version,:target,:type,:port,:interval,:timeout,:up_after,:down_after,:heartbeat,:at)
        RETURNING id
        """), {"id":config.id,"version":version,"target":config.target,
              "type":config.check_type.value,"port":config.port,
              "interval":config.interval_seconds,"timeout":config.timeout_seconds,
              "up_after":config.up_after,"down_after":config.down_after,
              "heartbeat":config.heartbeat_interval_seconds,"at":now_s}).scalar_one()

    for r in config.expected_http_codes:
        conn.execute(text("INSERT INTO http_expected_ranges (config_version_id,code_start,code_end) VALUES (:id,:a,:b)"),
                     {"id":v_id,"a":r.start,"b":r.end})

    conn.execute(text("""
        INSERT INTO resource_states (resource_id,config_version_id,updated_at)
        VALUES (:id,:config_id,:at)
        ON CONFLICT(resource_id) DO UPDATE SET
            config_version_id=excluded.config_version_id,
            ok_count=0,fail_count=0,
            revision=resource_states.revision+1, updated_at=excluded.updated_at
        """), {"id":config.id,"config_id":v_id,"at":now_s})
    return v_id


def get_active_check_snapshot(conn: Connection, *, resource_id: str) -> CheckSnapshot:
    """Снимок конфигурации до сетевого запроса; вызывается внутри read_transaction.

    Конфигурация и диапазоны HTTP считываются из одной согласованной read-транзакции.
    config_version_id этого снимка обязательно переносится в CheckResultIn.
    """
    row = conn.execute(text("""
        SELECT r.active, c.id AS config_version_id, c.target, c.check_type,
               c.port, c.interval_seconds, c.timeout_seconds
        FROM resources r
        JOIN resource_states s ON s.resource_id=r.id
        JOIN resource_config_versions c ON c.id=s.config_version_id
        WHERE r.id=:rid AND c.retired_at IS NULL
    """), {"rid":resource_id}).mappings().first()
    if row is None:
        raise LookupError("Ресурс не найден или не имеет активной конфигурации")
    if not row["active"]:
        raise ValueError("Проверки выключенного ресурса запрещены")
    ranges = conn.execute(text("""
        SELECT code_start, code_end FROM http_expected_ranges
        WHERE config_version_id=:version ORDER BY code_start,code_end
    """), {"version":row["config_version_id"]}).all()
    return CheckSnapshot(
        resource_id=resource_id,
        config_version_id=row["config_version_id"],
        target=row["target"], check_type=CheckType(row["check_type"]),
        port=row["port"], interval_seconds=row["interval_seconds"],
        timeout_seconds=row["timeout_seconds"],
        expected_http_codes=[ExpectedHttpRange(start=a,end=b) for a,b in ranges],
    )
