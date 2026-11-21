
"""Наглядная локальная демонстрация первого этапа Артемия без доступа к ТПУ.

Запуск из корня репозитория: python -m scripts.demo_artemiy
Использует ТОЛЬКО временную SQLite. Секреты тестовые и не печатаются.
"""
from __future__ import annotations

import importlib
import secrets
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.config import AppSettings
from app.database import get_engine, get_read_connection
from app.main import app
from app.security_tokens import get_settings
from db.session import initialize, make_engine, read_transaction, write_transaction

READ = secrets.token_urlsafe(32)
SYNC = secrets.token_urlsafe(32)


def show(name: str, actual: int, expected: int) -> bool:
    passed = actual == expected
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{stamp}] {'PASS' if passed else 'FAIL'} {name}: HTTP {actual}, ожидали {expected}")
    return passed


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="tpu-artemiy-demo-") as directory:
        engine = make_engine(Path(directory) / "demo.sqlite")
        initialize(engine)
        migration = importlib.import_module("migrations.versions.003_tpu_inbox")
        with engine.begin() as conn:
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
        at = datetime(2026, 11, 13, 6, 15, tzinfo=timezone.utc).isoformat()
        with write_transaction(engine) as conn:
            conn.execute(text("""
                INSERT INTO resources(id,name,active,created_at,updated_at)
                VALUES ('web-local','Локальный HTTP-сервер',1,:at,:at)
            """),{"at":at})
            version=conn.execute(text("""
                INSERT INTO resource_config_versions (
                    resource_id,version_number,target,check_type,port,
                    interval_seconds,timeout_seconds,up_after,down_after,
                    heartbeat_interval_seconds,effective_at
                ) VALUES ('web-local',1,'http://127.0.0.1:18080/health',
                          'HTTP',NULL,60,5,2,3,300,:at) RETURNING id
            """), {"at":at}).scalar_one()
            conn.execute(text("""
                INSERT INTO resource_states(resource_id,config_version_id,current_status,updated_at)
                VALUES ('web-local',:version,'UNKNOWN',:at)
            """),{"version":version,"at":at})

        def override_read():
            with read_transaction(engine) as conn:
                yield conn

        app.dependency_overrides[get_engine]=lambda:engine
        app.dependency_overrides[get_read_connection]=override_read
        app.dependency_overrides[get_settings]=lambda:AppSettings(
            _env_file=None, read_token=READ, tpu_sync_token=SYNC
        )
        ok=[]
        try:
            with TestClient(app) as client:
                get_url="/api/v1/statuses"
                post_url="/api/v1/integrations/tpu/resource-events"
                payload={
                    "event_id":str(uuid4()),"source":"tpu-oracle-sync",
                    "entity":"servers","record_id":"tpu-demo-001",
                    "operation_code":3,"source_sequence":1042,
                    "occurred_at":"2026-11-13T06:15:00Z",
                    "data":{"name":"Демонстрационный сервер","active":True},
                }
                ok.append(show("GET без токена",client.get(get_url).status_code,401))
                read=client.get(get_url,headers={"Authorization":f"Bearer {READ}"})
                ok.append(show("GET с READ_TOKEN",read.status_code,200))
                print("    GET items:",read.json()["items"])
                ok.append(show("Oracle POST без токена",client.post(post_url,json=payload).status_code,401))
                headers={"Authorization":f"Bearer {SYNC}"}
                first=client.post(post_url,json=payload,headers=headers)
                ok.append(show("Новое событие → Inbox",first.status_code,202))
                again=client.post(post_url,json=payload,headers=headers)
                ok.append(show("Повтор event_id/payload",again.status_code,200))
                conflict=client.post(post_url,json={**payload,"data":{"name":"Изменено"}},headers=headers)
                ok.append(show("Повтор ID с другим JSON",conflict.status_code,409))
                invalid=client.post(post_url,json={**payload,"operation_code":9},headers=headers)
                ok.append(show("Неверная схема",invalid.status_code,422))
            with read_transaction(engine) as conn:
                count=conn.execute(text("SELECT COUNT(*) FROM inbox_events")).scalar_one()
                status=conn.execute(text("SELECT processing_status FROM inbox_events")).scalar_one()
            print(f"    SQLite: inbox_events={count}, processing_status={status}")
            ok.append(count==1 and status=="PENDING")
        finally:
            app.dependency_overrides.clear()
            engine.dispose()
        print(f"\nРезультат демонстрации: {sum(ok)}/{len(ok)} проверок PASS")
        print("Это только приём Inbox: применение UPDATE выполняет будущий Inbox Worker.")
        return 0 if all(ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
