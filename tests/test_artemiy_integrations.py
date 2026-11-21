
"""Тесты: GET /statuses + Oracle Sync Inbox.

Все записи выполняются ТОЛЬКО в отдельную временную SQLite; боевые адреса и
токены не используются. На пустой базе сначала проверяется миграция 003.
"""
from __future__ import annotations

import importlib
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.database import get_engine, get_read_connection
from app.main import app
from app.security_tokens import get_settings
from app.config import AppSettings
from db.session import initialize, make_engine, read_transaction, write_transaction

READ_TOKEN = "demo-read-token"
SYNC_TOKEN = "demo-oracle-token"
BASE = datetime(2026, 11, 13, 6, 15, tzinfo=timezone.utc)


@pytest.fixture
def api(tmp_path):
    engine = make_engine(tmp_path / "integration.sqlite")
    # Сначала — реальная схема текущего проекта, затем аддитивная миграция 003.
    initialize(engine)
    migration = importlib.import_module("migrations.versions.003_tpu_inbox")
    with engine.begin() as conn:
        context = MigrationContext.configure(conn)
        with Operations.context(context):
            migration.upgrade()

    def override_read():
        with read_transaction(engine) as conn:
            yield conn

    app.dependency_overrides[get_read_connection] = override_read
    app.dependency_overrides[get_engine] = lambda: engine
    app.dependency_overrides[get_settings] = lambda: AppSettings(
        _env_file=None,
        read_token=READ_TOKEN,
        tpu_sync_token=SYNC_TOKEN,
        event_token="another-token-for-oko",
    )
    try:
        with TestClient(app) as client:
            yield client, engine
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


def auth(token):
    return {"Authorization": f"Bearer {token}"}


def event_payload(**changes):
    data = {
        "event_id": str(uuid4()),
        "source": "tpu-oracle-sync",
        "entity": "servers",
        "record_id": "tpu-demo-001",
        "operation_code": 3,
        "source_sequence": 1042,
        "occurred_at": "2026-11-13T06:15:00Z",
        "data": {"name": "Демо", "active": True},
    }
    data.update(changes)
    return data


def seed_resource(engine, key, *, active=True, status="UNKNOWN"):
    # Заполняем существующую нормализованную схему, не меняя её контракт.
    with write_transaction(engine) as conn:
        conn.execute(text("""
            INSERT INTO resources (id,name,active,created_at,updated_at)
            VALUES (:id,:name,:active,:ts,:ts)
        """), {"id":key, "name":f"Resource {key}", "active":int(active), "ts":BASE.isoformat()})
        v = conn.execute(text("""
            INSERT INTO resource_config_versions (
                resource_id,version_number,target,check_type,port,interval_seconds,
                timeout_seconds,up_after,down_after,heartbeat_interval_seconds,effective_at
            ) VALUES (
                :id,1,'https://example.org/health','HTTP',NULL,60,5,2,3,300,:ts
            ) RETURNING id
        """), {"id":key, "ts":BASE.isoformat()}).scalar_one()
        conn.execute(text("""
            INSERT INTO resource_states (resource_id,config_version_id,current_status,updated_at)
            VALUES (:id,:version,:status,:ts)
        """), {"id":key, "version":v, "status":status, "ts":BASE.isoformat()})


def test_swagger_paths_are_published(api):
    client, _ = api
    paths=client.get("/openapi.json").json()["paths"]
    assert "/api/v1/statuses" in paths
    assert "/api/v1/integrations/tpu/resource-events" in paths


def test_get_read_requires_own_token_and_no_targets(api):
    client, engine = api
    seed_resource(engine, "web-a", status="DOWN")
    assert client.get("/api/v1/statuses").status_code == 401
    assert client.get("/api/v1/statuses", headers=auth(SYNC_TOKEN)).status_code == 401
    r=client.get("/api/v1/statuses", headers=auth(READ_TOKEN))
    assert r.status_code == 200, r.text
    body=r.json()
    assert body["limit"] == 50 and body["offset"] == 0 and not body["has_more"]
    assert body["items"] == [{
        "resource_id":"web-a", "resource_name":"Resource web-a", "check_type":"HTTP",
        "status":"DOWN", "active":True, "checked_at":None,"status_changed_at":None,
    }]
    for secret in ("target","error", "127.0.0.1", READ_TOKEN, SYNC_TOKEN):
        assert secret not in r.text


def test_get_pagination_and_timestamps(api):
    client, engine=api
    for i in ('b','a','c'): seed_resource(engine, f"web-{i}", active=i!='b')
    with write_transaction(engine) as conn:
        conn.execute(text("""
            UPDATE resource_states
            SET last_checked_at=:checked,status_changed_at=:changed,current_status='UP'
            WHERE resource_id='web-a'
        """),{"checked":BASE.isoformat(),"changed":(BASE-timedelta(minutes=5)).isoformat()})
    first=client.get("/api/v1/statuses?limit=2",headers=auth(READ_TOKEN)).json()
    assert [r['resource_id'] for r in first['items']]==['web-a','web-b']
    assert first['has_more'] is True and first['items'][0]['checked_at'] is not None
    assert first['items'][1]['active'] is False
    second=client.get("/api/v1/statuses?offset=2&limit=2",headers=auth(READ_TOKEN)).json()
    assert [r['resource_id'] for r in second['items']]==['web-c']
    assert second['has_more'] is False
    for query in ('limit=0','limit=101','offset=-1'):
        assert client.get('/api/v1/statuses?'+query,headers=auth(READ_TOKEN)).status_code==422


def test_inbox_new_duplicate_conflict_and_persistence(api):
    client,engine=api
    payload=event_payload()
    url="/api/v1/integrations/tpu/resource-events"
    resp=client.post(url,json=payload,headers=auth(SYNC_TOKEN))
    assert resp.status_code==202,resp.text
    assert resp.json()=={'event_id':payload['event_id'],'result':'accepted'}
    # Дубликат не создаёт вторую запись. Нормализация игнорирует порядок JSON-ключей.
    same={k:payload[k] for k in reversed(list(payload))}
    assert client.post(url,json=same,headers=auth(SYNC_TOKEN)).status_code==200
    changed={**payload,'data':{'name':'ДРУГОЙ'}}
    assert client.post(url,json=changed,headers=auth(SYNC_TOKEN)).status_code==409
    with read_transaction(engine) as conn:
        rows=conn.execute(text("""
            SELECT event_id, operation, processing_status, payload_json, payload_hash,
                   attempts, source_sequence FROM inbox_events
        """)).mappings().all()
    assert len(rows)==1
    row=rows[0]
    assert row['event_id']==payload['event_id']
    assert row['operation']=='UPDATE' and row['processing_status']=='PENDING'
    assert row['attempts']==0 and row['source_sequence']==1042
    assert json.loads(row['payload_json'])==payload and len(row['payload_hash'])==64


def test_post_rejects_wrong_token_and_bad_input(api):
    client,engine=api
    url="/api/v1/integrations/tpu/resource-events"
    payload=event_payload()
    assert client.post(url,json=payload).status_code==401
    assert client.post(url,json=payload,headers=auth(READ_TOKEN)).status_code==401
    assert client.post(url,content=b'{}',headers={**auth(SYNC_TOKEN),'Content-Type':'text/plain'}).status_code==415
    assert client.post(url,json={**payload,'operation_code':9},headers=auth(SYNC_TOKEN)).status_code==422
    assert client.post(url,content=b'not-json',headers={**auth(SYNC_TOKEN),'Content-Type':'application/json'}).status_code==422
    assert client.post(url,json={**payload,'occurred_at':'2026-11-13T09:15:00+03:00'},headers=auth(SYNC_TOKEN)).status_code==422
    assert client.post(url,content=b'{"data":"'+b'x'*70000+b'"}',headers={**auth(SYNC_TOKEN),'Content-Type':'application/json'}).status_code==413
    with read_transaction(engine) as conn:
        assert conn.execute(text('SELECT COUNT(*) FROM inbox_events')).scalar_one()==0


def test_invalid_non_json_constant_rejected(api):
    client,engine=api
    url="/api/v1/integrations/tpu/resource-events"
    raw=json.dumps(event_payload()).replace('"active": true','"active": NaN')
    resp=client.post(url,content=raw,headers={**auth(SYNC_TOKEN),'Content-Type':'application/json'})
    assert resp.status_code==422
    with read_transaction(engine) as conn:
        assert conn.execute(text('SELECT COUNT(*) FROM inbox_events')).scalar_one()==0


def test_event_survives_new_db_connection(api):
    client,engine=api
    p=event_payload()
    assert client.post('/api/v1/integrations/tpu/resource-events',json=p,headers=auth(SYNC_TOKEN)).status_code==202
    # Вторая независимая SQLite Engine читает данные, а не состояние в памяти API.
    from sqlalchemy.engine import make_url
    new_engine=make_engine(make_url(str(engine.url)).database)
    try:
        with read_transaction(new_engine) as conn:
            row=conn.execute(text('SELECT event_id,processing_status FROM inbox_events')).one()
        assert row[0]==p['event_id'] and row[1]=='PENDING'
    finally:
        new_engine.dispose()


@pytest.mark.parametrize('operation_code,operation',[(1,'INSERT'),(2,'DELETE'),(3,'UPDATE')])
def test_operation_code_normalization(api,operation_code,operation):
    client,engine=api
    response=client.post(
        '/api/v1/integrations/tpu/resource-events',
        json=event_payload(operation_code=operation_code),
        headers=auth(SYNC_TOKEN),
    )
    assert response.status_code==202
    with read_transaction(engine) as conn:
        assert conn.execute(text('SELECT operation FROM inbox_events')).scalar_one()==operation


def test_no_tokens_is_fail_closed(api):
    client,_=api
    app.dependency_overrides[get_settings] = lambda: AppSettings(
        _env_file=None, read_token=None, tpu_sync_token=None
    )
    assert client.get('/api/v1/statuses',headers=auth(READ_TOKEN)).status_code==503
    assert client.post('/api/v1/integrations/tpu/resource-events',json=event_payload(),headers=auth(SYNC_TOKEN)).status_code==503


def test_migration_mapping_constraints(api):
    _client,engine=api
    seed_resource(engine,'web-a')
    with write_transaction(engine) as conn:
        conn.execute(text("""
            INSERT INTO resource_source_mappings
            (source_system,source_entity,source_record_id,resource_id,created_at,updated_at)
            VALUES ('tpu','servers','record-1','web-a','2026-11-13T00:00:00Z','2026-11-13T00:00:00Z')
        """))
    with pytest.raises(Exception):
        with write_transaction(engine) as conn:
            conn.execute(text("""
                INSERT INTO resource_source_mappings
                (source_system,source_entity,source_record_id,resource_id,created_at,updated_at)
                VALUES ('tpu','servers','record-1','web-a','2026-11-13T00:00:00Z','2026-11-13T00:00:00Z')
            """))
    with read_transaction(engine) as conn:
        assert conn.execute(text('PRAGMA foreign_keys')).scalar_one()==1
        assert conn.execute(text('SELECT COUNT(*) FROM resource_source_mappings')).scalar_one()==1
