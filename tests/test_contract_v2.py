from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
from threading import Barrier

import pytest
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, DatabaseError

from db.session import initialize, make_engine, read_transaction, write_transaction
from services.resources import upsert_configuration, get_active_check_snapshot
from services.status import apply_check_result, StaleConfigurationResult
from services.events import enqueue_heartbeat_if_due, claim_one_event, finish_delivery
from shared.schemas import CheckResultIn, ResourceConfig, CheckType, ExpectedHttpRange

BASE = datetime(2026, 10, 31, 8, 0, 0, tzinfo=timezone.utc)


def at(seconds):
    return BASE + timedelta(seconds=seconds)


@pytest.fixture
def engine(tmp_path):
    engine = make_engine(tmp_path / "monitoring.sqlite")
    initialize(engine)
    yield engine
    engine.dispose()


def create_http(engine):
    with write_transaction(engine) as con:
        return upsert_configuration(con, ResourceConfig(
            id="server-001", name="Test Server",target="https://example.org/health",
            check_type=CheckType.HTTP,
            expected_http_codes=[ExpectedHttpRange(start=200,end=299)]
        ), now=at(0))


def send_check(engine, offset, success, *, code=None, error=None, config_version_id=None):
    if config_version_id is None:
        with read_transaction(engine) as con:
            config_version_id = get_active_check_snapshot(con, resource_id="server-001").config_version_id
    with write_transaction(engine) as con:
        return apply_check_result(con, result=CheckResultIn(
            resource_id="server-001", config_version_id=config_version_id, checked_at=at(offset),success=success,
            response_time_ms=10.0, http_status_code=code,
            error_message=error,
        ))


def scalar(engine, query):
    with read_transaction(engine) as con:
        return con.execute(text(query)).scalar_one()


def test_schema_and_foreign_keys(engine):
    with read_transaction(engine) as con:
        names = {row[0] for row in con.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))}
        enabled = con.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
    assert enabled == 1
    assert {"resources","resource_config_versions","http_expected_ranges",
            "resource_states","check_results","status_transitions",
            "outbox_events","delivery_attempts"}.issubset(names)
    with pytest.raises(IntegrityError):
        with write_transaction(engine) as con:
            con.execute(text("INSERT INTO resource_states (resource_id,config_version_id,updated_at) VALUES ('missing',1,'2026-01-01T00:00:00Z')"))


def test_full_threshold_lifecycle(engine):
    create_http(engine)
    steps = [(1, True, 200, None), (2, True, 200, None),
             (3, False, 503, "HTTP 503"), (4, False, 503, "HTTP 503"),
             (5, False, 503, "HTTP 503"), (6, True, 200, None), (7, True, 200, None)]
    results = [send_check(engine, sec, ok, code=code, error=error) for sec,ok,code,error in steps]
    assert [x["current_status"] for x in results] == ["UNKNOWN","UP","UP","UP","DOWN","DOWN","UP"]
    assert [x["status_changed"] for x in results] == [False,True,False,False,True,False,True]
    assert scalar(engine,"SELECT count(*) FROM check_results") == 7
    assert scalar(engine,"SELECT count(*) FROM status_transitions") == 3
    assert scalar(engine,"SELECT count(*) FROM outbox_events WHERE event_type='STATUS_CHANGED'") == 3
    with read_transaction(engine) as con:
        rows=con.execute(text("SELECT payload_json FROM outbox_events ORDER BY created_at")).all()
        for row in rows:
            d=json.loads(row[0]); assert d["resource_id"]=="server-001" and d["event_id"]


def test_atomic_rollback_on_exception(engine):
    create_http(engine)
    send_check(engine,1,True,code=200)
    with read_transaction(engine) as con:
        version = get_active_check_snapshot(con, resource_id="server-001").config_version_id
    with pytest.raises(RuntimeError):
        with write_transaction(engine) as con:
            apply_check_result(con, result=CheckResultIn(
                resource_id="server-001",config_version_id=version,
                checked_at=at(2),success=True,http_status_code=200))
            raise RuntimeError("Сбой до commit")
    assert scalar(engine,"SELECT count(*) FROM check_results") == 1
    assert scalar(engine,"SELECT count(*) FROM outbox_events") == 0
    assert scalar(engine,"SELECT ok_count FROM resource_states WHERE resource_id='server-001'") == 1
    assert send_check(engine,2,True,code=200)["current_status"] == "UP"


def test_heartbeat_no_duplicate_and_immutable_payload(engine):
    create_http(engine)
    send_check(engine,1,True,code=200)
    send_check(engine,2,True,code=200)
    with write_transaction(engine) as con:
        assert enqueue_heartbeat_if_due(con,resource_id="server-001",now=at(100)) is None
    with write_transaction(engine) as con:
        assert enqueue_heartbeat_if_due(con,resource_id="server-001",now=at(303)) is not None
    with write_transaction(engine) as con:
        assert enqueue_heartbeat_if_due(con,resource_id="server-001",now=at(304)) is None
    assert scalar(engine,"SELECT count(*) FROM outbox_events WHERE event_type='HEARTBEAT'") == 1
    with pytest.raises(DatabaseError):
        with write_transaction(engine) as con:
            con.execute(text("UPDATE outbox_events SET payload_json='{}' WHERE event_type='HEARTBEAT'"))


def test_versions_and_state_reset_preserves_history(engine):
    v1 = create_http(engine)
    send_check(engine,1,True,code=200)
    send_check(engine,2,True,code=200)
    with write_transaction(engine) as con:
        v2 = upsert_configuration(con,ResourceConfig(
            id="server-001",name="Test Server renamed",target="https://example.org/health",
            check_type=CheckType.HTTP,up_after=3,
            expected_http_codes=[ExpectedHttpRange(start=200,end=299)]
        ),now=at(400))
    assert v2 != v1
    assert scalar(engine,"SELECT count(*) FROM resource_config_versions") == 2
    assert scalar(engine,"SELECT count(*) FROM check_results") == 2
    assert scalar(engine,"SELECT current_status FROM resource_states") == "UP"
    assert scalar(engine,"SELECT ok_count FROM resource_states") == 0
    assert scalar(engine,"SELECT count(*) FROM resource_config_versions WHERE retired_at IS NULL") == 1


def test_tcp_validation():
    with pytest.raises(ValidationError):
        ResourceConfig(id="tcp",name="TCP",target="127.0.0.1",check_type="TCP",port=5432)
    o=ResourceConfig(id="tcp",name="TCP",target="127.0.0.1",check_type="TCP",port=5432,expected_http_codes=[])
    assert o.port == 5432
    with pytest.raises(ValidationError):
        ResourceConfig(id="bad",name="HTTP",target="invalid",check_type="HTTP")


def test_delivery_retry_claim_and_attempt_history(engine):
    create_http(engine)
    send_check(engine,1,True,code=200)
    send_check(engine,2,True,code=200)
    with write_transaction(engine) as con:
        claim = claim_one_event(con,worker_id="worker-a",now=at(3))
        assert claim
    with write_transaction(engine) as con:
        assert claim_one_event(con,worker_id="worker-b",now=at(4)) is None
        assert finish_delivery(con,event_id=claim["event_id"],worker_id="worker-a",
                               http_status_code=500,error_message="HTTP 500",now=at(5))=="PENDING"
    assert scalar(engine,"SELECT attempts FROM outbox_events") == 1
    with write_transaction(engine) as con:
        assert claim_one_event(con,worker_id="worker-a",now=at(6)) is None
    with write_transaction(engine) as con:
        claim2=claim_one_event(con,worker_id="worker-b",now=at(7))
        assert claim2["event_id"] == claim["event_id"]
    with write_transaction(engine) as con:
        assert finish_delivery(con,event_id=claim2["event_id"],worker_id="worker-b",
                               http_status_code=202,now=at(8)) == "SENT"
    assert scalar(engine,"SELECT attempts FROM outbox_events") == 2
    assert scalar(engine,"SELECT count(*) FROM delivery_attempts") == 2
    assert scalar(engine,"SELECT delivery_status FROM outbox_events") == "SENT"


def test_competing_claims_only_one_wins(engine):
    create_http(engine)
    send_check(engine,1,True,code=200)
    send_check(engine,2,True,code=200)
    barrier = Barrier(2)
    def attempt(worker):
        barrier.wait()
        with write_transaction(engine) as con:
            row=claim_one_event(con,worker_id=worker,now=at(3))
            return row["event_id"] if row else None
    with ThreadPoolExecutor(max_workers=2) as pool:
        a=pool.submit(attempt,"a");b=pool.submit(attempt,"b")
        ids=[a.result(),b.result()]
    assert sum(x is not None for x in ids) == 1


def test_stale_check_is_rejected(engine):
    create_http(engine)
    send_check(engine,10,True,code=200)
    with pytest.raises(ValueError,match="устаревшая"):
        send_check(engine,10,True,code=200)
    assert scalar(engine,"SELECT count(*) FROM check_results") == 1


def test_repeat_same_configuration_is_idempotent(engine):
    v1=create_http(engine)
    send_check(engine,1,True,code=200)
    cfg=ResourceConfig(id="server-001", name="Test Server",target="https://example.org/health",
                      check_type="HTTP",expected_http_codes=[ExpectedHttpRange(start=200,end=299)])
    with write_transaction(engine) as con:
        v2=upsert_configuration(con,cfg,now=at(200))
    assert v1 == v2
    assert scalar(engine,"SELECT ok_count FROM resource_states") == 1
    assert scalar(engine,"SELECT count(*) FROM resource_config_versions") == 1


def test_target_change_requires_new_resource_id(engine):
    create_http(engine)
    with pytest.raises(ValueError, match="нового resource_id"):
        with write_transaction(engine) as con:
            upsert_configuration(con,ResourceConfig(id="server-001",name="Test Server",
                target="https://example.org/other",check_type="HTTP"),now=at(100))
    assert scalar(engine,"SELECT count(*) FROM resource_config_versions") == 1


def test_check_event_insertion_failure_rolls_back_everything(engine):
    create_http(engine)
    send_check(engine,1,True,code=200)
    # Имитация сбоя именно при INSERT события после расчёта UP.
    with write_transaction(engine) as con:
        con.exec_driver_sql("""CREATE TRIGGER deny_event BEFORE INSERT ON outbox_events
                               BEGIN SELECT RAISE(ABORT,'send queue unavailable'); END""")
    with pytest.raises(DatabaseError):
        send_check(engine,2,True,code=200)
    assert scalar(engine,"SELECT count(*) FROM check_results") == 1
    assert scalar(engine,"SELECT current_status FROM resource_states") == "UNKNOWN"
    assert scalar(engine,"SELECT count(*) FROM status_transitions") == 0
    assert scalar(engine,"SELECT count(*) FROM outbox_events") == 0


def test_consistency_checks_for_snapshot(engine):
    create_http(engine)
    send_check(engine,1,True,code=200)
    send_check(engine,2,True,code=200)
    with read_transaction(engine) as con:
        event=con.execute(text("SELECT event_id,payload_json FROM outbox_events")).one()
    assert json.loads(event.payload_json)["event_id"]==event.event_id
    with pytest.raises(IntegrityError):
        with write_transaction(engine) as con:
            con.execute(text("""
               INSERT INTO outbox_events (event_id,resource_id,event_type,status,checked_at,payload_json,created_at)
               VALUES ('fake','server-001','HEARTBEAT','UP','2026-10-31T08:00:00.000000Z',:payload,'2026-10-31T08:00:00.000000Z')
            """), {"payload":event.payload_json})


def state_snapshot(engine):
    with read_transaction(engine) as con:
        return dict(con.execute(text("""
           SELECT config_version_id,current_status,ok_count,fail_count,
                  last_checked_at,status_changed_at,revision
           FROM resource_states WHERE resource_id='server-001'
        """)).mappings().one())


def counts(engine):
    return tuple(scalar(engine, f"SELECT count(*) FROM {table}") for table in
                 ("check_results", "status_transitions", "outbox_events"))


def test_missing_version_token_is_validation_error():
    # Без метки начала проверки на фоне динамического конфигурационного обновления
    # нельзя доказать, что ответ принадлежит активной версии.
    with pytest.raises(ValidationError):
        CheckResultIn(resource_id="server-001", checked_at=at(100), success=True,
                      http_status_code=200)


def test_changed_configuration_rejects_late_result_without_side_effects(engine):
    v1 = create_http(engine)
    with read_transaction(engine) as con:
        launched = get_active_check_snapshot(con, resource_id="server-001")
    assert launched.config_version_id == v1
    assert launched.target == "https://example.org/health"
    # Пока HTTP-запрос v1 ожидает ответа, настраивают иной порог.
    with write_transaction(engine) as con:
        v2 = upsert_configuration(con, ResourceConfig(
            id="server-001", name="Test Server", target="https://example.org/health",
            check_type="HTTP", up_after=3,
            expected_http_codes=[ExpectedHttpRange(start=200,end=299)]
        ), now=at(20))
    assert v2 != launched.config_version_id
    before, old_counts = state_snapshot(engine), counts(engine)
    # Ответ v1 пришёл позже новой конфигурации: timestamp более свежий,
    # поэтому одно сравнение checked_at с last_checked_at не спасло бы.
    with pytest.raises(StaleConfigurationResult, match="Устаревшая версия конфигурации"):
        send_check(engine, 40, True, code=200,
                   config_version_id=launched.config_version_id)
    assert state_snapshot(engine) == before
    assert counts(engine) == old_counts
    # Снимок новой версии применяется штатно, без отравления счётчиков старым ответом.
    assert send_check(engine, 41, True, code=200, config_version_id=v2)["current_status"] == "UNKNOWN"
    assert send_check(engine, 42, True, code=200, config_version_id=v2)["current_status"] == "UNKNOWN"
    result = send_check(engine, 43, True, code=200, config_version_id=v2)
    assert result["current_status"] == "UP" and result["status_changed"]
    assert counts(engine) == (3, 1, 1)


def test_disable_then_enable_cannot_resurrect_inflight_result(engine):
    v1=create_http(engine)
    with read_transaction(engine) as con:
        launched=get_active_check_snapshot(con,resource_id="server-001")
    assert launched.config_version_id == v1
    with write_transaction(engine) as con:
        v2=upsert_configuration(con, ResourceConfig(
            id="server-001",name="Test Server",target="https://example.org/health",
            check_type="HTTP",active=False), now=at(20))
    with read_transaction(engine) as con:
        with pytest.raises(ValueError, match="выключенного"):
            get_active_check_snapshot(con,resource_id="server-001")
    with write_transaction(engine) as con:
        v3=upsert_configuration(con, ResourceConfig(
            id="server-001",name="Test Server",target="https://example.org/health",
            check_type="HTTP",active=True), now=at(30))
    assert v1 != v2 and v2 != v3
    before=state_snapshot(engine)
    with pytest.raises(StaleConfigurationResult):
        send_check(engine, 40, True, code=200, config_version_id=v1)
    assert state_snapshot(engine) == before
    assert counts(engine) == (0,0,0)
    assert send_check(engine,41,True,code=200,config_version_id=v3)["current_status"] == "UNKNOWN"


def test_database_direct_insert_rejects_stale_version(engine):
    v1=create_http(engine)
    with write_transaction(engine) as con:
        v2=upsert_configuration(con, ResourceConfig(
            id="server-001",name="Test Server",target="https://example.org/health",
            check_type="HTTP",up_after=5),now=at(20))
    assert v1 != v2
    # Даже сторонний SQL-код не должен обойти защиту.
    with pytest.raises(DatabaseError,match="Stale or inactive check configuration"):
        with write_transaction(engine) as con:
            con.execute(text("""INSERT INTO check_results (
                resource_id,config_version_id,checked_at,success,resulting_status
            ) VALUES ('server-001',:old,:at,1,'UNKNOWN')"""),
            {"old":v1,"at":"2026-10-31T08:00:40.000000Z"})
    assert counts(engine) == (0,0,0)


def test_repeated_same_version_is_valid(engine):
    v1=create_http(engine)
    with read_transaction(engine) as con:
        launched=get_active_check_snapshot(con,resource_id="server-001")
    with write_transaction(engine) as con:
        v_same=upsert_configuration(con,ResourceConfig(
            id="server-001",name="Test Server",target="https://example.org/health",
            check_type="HTTP"),now=at(20))
    assert v_same == v1
    assert send_check(engine,25,True,code=200,config_version_id=launched.config_version_id)["current_status"] == "UNKNOWN"


def test_outbox_stays_unchanged_when_stale_result_would_change_status(engine):
    v1=create_http(engine)
    send_check(engine,1,True,code=200,config_version_id=v1)
    with write_transaction(engine) as con:
        v2=upsert_configuration(con,ResourceConfig(
            id="server-001",name="Test Server",target="https://example.org/health",
            check_type="HTTP",timeout_seconds=8.0),now=at(10))
    # Два ответа старой версии после смены параметров ошибочно создали бы UP.
    for sec in (11,12):
        with pytest.raises(StaleConfigurationResult):
            send_check(engine,sec,True,code=200,config_version_id=v1)
    assert counts(engine) == (1,0,0)
    assert state_snapshot(engine)["current_status"] == "UNKNOWN"
    assert send_check(engine,13,True,code=200,config_version_id=v2)["current_status"] == "UNKNOWN"


def test_inflight_check_race_with_config_update(engine):
    """Сетевой вызов живёт дольше транзакции, а конфиг успевает измениться."""
    from threading import Event

    create_http(engine)
    request_started = Event()
    allow_response = Event()

    def simulated_http_request():
        # Зафиксировали v1 ДО запроса и сразу отпустили read-транзакцию.
        with read_transaction(engine) as con:
            snapshot=get_active_check_snapshot(con,resource_id="server-001")
        request_started.set()
        assert allow_response.wait(timeout=5)
        try:
            with write_transaction(engine) as con:
                apply_check_result(con,result=CheckResultIn(
                    resource_id=snapshot.resource_id,
                    config_version_id=snapshot.config_version_id,
                    checked_at=at(50),success=True,http_status_code=200
                ))
        except StaleConfigurationResult:
            return "REJECTED_STALE"
        return "APPLIED"  # ошибка регрессии

    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(simulated_http_request)
        assert request_started.wait(timeout=5)
        with write_transaction(engine) as con:
            upsert_configuration(con,ResourceConfig(
                id="server-001",name="Test Server",
                target="https://example.org/health",check_type="HTTP",
                timeout_seconds=8.0
            ),now=at(20))
        before=state_snapshot(engine)
        allow_response.set()
        assert future.result(timeout=5) == "REJECTED_STALE"
    assert state_snapshot(engine)==before
    assert counts(engine)==(0,0,0)
