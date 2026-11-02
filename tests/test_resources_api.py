
"""Тесты REST API ресурсов мониторинга ТПУ."""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.database import get_read_connection
from app.main import app
from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)
from services.resources import upsert_configuration
from services.status import apply_check_result
from shared.schemas import CheckResultIn, ResourceConfig


BASE = datetime(2026, 11, 2, 8, 0, tzinfo=timezone.utc)


@pytest.fixture
def api(tmp_path):
    """Создаём отдельную тестовую базу для каждого теста."""

    engine = make_engine(tmp_path / "resources_api.sqlite")
    initialize(engine)

    # Подменяем рабочую базу временной тестовой базой
    def override_connection():
        with read_transaction(engine) as conn:
            yield conn

    app.dependency_overrides[get_read_connection] = override_connection

    try:
        with TestClient(app) as client:
            yield client, engine
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


def add_resource(engine, resource_id):
    """Добавляем тестовый HTTP-ресурс в SQLite."""

    with write_transaction(engine) as conn:
        return upsert_configuration(
            conn,
            ResourceConfig(
                id=resource_id,
                name=f"Resource {resource_id}",
                target="https://example.org/health",
                check_type="HTTP",
            ),
            now=BASE,
        )


# --------------------------------------------------
# Тест 1. Пустой список ресурсов
# --------------------------------------------------

def test_empty_resources(api):
    client, _ = api

    response = client.get("/api/v1/resources")

    assert response.status_code == 200
    assert response.json() == []


# --------------------------------------------------
# Тест 2. Получение списка ресурсов
# --------------------------------------------------

def test_list_resources(api):
    client, engine = api

    add_resource(engine, "server-001")

    response = client.get("/api/v1/resources")

    assert response.status_code == 200

    resources = response.json()
    assert len(resources) == 1

    resource = resources[0]

    assert resource["id"] == "server-001"
    assert resource["name"] == "Resource server-001"
    assert resource["current_status"] == "UNKNOWN"
    assert resource["check_type"] == "HTTP"
    assert resource["active"] is True
    assert resource["config_version"] == 1

    assert resource["expected_http_codes"] == [
        {"start": 200, "end": 299}
    ]


# --------------------------------------------------
# Тест 3. Обновление статуса после проверок
# --------------------------------------------------

def test_status_updates_after_checks(api):
    client, engine = api

    version = add_resource(engine, "server-001")

    # Две успешные проверки переводят ресурс в UP
    for seconds in (1, 2):
        with write_transaction(engine) as conn:
            apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id="server-001",
                    config_version_id=version,
                    checked_at=BASE + timedelta(seconds=seconds),
                    success=True,
                    response_time_ms=10.0,
                    http_status_code=200,
                ),
            )

    response = client.get("/api/v1/resources")

    assert response.status_code == 200

    resource = response.json()[0]

    assert resource["current_status"] == "UP"
    assert resource["ok_count"] == 2
    assert resource["fail_count"] == 0
    assert resource["last_checked_at"] is not None


# --------------------------------------------------
# Тест 4. Пагинация списка ресурсов
# --------------------------------------------------

def test_resources_pagination(api):
    client, engine = api

    for resource_id in (
        "server-b",
        "server-a",
        "server-c",
    ):
        add_resource(engine, resource_id)

    response = client.get(
        "/api/v1/resources",
        params={
            "limit": 2,
            "offset": 1,
        },
    )

    assert response.status_code == 200

    ids = [
        resource["id"]
        for resource in response.json()
    ]

    # Ресурсы отсортированы по ID
    assert ids == ["server-b", "server-c"]


# --------------------------------------------------
# Тесты 5-7. Некорректные параметры пагинации
# --------------------------------------------------

@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"limit": 101},
        {"offset": -1},
    ],
)
def test_invalid_pagination(api, params):
    client, _ = api

    response = client.get(
        "/api/v1/resources",
        params=params,
    )

    assert response.status_code == 422


# --------------------------------------------------
# Тест 8. Получение ресурса по ID
# --------------------------------------------------

def test_get_resource_by_id(api):
    client, engine = api

    # Создаём два ресурса
    add_resource(engine, "server-001")
    add_resource(engine, "server-002")

    # Запрашиваем только второй
    response = client.get(
        "/api/v1/resources/server-002"
    )

    assert response.status_code == 200

    resource = response.json()

    assert resource["id"] == "server-002"
    assert resource["name"] == "Resource server-002"
    assert resource["current_status"] == "UNKNOWN"
    assert resource["config_version"] == 1
    assert resource["active"] is True

    assert resource["expected_http_codes"] == [
        {"start": 200, "end": 299}
    ]


# --------------------------------------------------
# Тест 9. Запрос несуществующего ресурса
# --------------------------------------------------

def test_get_nonexistent_resource(api):
    client, _ = api

    response = client.get(
        "/api/v1/resources/does-not-exist"
    )

    assert response.status_code == 404

    assert response.json()["detail"] == (
        "Ресурс не найден"
    )

def test_get_initial_resource_status(api):
    client, engine = api

    add_resource(engine, "server-001")

    response = client.get(
        "/api/v1/resources/server-001/status"
    )

    assert response.status_code == 200

    data = response.json()

    assert data["resource_id"] == "server-001"
    assert data["status"] == "UNKNOWN"
    assert data["checked_at"] is None
    assert data["error"] is None


def test_get_status_after_failed_checks(api):
    client, engine = api

    version = add_resource(engine, "server-001")

    # Сначала две успешные проверки переводят в UP
    for seconds in (1, 2):
        with write_transaction(engine) as conn:
            apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id="server-001",
                    config_version_id=version,
                    checked_at=BASE + timedelta(seconds=seconds),
                    success=True,
                    response_time_ms=10.0,
                    http_status_code=200,
                ),
            )

    # Три ошибки подряд переводят ресурс в DOWN
    for seconds in (3, 4, 5):
        with write_transaction(engine) as conn:
            apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id="server-001",
                    config_version_id=version,
                    checked_at=BASE + timedelta(seconds=seconds),
                    success=False,
                    response_time_ms=20.0,
                    http_status_code=503,
                    error_message="HTTP 503",
                ),
            )

    response = client.get(
        "/api/v1/resources/server-001/status"
    )

    assert response.status_code == 200

    data = response.json()

    assert data["resource_id"] == "server-001"
    assert data["status"] == "DOWN"
    assert data["error"] == "HTTP 503"

    checked_at = datetime.fromisoformat(
        data["checked_at"].replace("Z", "+00:00")
    )
    assert checked_at == BASE + timedelta(seconds=5)


def test_get_status_nonexistent_resource(api):
    client, _ = api

    response = client.get(
        "/api/v1/resources/unknown-server/status"
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Ресурс не найден"

def test_get_checks_empty_history(api):
    client, engine = api

    add_resource(engine, "server-001")

    response = client.get(
        "/api/v1/resources/server-001/checks"
    )

    assert response.status_code == 200
    assert response.json() == []


def test_get_checks_history_and_pagination(api):
    client, engine = api

    version = add_resource(engine, "server-001")

    # Создаём три проверки.
    checks = [
        (1, True, 200, None),
        (2, True, 200, None),
        (3, False, 503, "HTTP 503"),
    ]

    for seconds, success, code, error in checks:
        with write_transaction(engine) as conn:
            apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id="server-001",
                    config_version_id=version,
                    checked_at=BASE + timedelta(seconds=seconds),
                    success=success,
                    response_time_ms=10.0,
                    http_status_code=code,
                    error_message=error,
                ),
            )

    response = client.get(
        "/api/v1/resources/server-001/checks"
    )

    assert response.status_code == 200

    history = response.json()

    assert len(history) == 3

    # Самая новая проверка должна быть первой.
    assert [item["success"] for item in history] == [
        False, True, True
    ]

    assert history[0]["http_status_code"] == 503
    assert history[0]["error_message"] == "HTTP 503"
    assert history[0]["resulting_status"] == "UP"

    assert history[1]["resulting_status"] == "UP"
    assert history[2]["resulting_status"] == "UNKNOWN"

    # Проверяем пагинацию.
    paginated = client.get(
        "/api/v1/resources/server-001/checks",
        params={"limit": 1, "offset": 1},
    )

    assert paginated.status_code == 200
    assert len(paginated.json()) == 1
    assert paginated.json()[0]["success"] is True
    assert paginated.json()[0]["http_status_code"] == 200


def test_get_checks_nonexistent_resource(api):
    client, _ = api

    response = client.get(
        "/api/v1/resources/unknown-server/checks"
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Ресурс не найден"


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"limit": 101},
        {"offset": -1},
    ],
)
def test_get_checks_invalid_pagination(api, params):
    client, engine = api

    add_resource(engine, "server-001")

    response = client.get(
        "/api/v1/resources/server-001/checks",
        params=params,
    )

    assert response.status_code == 422
