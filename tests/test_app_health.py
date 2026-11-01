
from fastapi.testclient import TestClient

from app.main import app


def test_health_returns_200():
    # Проверяем доступность приложения
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_swagger_available():
    # Проверяем документацию Swagger
    with TestClient(app) as client:
        response = client.get("/docs")

    assert response.status_code == 200


def test_openapi_schema():
    # Проверяем наличие маршрута в OpenAPI
    with TestClient(app) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200

    schema = response.json()

    assert schema["info"]["title"] == "Мониторинг ресурсов ТПУ"
    assert "/health" in schema["paths"]
    assert "get" in schema["paths"]["/health"]
    assert schema["components"]["schemas"]["HealthResponse"]
