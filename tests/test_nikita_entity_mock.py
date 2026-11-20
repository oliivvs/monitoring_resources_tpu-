
"""Тестирование Mock Entity API ТПУ."""

from fastapi.testclient import TestClient

from mock_entity_api.main import app


PREFIX = "/mock/tpu/entity/v1"


def test_entities():
    with TestClient(app) as client:
        response = client.get(f"{PREFIX}/entities")

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "entity_id": "servers",
                "name": "Тестовые серверы",
            }
        ]
    }


def test_structure():
    with TestClient(app) as client:
        response = client.get(
            f"{PREFIX}/entities/servers/structure"
        )

    assert response.status_code == 200
    assert len(response.json()["fields"]) == 5


def test_parameters_and_filters():
    with TestClient(app) as client:
        parameters = client.get(
            f"{PREFIX}/entities/servers/parameters"
        )
        filters = client.get(
            f"{PREFIX}/entities/servers/filters"
        )

    assert parameters.status_code == 200
    assert [
        item["name"]
        for item in parameters.json()["parameters"]
    ] == ["limit", "cursor"]

    assert filters.status_code == 200
    assert filters.json()["filters"][0]["name"] == "active"


def test_records_pagination():
    with TestClient(app) as client:
        first = client.get(
            f"{PREFIX}/entities/servers/records",
            params={"limit": 2},
        )

        second = client.get(
            f"{PREFIX}/entities/servers/records",
            params={
                "limit": 2,
                "cursor": first.json()["next_cursor"],
            },
        )

    assert first.status_code == 200
    assert len(first.json()["items"]) == 2
    assert first.json()["next_cursor"] == "2"

    assert len(second.json()["items"]) == 1
    assert second.json()["next_cursor"] is None


def test_filter_active():
    with TestClient(app) as client:
        response = client.get(
            f"{PREFIX}/entities/servers/records?active=true"
        )

    assert response.status_code == 200
    assert len(response.json()["items"]) == 2
    assert all(
        item["active"]
        for item in response.json()["items"]
    )


def test_unknown_entity():
    with TestClient(app) as client:
        for suffix in (
            "structure",
            "parameters",
            "filters",
            "records",
        ):
            response = client.get(
                f"{PREFIX}/entities/unknown/{suffix}"
            )

            assert response.status_code == 404


def test_bad_pagination():
    with TestClient(app) as client:
        for params in (
            {"limit": 0},
            {"limit": 101},
            {"cursor": "bad"},
        ):
            response = client.get(
                f"{PREFIX}/entities/servers/records",
                params=params,
            )

            assert response.status_code == 422
