
from contextlib import contextmanager

import pytest
from sqlalchemy import text

from app import database
from db.session import make_engine

from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.engine import Connection


@pytest.fixture
def test_engine(tmp_path, monkeypatch):
    # Отдельная временная база для каждого теста
    engine = make_engine(tmp_path / "test.db")

    monkeypatch.setattr(database, "get_engine", lambda: engine)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE test_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """))

    yield engine
    engine.dispose()


def test_foreign_keys_enabled(test_engine):
    # SQLite должен проверять внешние ключи
    with contextmanager(database.get_read_connection)() as conn:
        result = conn.execute(
            text("PRAGMA foreign_keys")
        ).scalar_one()

    assert result == 1
    assert conn.closed


def test_write_transaction_commit(test_engine):
    # Успешная транзакция сохраняет изменения
    with contextmanager(database.get_write_connection)() as conn:
        conn.execute(
            text("INSERT INTO test_items (name) VALUES (:name)"),
            {"name": "Тестовый ресурс"}
        )

    assert conn.closed

    with contextmanager(database.get_read_connection)() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM test_items")
        ).scalar_one()

    assert count == 1
    assert conn.closed


def test_write_transaction_rollback(test_engine):
    # Ошибка должна отменять всю транзакцию
    with pytest.raises(RuntimeError):
        with contextmanager(database.get_write_connection)() as conn:
            conn.execute(
                text("INSERT INTO test_items (name) VALUES (:name)"),
                {"name": "Не должен сохраниться"}
            )
            raise RuntimeError("Тестовая ошибка")

    assert conn.closed

    with contextmanager(database.get_read_connection)() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM test_items")
        ).scalar_one()

    assert count == 0
    assert conn.closed

def test_fastapi_read_dependency(test_engine):
    # Проверяем подключение для чтения через Depends
    test_app = FastAPI()
    connections = []

    @test_app.get("/test/read")
    def read_data(
        conn: Connection = Depends(database.get_read_connection),
    ):
        connections.append(conn)
        value = conn.execute(text("SELECT 1")).scalar_one()
        return {"value": value}

    with TestClient(test_app) as client:
        response = client.get("/test/read")

    assert response.status_code == 200
    assert response.json() == {"value": 1}
    assert connections[0].closed


def test_fastapi_write_dependency(test_engine):
    # Проверяем сохранение данных после HTTP-запроса
    test_app = FastAPI()
    connections = []

    @test_app.post("/test/write")
    def write_data(
        conn: Connection = Depends(database.get_write_connection),
    ):
        connections.append(conn)
        conn.execute(
            text("INSERT INTO test_items (name) VALUES (:name)"),
            {"name": "Ресурс FastAPI"},
        )
        return {"saved": True}

    with TestClient(test_app) as client:
        response = client.post("/test/write")

    assert response.status_code == 200
    assert response.json() == {"saved": True}
    assert connections[0].closed

    with test_engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM test_items")
        ).scalar_one()

    assert count == 1


def test_fastapi_write_rollback_on_error(test_engine):
    # Ошибка HTTP-запроса должна откатить изменения
    test_app = FastAPI()

    @test_app.post("/test/error")
    def write_with_error(
        conn: Connection = Depends(database.get_write_connection),
    ):
        conn.execute(
            text("INSERT INTO test_items (name) VALUES (:name)"),
            {"name": "Не сохранять"},
        )
        raise HTTPException(
            status_code=409,
            detail="Тестовая ошибка",
        )

    with TestClient(test_app) as client:
        response = client.post("/test/error")

    assert response.status_code == 409

    with test_engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM test_items")
        ).scalar_one()

    assert count == 0
