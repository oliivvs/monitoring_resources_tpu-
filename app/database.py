
from collections.abc import Iterator
from functools import lru_cache

from sqlalchemy.engine import Connection, Engine, make_url

from app.config import AppSettings
from db.session import make_engine, read_transaction, write_transaction


@lru_cache
def get_engine() -> Engine:
    # Используем общую базу SQLite
    url = make_url(AppSettings().database_url)

    if url.drivername != "sqlite":
        raise ValueError("Проект поддерживает только SQLite")

    if not url.database or url.database == ":memory:":
        raise ValueError("Необходимо указать файл базы SQLite")

    return make_engine(url.database)


def get_read_connection() -> Iterator[Connection]:
    # Подключение для чтения данных
    with read_transaction(get_engine()) as conn:
        yield conn


def get_write_connection() -> Iterator[Connection]:
    # Подключение для изменения данных
    with write_transaction(get_engine()) as conn:
        yield conn
