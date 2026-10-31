"""SQLite с явными транзакциями и обязательными внешними ключами."""
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine


def make_engine(path: str | Path) -> Engine:
    engine = create_engine(f"sqlite:///{path}", connect_args={"timeout": 30})

    @event.listens_for(engine, "connect")
    def sqlite_on_connect(connection, _):
        connection.isolation_level = None
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

    @event.listens_for(engine, "begin")
    def sqlite_on_begin(connection):
        connection.exec_driver_sql(
            "BEGIN IMMEDIATE" if connection.info.get("write_mode") else "BEGIN"
        )

    return engine


def initialize(engine: Engine):
    script = Path(__file__).resolve().parent.parent / "schema.sql"
    # executescript сам управляет фиксацией, поэтому применяем только при инициализации.
    with engine.raw_connection() as raw:
        raw.executescript(script.read_text(encoding="utf-8"))
        raw.commit()


@contextmanager
def write_transaction(engine: Engine):
    """Сериализует операции чтение→изменение; lock до первого SELECT."""
    with engine.connect() as conn:
        conn.info["write_mode"] = True
        try:
            with conn.begin():
                yield conn
        finally:
            conn.info.pop("write_mode", None)


@contextmanager
def read_transaction(engine: Engine):
    with engine.begin() as conn:
        yield conn
