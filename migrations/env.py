"""Alembic применяет версионированные миграции SQLite в явно управляемой транзакции."""
from logging.config import fileConfig
import os
from alembic import context
from sqlalchemy import create_engine, event, pool

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

url = os.getenv("MONITORING_DATABASE_URL", config.get_main_option("sqlalchemy.url"))


def run_migrations_offline():
    context.configure(url=url, target_metadata=None, literal_binds=True,
                      dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    engine = create_engine(url, poolclass=pool.NullPool)

    @event.listens_for(engine, "connect")
    def on_connect(conn, _):
        conn.isolation_level = None
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")

    @event.listens_for(engine, "begin")
    def on_begin(connection):
        connection.exec_driver_sql("BEGIN IMMEDIATE")

    with engine.connect() as connection:
        context.configure(connection=connection,target_metadata=None,transactional_ddl=True)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
