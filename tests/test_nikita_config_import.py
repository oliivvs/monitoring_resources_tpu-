
"""N10: тесты импорта на временной SQLite, без внешней сети."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import text

from db.session import (
    initialize,
    make_engine,
    read_transaction,
    write_transaction,
)
from scripts.import_resources import main
from services.config_import import import_resources_file
from services.resources import upsert_configuration
from services.status import apply_check_result
from shared.schemas import CheckResultIn, ResourceConfig


@pytest.fixture
def engine(tmp_path):
    # Отдельная временная SQLite для каждого теста.
    db = make_engine(tmp_path / "n10.sqlite")
    initialize(db)
    yield db
    db.dispose()


def cfg(
    rid,
    *,
    interval=60,
    target="http://127.0.0.1:18080/health",
):
    return ResourceConfig(
        id=rid,
        name=rid,
        target=target,
        check_type="HTTP",
        interval_seconds=interval,
    )


def write_json(tmp_path, *configs):
    path = tmp_path / "resources.json"
    path.write_text(
        json.dumps({
            "resources": [
                c.model_dump(mode="json")
                for c in configs
            ]
        }),
        encoding="utf-8",
    )
    return path


def count(engine, table):
    with read_transaction(engine) as conn:
        return conn.execute(
            text(f"SELECT count(*) FROM {table}")
        ).scalar_one()


def test_initial_import_and_idempotence(engine, tmp_path):
    """Повторный импорт не должен создавать дубликаты версий."""
    path = write_json(tmp_path, cfg("a"), cfg("b"))

    first = import_resources_file(engine, path)

    assert first.total == 2
    assert first.count("CREATED") == 2
    assert count(engine, "resource_config_versions") == 2

    second = import_resources_file(engine, path)

    assert second.count("UNCHANGED") == 2
    assert second.items[0].version_id == first.items[0].version_id
    assert count(engine, "resource_config_versions") == 2


def test_version_change_preserves_confirmed_status(engine, tmp_path):
    """Новая версия сбрасывает счётчики, но сохраняет статус UP."""
    path = write_json(tmp_path, cfg("a"))
    first = import_resources_file(engine, path)
    version = first.items[0].version_id

    # Две успешные проверки: UNKNOWN -> UP.
    base = datetime.now(timezone.utc)

    for n in (1, 2):
        with write_transaction(engine) as conn:
            apply_check_result(
                conn,
                result=CheckResultIn(
                    resource_id="a",
                    config_version_id=version,
                    checked_at=base + timedelta(seconds=n),
                    success=True,
                    http_status_code=200,
                ),
            )

    updated = write_json(tmp_path, cfg("a", interval=120))
    report = import_resources_file(engine, updated)

    assert report.count("UPDATED") == 1
    assert report.items[0].version_id != version

    with read_transaction(engine) as conn:
        status, ok_count = conn.execute(text(
            "SELECT current_status, ok_count "
            "FROM resource_states WHERE resource_id='a'"
        )).one()

    assert (status, ok_count) == ("UP", 0)
    assert count(engine, "resource_config_versions") == 2


def test_atomic_rollback_when_later_resource_conflicts(
    engine, tmp_path,
):
    """Ошибка второго ресурса откатывает создание первого."""
    with write_transaction(engine) as conn:
        upsert_configuration(conn, cfg("existing"))

    path = write_json(
        tmp_path,
        cfg("new"),
        cfg(
            "existing",
            target="http://127.0.0.1:18081/health",
        ),
    )

    # В контракте изменение target требует нового resource_id.
    with pytest.raises(ValueError, match="нового resource_id"):
        import_resources_file(engine, path)

    assert count(engine, "resources") == 1
    assert count(engine, "resource_config_versions") == 1


def test_invalid_yaml_rejected_before_write(engine, tmp_path):
    """Повтор ключа в YAML не должен менять БД."""
    path = tmp_path / "resources.yaml"
    path.write_text(
        "resources: []\nresources: []\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Повтор"):
        import_resources_file(engine, path)

    assert count(engine, "resources") == 0


def test_cli_refuses_to_create_database(tmp_path):
    """CLI не создаёт неподготовленную SQLite."""
    path = write_json(tmp_path, cfg("a"))
    missing = tmp_path / "missing.sqlite"

    with pytest.raises(SystemExit) as exc:
        main([
            "--config", str(path),
            "--database", str(missing),
        ])

    assert exc.value.code == 2
    assert not missing.exists()


def test_cli_import_to_existing_database(
    engine, tmp_path, capsys,
):
    """Проверяем реальный вызов команды импорта."""
    path = write_json(tmp_path, cfg("a"))

    code = main([
        "--config", str(path),
        "--database", str(tmp_path / "n10.sqlite"),
    ])

    assert code == 0
    out = capsys.readouterr().out

    assert "Всего ресурсов: 1" in out
    assert "Создано: 1" in out
    assert count(engine, "resources") == 1


def test_cli_invalid_configuration_does_not_write(
    engine, tmp_path,
):
    """Неправильный файл не должен частично импортироваться."""
    path = tmp_path / "bad.json"
    path.write_text(
        '{"resources":[]}',
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        main([
            "--config", str(path),
            "--database", str(tmp_path / "n10.sqlite"),
        ])

    assert exc.value.code == 1
    assert count(engine, "resources") == 0
