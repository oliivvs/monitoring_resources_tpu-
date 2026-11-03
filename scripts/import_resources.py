
"""N10: загрузка ресурсов в подготовленную SQLite через терминал."""
from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from sqlalchemy.exc import SQLAlchemyError

from db.session import make_engine
from services.config_import import import_resources_file


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Загрузить JSON/YAML ресурсы в существующую SQLite"
    )

    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Файл ресурсов",
    )
    parser.add_argument(
        "--database",
        required=True,
        type=Path,
        help="Файл подготовленной SQLite",
    )

    args = parser.parse_args(argv)

    # Не создаём рабочую БД автоматически.
    # Сначала должны быть применены миграции Alembic.
    if not args.database.is_file():
        parser.error(
            "Файл SQLite не найден. Сначала выполните миграции Alembic."
        )

    engine = make_engine(args.database)

    try:
        report = import_resources_file(engine, args.config)
    except (
        OSError,
        ValueError,
        yaml.YAMLError,
        SQLAlchemyError,
    ) as exc:
        # Текст ошибки может содержать URL или другие чувствительные данные.
        parser.exit(
            1,
            f"Импорт отклонён ({type(exc).__name__}); "
            "SQLite не изменена.\n",
        )
    finally:
        engine.dispose()

    print(f"Всего ресурсов: {report.total}")
    print(f"Создано: {report.count('CREATED')}")
    print(f"Новых версий: {report.count('UPDATED')}")
    print(f"Без новой версии: {report.count('UNCHANGED')}")

    for item in report.items:
        print(
            f"  {item.resource_id}: "
            f"версия ID={item.version_id}, {item.action}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
