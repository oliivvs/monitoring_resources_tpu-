
"""N22: запуск автоматического HEARTBEAT на существующей SQLite.

Примеры:
    python -m scripts.run_heartbeat_worker --database entity_demo.sqlite --once
    python -m scripts.run_heartbeat_worker --database entity_demo.sqlite --loop
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from checks.heartbeat_worker import HeartbeatWorker
from db.session import make_engine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Создание HEARTBEAT в очереди Outbox"
    )
    parser.add_argument(
        "--database",
        required=True,
        type=Path,
        help="Существующий файл SQLite с применённой схемой мониторинга",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Один проход (по умолчанию)",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Запустить непрерывный фоновый цикл",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=10.0,
        help="Частота опроса БД; не интервал самого HEARTBEAT",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.loop and args.once:
        parser.error("Выбери только --once или --loop")

    if not args.database.is_file():
        parser.error("SQLite не найдена. Сначала создай БД и примени схему")

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    engine = make_engine(args.database)

    try:
        worker = HeartbeatWorker(engine, poll_seconds=args.poll_seconds)

        if not args.loop:
            result = asyncio.run(worker.run_once())
            print(f"Проверено ресурсов: {result.scanned}")
            print(f"Добавлено HEARTBEAT: {result.queued}")
        else:
            print("Цикл HEARTBEAT запущен. Остановка: Ctrl+C")
            asyncio.run(worker.run())

    except KeyboardInterrupt:
        print("Цикл HEARTBEAT остановлен")
    finally:
        engine.dispose()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
