
"""N25: запуск HEARTBEAT из CLI или Docker с корректной остановкой.

Примеры:
    python -m scripts.run_heartbeat_worker --database entity_demo.sqlite --once
    python -m scripts.run_heartbeat_worker --database /data/monitoring.db --loop
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
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
        help="Существующая SQLite с применёнными миграциями",
    )

    modes = parser.add_mutually_exclusive_group()

    modes.add_argument(
        "--once",
        action="store_true",
        help="Однократный проход (по умолчанию)",
    )

    modes.add_argument(
        "--loop",
        action="store_true",
        help="Работа до штатной остановки",
    )

    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=10.0,
        help="Период опроса SQLite, не интервал самого HEARTBEAT",
    )

    return parser


async def run_worker(
    worker: HeartbeatWorker,
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """При SIGTERM/SIGINT просим worker завершиться после прохода.

    На Windows add_signal_handler может быть недоступен.
    В этом случае Ctrl+C перехватывается в main().
    """

    loop = loop or asyncio.get_running_loop()
    registered: list[signal.Signals] = []

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, worker.stop)

            except (NotImplementedError, RuntimeError):
                # Windows ProactorEventLoop может не
                # поддерживать обработчики сигналов.
                continue

            registered.append(sig)

        await worker.run()

    finally:
        worker.stop()

        for sig in registered:
            loop.remove_signal_handler(sig)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.database.is_file():
        parser.error(
            "SQLite не найдена. "
            "Сначала создай БД и примени миграции"
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    engine = make_engine(args.database)

    try:
        worker = HeartbeatWorker(
            engine,
            poll_seconds=args.poll_seconds,
        )

        if not args.loop:
            result = asyncio.run(worker.run_once())

            print(
                f"Проверено ресурсов: {result.scanned}"
            )
            print(
                f"Добавлено HEARTBEAT: {result.queued}"
            )

        else:
            print(
                "Цикл HEARTBEAT запущен. "
                "Остановка: Ctrl+C / SIGTERM"
            )

            asyncio.run(run_worker(worker))

    except KeyboardInterrupt:
        print("Цикл HEARTBEAT остановлен")

    finally:
        engine.dispose()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
