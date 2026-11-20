
"""N21: запуск синхронизации Mock Entity API с SQLite."""

from __future__ import annotations

import argparse
import asyncio
import sys

from pathlib import Path

import httpx

from checks.security import NetworkPolicy
from db.session import make_engine

from integrations.tpu.entity_client import (
    EntityRoutes,
    TpuEntityClient,
)

from integrations.tpu.entity_mapper import (
    EntityMapper,
    load_mapping,
)

from integrations.tpu.entity_sync import (
    fetch_validated_resources,
    persist_validated_resources,
)


def build_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        description="Синхронизация Mock Entity API -> SQLite"
    )

    parser.add_argument(
        "--database",
        type=Path,
        required=True,
        help="Существующий файл SQLite",
    )

    parser.add_argument(
        "--mapping",
        type=Path,
        default=Path("config/tpu_entity_mapping.yaml"),
    )

    parser.add_argument(
        "--mock-url",
        default="http://127.0.0.1:18082",
    )

    parser.add_argument(
        "--allow-host",
        action="append",
        default=[],
    )

    parser.add_argument(
        "--allow-port",
        type=int,
        action="append",
        default=[],
    )

    parser.add_argument(
        "--allow-local-loopback",
        action="store_true",
        help="Разрешить loopback только для тестов",
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Сохранить проверенные данные в SQLite",
    )

    return parser


async def run(args: argparse.Namespace) -> int:

    mapping = load_mapping(args.mapping)

    policy = NetworkPolicy(
        allowed_hosts=frozenset(args.allow_host),
        allowed_ports=frozenset(args.allow_port),
        permit_loopback_for_tests=args.allow_local_loopback,
    )

    mapper = EntityMapper(mapping, policy)

    async with httpx.AsyncClient(
        trust_env=False
    ) as http_client:

        client = TpuEntityClient(
            base_url=args.mock_url,
            routes=EntityRoutes.local_mock(),
            http_client=http_client,
        )

        items = await fetch_validated_resources(
            client,
            mapper,
            entity_id=mapping.source_entity,
        )

    print(f"Проверено записей: {len(items)}")

    if not args.apply:
        print(
            "Dry-run: изменений SQLite нет. "
            "Для записи укажи --apply."
        )
        return 0

    if not args.database.is_file():
        raise FileNotFoundError(
            "SQLite не найдена: сначала выполните миграции"
        )

    engine = make_engine(args.database)

    try:
        report = await asyncio.to_thread(
            persist_validated_resources,
            engine,
            items,
        )
    finally:
        engine.dispose()

    print(f"Сохранено: {report.total}")

    for action in ("CREATED", "UPDATED", "UNCHANGED"):
        print(f"{action}: {report.count(action)}")

    return 0


def main() -> int:

    args = build_parser().parse_args()

    try:
        return asyncio.run(run(args))

    except Exception as exc:
        # Не раскрываем приватные URL и содержимое ответов API.
        print(
            f"Ошибка синхронизации: {type(exc).__name__}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
