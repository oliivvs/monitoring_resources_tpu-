"""A25.2: автономная проверка примера ответа Entity API."""

from __future__ import annotations

import argparse
import json
import os

from pathlib import Path


from checks.security import NetworkPolicy
from integrations.tpu.entity_contract import (
    ContractMismatch,
    load_contract,
)
from integrations.tpu.entity_client import EntityRoutes
from integrations.tpu.entity_mapper import (
    EntityMapper,
    load_mapping,
)
from integrations.tpu.entity_models import RecordsResponse
from integrations.tpu.oracle_prepare import FULL_UPDATE_FIELDS








def validate_fixture(
    *,
    contract: dict,
    mapping,
    payload: dict,
    record_id: str,
    sequence: int,
    worker_records_path: str | None = None,
):
    if (
        worker_records_path is not None
        and contract["records_path"] != worker_records_path
    ):
        raise ContractMismatch(
            "Маршрут контракта не совпадает с Worker"
        )

    if contract["source_entity"] != mapping.source_entity:
        raise ContractMismatch("Не совпадает сущность источника")

    identity_field = mapping.fields["source_record_id"]

    if contract["identity_field"] != identity_field:
        raise ContractMismatch("Не совпадает поле идентификатора")

    try:
        response = RecordsResponse.model_validate(payload)
    except ValueError:
        raise ContractMismatch(
            "Неверный формат ответа Entity API"
        ) from None

    if response.next_cursor is not None:
        raise ContractMismatch(
            "Нельзя подтвердить единственность записи"
        )

    if len(response.items) != 1:
        raise ContractMismatch(
            "Ожидается ровно одна запись"
        )

    record = response.items[0]

    if record.get(identity_field) != record_id:
        raise ContractMismatch(
            "Не совпадает идентификатор ресурса"
        )

    actual_sequence = record.get(
        contract["sequence_field"]
    )

    if (
        type(actual_sequence) is not int
        or actual_sequence != sequence
    ):
        raise ContractMismatch(
            "Не совпадает версия снимка"
        )

    if not FULL_UPDATE_FIELDS.issubset(mapping.fields):
        raise ContractMismatch(
            "Mapping содержит не все необходимые поля"
        )

    required = {
        mapping.fields[field]
        for field in FULL_UPDATE_FIELDS
    }

    if not required.issubset(record):
        raise ContractMismatch(
            "В снимке отсутствуют обязательные поля"
        )

    # Никаких сетевых запросов: map_record проверяет
    # структуру и синтаксис конфигурации локально.
    mapper = EntityMapper(
        mapping,
        NetworkPolicy(
            allowed_hosts=frozenset({"1.1.1.1"}),
            allowed_ports=frozenset({443}),
        ),
    )

    try:
        mapped = mapper.map_record(record)
    except ValueError:
        raise ContractMismatch(
            "Запись не преобразуется в ResourceConfig"
        ) from None

    return mapped


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--contract",
        default="config/tpu_entity_contract.yaml",
    )
    parser.add_argument(
        "--mapping",
        default="config/tpu_oracle_mapping.yaml",
    )
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--record-id", required=True)
    parser.add_argument("--sequence", type=int, required=True)

    args = parser.parse_args()

    contract = load_contract(args.contract)
    mapping = load_mapping(args.mapping)

    payload = json.loads(
        Path(args.fixture).read_text(encoding="utf-8")
    )

    worker_records_path = (
        os.environ.get("MONITORING_ORACLE_SNAPSHOT_RECORDS_PATH")
        or EntityRoutes.local_mock().records
    )

    result = validate_fixture(
        contract=contract,
        mapping=mapping,
        payload=payload,
        record_id=args.record_id,
        sequence=args.sequence,
        worker_records_path=worker_records_path,
    )

    print("ENTITY API CONTRACT: OK")
    print("resource_id:", result.config.id)
    print("check_type:", result.config.check_type)
    print("interval_seconds:", result.config.interval_seconds)


if __name__ == "__main__":
    main()
