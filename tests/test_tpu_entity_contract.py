"""Проверки учебного контракта Entity API."""

import json
from pathlib import Path

import pytest
import yaml

from integrations.tpu.entity_client import EntityRoutes
from integrations.tpu.entity_mapper import load_mapping
from scripts.check_tpu_entity_contract import (
    ContractMismatch,
    load_contract,
    validate_fixture,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_FILE = ROOT / "config/tpu_entity_contract.yaml"
MAPPING_FILE = ROOT / "config/tpu_oracle_mapping.yaml"
FIXTURE_FILE = ROOT / "tests/fixtures/tpu_entity_snapshot.json"


@pytest.fixture
def payload():
    return json.loads(FIXTURE_FILE.read_text(encoding="utf-8"))


def validate(payload, *, record_id="example-server-001",
             sequence=2, contract=None, worker_path=None):
    return validate_fixture(
        contract=contract or load_contract(CONTRACT_FILE),
        mapping=load_mapping(MAPPING_FILE),
        payload=payload,
        record_id=record_id,
        sequence=sequence,
        worker_records_path=worker_path,
    )


def test_valid_contract(payload):
    result = validate(payload)

    assert result.config.id == "tpu-oracle-example-server-001"
    assert result.config.interval_seconds == 120


@pytest.mark.parametrize("sequence", [1, 3])
def test_wrong_sequence(payload, sequence):
    with pytest.raises(ContractMismatch):
        validate(payload, sequence=sequence)


@pytest.mark.parametrize("value", [True, "2", None])
def test_invalid_sequence_type(payload, value):
    payload["items"][0]["source_sequence"] = value

    with pytest.raises(ContractMismatch):
        validate(payload)


def test_duplicate_records(payload):
    payload["items"].append(dict(payload["items"][0]))

    with pytest.raises(ContractMismatch):
        validate(payload)


def test_empty_records(payload):
    payload["items"] = []

    with pytest.raises(ContractMismatch):
        validate(payload)


@pytest.mark.parametrize("field", ["name", "target"])
def test_missing_required_field(payload, field):
    del payload["items"][0][field]

    with pytest.raises(ContractMismatch):
        validate(payload)


def test_wrong_record_id(payload):
    with pytest.raises(ContractMismatch):
        validate(payload, record_id="another-server")


def test_invalid_response_shape(payload):
    payload["items"] = [{}]

    with pytest.raises(ContractMismatch):
        validate(payload)


def test_unexpected_pagination(payload):
    payload["next_cursor"] = "page-2"

    with pytest.raises(ContractMismatch):
        validate(payload)


@pytest.mark.parametrize(
    "bad_path",
    [
        "https://evil.example.org/{entity_id}",
        "/api/../entities/{entity_id}",
    ],
)
def test_unsafe_contract_path(tmp_path, bad_path):
    contract = yaml.safe_load(
        CONTRACT_FILE.read_text(encoding="utf-8")
    )
    contract["records_path"] = bad_path

    path = tmp_path / "contract.yaml"
    path.write_text(
        yaml.safe_dump(contract),
        encoding="utf-8",
    )

    with pytest.raises(ContractMismatch):
        load_contract(path)


def test_unsupported_response_schema(tmp_path):
    contract = yaml.safe_load(
        CONTRACT_FILE.read_text(encoding="utf-8")
    )
    contract["response_items_field"] = "data"

    path = tmp_path / "contract.yaml"
    path.write_text(
        yaml.safe_dump(contract),
        encoding="utf-8",
    )

    with pytest.raises(ContractMismatch):
        load_contract(path)


def test_worker_route_mismatch(payload):
    contract = dict(load_contract(CONTRACT_FILE))
    contract["records_path"] = (
        "/api/v1/entities/{entity_id}/records"
    )

    with pytest.raises(
        ContractMismatch,
        match="Маршрут контракта",
    ):
        validate(
            payload,
            contract=contract,
            worker_path=EntityRoutes.local_mock().records,
        )


def test_worker_route_override(payload):
    contract = dict(load_contract(CONTRACT_FILE))
    custom_path = "/api/v1/entities/{entity_id}/records"
    contract["records_path"] = custom_path

    result = validate(
        payload,
        contract=contract,
        worker_path=custom_path,
    )

    assert result.config.interval_seconds == 120
