"""A25.3: подключение YAML-контракта к Oracle Worker."""

from pathlib import Path

import pytest
import yaml

from integrations.tpu.entity_client import EntityRoutes
from scripts.run_inbox_worker import (
    OracleWorkerSettings,
    build_snapshot_client,
    build_worker,
)


ROOT = Path(__file__).resolve().parents[1]
MAPPING = ROOT / "config/tpu_oracle_mapping.yaml"
CONTRACT = ROOT / "config/tpu_entity_contract.yaml"


def settings(**changes):
    values = {
        "enabled": True,
        "mapping_path": MAPPING,
        "allowed_hosts": "example.org",
        "allowed_ports": "443",
        "snapshot_enabled": True,
        "snapshot_base_url": "https://entity.example.org",
        "snapshot_allowed_hosts": "entity.example.org",
        "snapshot_token": "test-only-token",
    }

    values.update(changes)

    return OracleWorkerSettings(
        _env_file=None,
        **values,
    )


@pytest.fixture
def contract_file(tmp_path):
    data = yaml.safe_load(
        CONTRACT.read_text(encoding="utf-8")
    )

    data["records_path"] = (
        "/api/v2/entities/{entity_id}/records"
    )

    path = tmp_path / "contract.yaml"
    path.write_text(
        yaml.safe_dump(
            data,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    return path


def modify_contract(path, **changes):
    data = yaml.safe_load(
        path.read_text(encoding="utf-8")
    )

    data.update(changes)

    path.write_text(
        yaml.safe_dump(
            data,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )


def test_worker_uses_route_from_yaml(contract_file):
    client = build_snapshot_client(
        settings(
            snapshot_contract_path=contract_file,
        )
    )

    assert client._routes.records == (
        "/api/v2/entities/{entity_id}/records"
    )


def test_worker_rejects_conflicting_route(contract_file):
    with pytest.raises(
        ValueError,
        match="records_path",
    ):
        build_snapshot_client(
            settings(
                snapshot_contract_path=contract_file,
                snapshot_records_path=(
                    "/other/{entity_id}/records"
                ),
            )
        )


def test_worker_rejects_wrong_entity(contract_file):
    modify_contract(
        contract_file,
        source_entity="another_entity",
    )

    with pytest.raises(
        ValueError,
        match="source_entity",
    ):
        build_snapshot_client(
            settings(
                snapshot_contract_path=contract_file,
            )
        )


def test_worker_rejects_wrong_identity_field(contract_file):
    modify_contract(
        contract_file,
        identity_field="another_id",
    )

    with pytest.raises(
        ValueError,
        match="identity_field",
    ):
        build_snapshot_client(
            settings(
                snapshot_contract_path=contract_file,
            )
        )


def test_worker_rejects_unsupported_response(contract_file):
    modify_contract(
        contract_file,
        response_items_field="data",
    )

    with pytest.raises(
        ValueError,
        match="Формат пока не поддерживается",
    ):
        build_snapshot_client(
            settings(
                snapshot_contract_path=contract_file,
            )
        )


def test_worker_rejects_missing_contract(tmp_path):
    with pytest.raises(FileNotFoundError):
        build_snapshot_client(
            settings(
                snapshot_contract_path=(
                    tmp_path / "missing.yaml"
                ),
            )
        )


def test_default_route_remains_unchanged():
    client = build_snapshot_client(settings())

    assert client._routes.records == (
        EntityRoutes.local_mock().records
    )


def test_build_worker_uses_yaml_contract(
    contract_file,
    tmp_path,
):
    from db.session import initialize, make_engine

    engine = make_engine(tmp_path / "worker.sqlite")
    initialize(engine)

    try:
        worker = build_worker(
            engine,
            settings(
                snapshot_contract_path=contract_file,
            ),
        )

        handler = worker._prepare.__self__
        client = handler._snapshot_client

        assert client is not None
        assert client._routes.records == (
            "/api/v2/entities/{entity_id}/records"
        )

    finally:
        engine.dispose()
