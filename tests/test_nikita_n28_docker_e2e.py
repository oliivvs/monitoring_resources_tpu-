
"""N28: защита Docker E2E от случайного запуска."""

import pytest

from scripts.n28_docker_e2e import check_environment


def test_n28_disabled_by_default(monkeypatch):
    monkeypatch.delenv(
        "N28_E2E_ENABLED",
        raising=False,
    )

    with pytest.raises(RuntimeError, match="запрещён"):
        check_environment()


def test_n28_rejects_other_database(monkeypatch):
    monkeypatch.setenv(
        "N28_E2E_ENABLED",
        "true",
    )
    monkeypatch.setenv(
        "MONITORING_DATABASE_URL",
        "sqlite:///monitoring.db",
    )

    with pytest.raises(RuntimeError, match="тестовую SQLite"):
        check_environment()
