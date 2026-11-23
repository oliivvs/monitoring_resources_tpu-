
"""N25: конфигурация Compose и штатная остановка worker."""

from __future__ import annotations

import signal
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from scripts import run_heartbeat_worker as runner


ROOT = Path(__file__).resolve().parents[1]


def test_compose_overlay_uses_existing_database_and_migration():
    base = yaml.safe_load(
        (ROOT / "compose.yaml").read_text(encoding="utf-8")
    )
    overlay = yaml.safe_load(
        (ROOT / "compose.heartbeat.yaml").read_text(
            encoding="utf-8"
        )
    )

    assert "migrate" in base["services"]
    assert "monitoring-data" in base["volumes"]

    worker = overlay["services"]["heartbeat"]

    assert (
        worker["depends_on"]["migrate"]["condition"]
        == "service_completed_successfully"
    )
    assert "monitoring-data:/data" in worker["volumes"]
    assert worker["command"] == [
        "python",
        "-m",
        "scripts.run_heartbeat_worker",
        "--database",
        "/data/monitoring.db",
        "--loop",
        "--poll-seconds",
        "10",
    ]

    assert worker["restart"] == "unless-stopped"
    assert worker["init"] is True
    assert "ports" not in worker
    assert "MONITORING_EVENT_TOKEN" not in worker.get(
        "environment", {}
    )


class FakeLoop:
    def __init__(self, *, unsupported=False):
        self.unsupported = unsupported
        self.handlers = {}
        self.removed = []

    def add_signal_handler(self, sig, callback):
        if self.unsupported:
            raise NotImplementedError

        self.handlers[sig] = callback

    def remove_signal_handler(self, sig):
        self.removed.append(sig)
        self.handlers.pop(sig)
        return True


class FakeWorker:
    def __init__(self, *, fail=False):
        self.stopped = False
        self.executed = False
        self.fail = fail
        self.on_run = None

    def stop(self):
        self.stopped = True

    async def run(self):
        self.executed = True

        if self.on_run is not None:
            self.on_run()

        if self.fail:
            raise RuntimeError("Тестовый сбой")


@pytest.mark.asyncio
async def test_sigterm_requests_graceful_shutdown():
    loop = FakeLoop()
    worker = FakeWorker()

    worker.on_run = lambda: loop.handlers[
        signal.SIGTERM
    ]()

    await runner.run_worker(worker, loop=loop)

    assert worker.executed
    assert worker.stopped

    assert set(loop.removed) == {
        signal.SIGTERM,
        signal.SIGINT,
    }
    assert not loop.handlers


@pytest.mark.asyncio
async def test_unsupported_signal_handlers_do_not_block_worker():
    loop = FakeLoop(unsupported=True)
    worker = FakeWorker()

    await runner.run_worker(worker, loop=loop)

    assert worker.executed
    assert worker.stopped
    assert loop.removed == []


@pytest.mark.asyncio
async def test_signal_handlers_removed_after_exception():
    loop = FakeLoop()
    worker = FakeWorker(fail=True)

    with pytest.raises(
        RuntimeError,
        match="Тестовый сбой",
    ):
        await runner.run_worker(worker, loop=loop)

    assert worker.stopped
    assert set(loop.removed) == {
        signal.SIGINT,
        signal.SIGTERM,
    }


def test_cli_mode_arguments_are_exclusive():
    parser = runner.build_parser()

    args = parser.parse_args([
        "--database",
        "a.sqlite",
    ])

    assert not args.loop
    assert not args.once
    assert args.poll_seconds == 10.0

    with pytest.raises(SystemExit):
        parser.parse_args([
            "--database",
            "a.sqlite",
            "--once",
            "--loop",
        ])


def test_cli_once_closes_engine(
    tmp_path,
    monkeypatch,
    capsys,
):
    database = tmp_path / "empty.sqlite"
    database.touch()

    flags = {"disposed": False}

    class FakeEngine:
        def dispose(self):
            flags["disposed"] = True

    class OnePassWorker:
        def __init__(self, engine, *, poll_seconds):
            assert poll_seconds == 10.0

        async def run_once(self):
            return SimpleNamespace(
                scanned=0,
                queued=0,
            )

    monkeypatch.setattr(
        runner,
        "make_engine",
        lambda db: FakeEngine(),
    )
    monkeypatch.setattr(
        runner,
        "HeartbeatWorker",
        OnePassWorker,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_heartbeat_worker",
            "--database",
            str(database),
            "--once",
        ],
    )

    assert runner.main() == 0
    assert flags["disposed"]

    output = capsys.readouterr().out
    assert "Добавлено HEARTBEAT: 0" in output
