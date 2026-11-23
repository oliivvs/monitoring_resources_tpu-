"""A25.7.2: wrong sequence and HTTP 401 Docker E2E."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import tempfile
import time

from pathlib import Path
from uuid import uuid4


CASES = {
    "sequence": ("PENDING", "OracleSnapshotNotReady"),
    "unauthorized": ("FAILED", "OracleInboxError"),
}


def verify_failure():
    import httpx
    from sqlalchemy import text
    from sqlalchemy.engine import make_url

    from app.config import AppSettings
    from db.session import make_engine, read_transaction
    from scripts.a25_docker_e2e import (
        API_URL,
        SOURCE_ID,
        make_event,
        read_resource,
        send_event,
        wait_api,
    )

    case = os.environ["A25_FAILURE_CASE"]
    expected_status, expected_error = CASES[case]

    engine = make_engine(
        make_url(AppSettings().database_url).database
    )

    def read_event(event_id):
        with read_transaction(engine) as conn:
            row = conn.execute(
                text("""
                    SELECT processing_status, attempts, last_error
                    FROM inbox_events
                    WHERE event_id = :event_id
                """),
                {"event_id": event_id},
            ).mappings().one_or_none()

        return dict(row) if row is not None else None

    def check_original_resource():
        resource = read_resource(engine)

        assert resource is not None
        assert resource["version_number"] == 1, resource
        assert resource["interval_seconds"] == 60, resource
        assert resource["last_sequence"] == 6, resource
        assert resource["name"] == "Сервер до обновления"

    try:
        with httpx.Client(
            timeout=5,
            trust_env=False,
        ) as client:
            wait_api(client)
            check_original_resource()

            update = make_event(
                7,
                3,
                {
                    "name": "Обновлённый сервер Docker"
                },
            )

            event_id = send_event(client, update)

            deadline = time.monotonic() + 25

            while time.monotonic() < deadline:
                row = read_event(event_id)

                if row is not None:
                    if (
                        row["processing_status"] == expected_status
                        and row["last_error"] == expected_error
                        and row["attempts"] == 1
                    ):
                        break

                    if (
                        row["processing_status"] in {
                            "FAILED",
                            "PROCESSED",
                        }
                        and row["processing_status"] != expected_status
                    ):
                        raise AssertionError(
                            f"Неожиданный результат: {row}"
                        )

                time.sleep(0.2)

            else:
                raise RuntimeError(
                    f"Не дождались {expected_status}: "
                    f"{read_event(event_id)}"
                )

            check_original_resource()

            print(
                f"{case.upper()}: {expected_status}",
                flush=True,
            )
            print(
                f"ERROR TYPE: {expected_error}",
                flush=True,
            )
            print(
                "ATTEMPTS: 1",
                flush=True,
            )
            print(
                "SQLITE UNCHANGED: OK",
                flush=True,
            )

    finally:
        engine.dispose()


def run_host():
    tls_dir = Path(
        os.environ.get("ORACLE_LAB_TLS_DIR", "")
    )

    if (
        not tls_dir.is_absolute()
        or not all(
            (tls_dir / name).is_file()
            for name in (
                "ca.crt",
                "server.crt",
                "server.key",
            )
        )
    ):
        raise SystemExit(
            "Не найдены лабораторные TLS-сертификаты"
        )

    subprocess.run(
        [
            "openssl", "x509",
            "-in", str(tls_dir / "server.crt"),
            "-noout", "-checkend", "600",
        ],
        check=True,
    )

    for case in CASES:
        project = (
            "tpu-a25-fail-" + uuid4().hex[:8]
        )
        api_name = project + "-api"

        environment = os.environ.copy()

        lab_token = secrets.token_urlsafe(32)

        worker_token = (
            lab_token
            if case == "sequence"
            else secrets.token_urlsafe(32)
        )

        environment.update({
            "ORACLE_LAB_SNAPSHOT_TOKEN": lab_token,
            "A25_WORKER_SNAPSHOT_TOKEN": worker_token,
            "MONITORING_TPU_SYNC_TOKEN": secrets.token_urlsafe(32),
            "MONITORING_EVENT_TOKEN": secrets.token_urlsafe(32),
            "MONITORING_ORACLE_ENABLED": "true",
            "MONITORING_ORACLE_ALLOWED_HOSTS": "1.1.1.1",
            "MONITORING_ORACLE_ALLOWED_PORTS": "443",
            "ORACLE_LAB_SNAPSHOT_SEQUENCE": (
                "8" if case == "sequence" else "7"
            ),
            "ORACLE_LAB_SNAPSHOT_INTERVAL_SECONDS": "90",
            "A25_FAILURE_CASE": case,
            "MONITORING_ORACLE_SNAPSHOT_RECORDS_PATH": "",
            "MONITORING_ORACLE_SNAPSHOT_CONTRACT_PATH": "",
        })

        with tempfile.TemporaryDirectory(
            prefix="tpu-a25-fault-"
        ) as directory:
            override = (
                Path(directory) / "compose.fault.yaml"
            )

            override.write_text(
                "services:\n"
                "  oracle-worker:\n"
                "    environment:\n"
                "      MONITORING_ORACLE_SNAPSHOT_TOKEN: "
                "${A25_WORKER_SNAPSHOT_TOKEN:?}\n"
                "      MONITORING_ORACLE_RETRY_BASE_SECONDS: 60\n",
                encoding="utf-8",
            )

            compose = [
                "docker", "compose",
                "-p", project,
                "-f", "compose.yaml",
                "-f", "compose.oracle-lab.yaml",
                "-f", str(override),
            ]

            def docker(*args):
                subprocess.run(
                    [*compose, *args],
                    env=environment,
                    check=True,
                )

            def phase(module, name):
                docker(
                    "run", "--rm", "--no-deps", "-T",
                    "-e",
                    f"A25_E2E_API_URL=http://{api_name}:8000",
                    "-e",
                    f"A25_FAILURE_CASE={case}",
                    "api",
                    "python", "-m", module, name,
                )

            print(
                f"\n=== A25.7.2: {case} ===",
                flush=True,
            )
            print(
                "Docker project:", project,
                flush=True,
            )

            try:
                docker("config", "--quiet")

                docker(
                    "up", "-d", "--build",
                    "oracle-snapshot",
                    "oracle-worker",
                )

                docker(
                    "run", "-d", "--no-deps",
                    "--name", api_name,
                    "api",
                )

                # Создаём ресурс версии 1.
                phase(
                    "scripts.a25_negative_e2e",
                    "seed",
                )

                # Отправляем UPDATE и проверяем отказ.
                phase(
                    "scripts.a25_failure_cases_e2e",
                    "verify",
                )

                print(
                    f"A25.7.2 {case.upper()}: PASSED",
                    flush=True,
                )

            except subprocess.CalledProcessError:
                subprocess.run(
                    [
                        *compose, "logs",
                        "--tail", "50",
                        "oracle-worker",
                        "oracle-snapshot",
                    ],
                    env=environment,
                    check=False,
                )
                raise

            finally:
                subprocess.run(
                    ["docker", "rm", "-f", api_name],
                    capture_output=True,
                    check=False,
                )

                subprocess.run(
                    [
                        *compose,
                        "--profile", "oracle-lab",
                        "down", "-v", "--remove-orphans",
                    ],
                    env=environment,
                    check=False,
                )

    print(
        "\nA25.7.2 NEGATIVE DOCKER E2E: PASSED",
        flush=True,
    )


if __name__ == "__main__":
    if len(sys.argv) == 1:
        run_host()
    elif len(sys.argv) == 2 and sys.argv[1] == "verify":
        verify_failure()
    else:
        raise SystemExit("Неверные аргументы")
