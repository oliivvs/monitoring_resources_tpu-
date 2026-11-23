"""A25.7.1: Entity API outage -> RETRY -> recovery."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import time

from pathlib import Path
from uuid import uuid4


def run_phase(phase: str) -> None:
    """Выполняется внутри отдельного Docker-контейнера."""

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
        wait_processed,
    )

    engine = make_engine(
        make_url(AppSettings().database_url).database
    )

    def check_resource(version, interval, sequence, name):
        state = read_resource(engine)

        assert state is not None, "Ресурс отсутствует"
        assert state["version_number"] == version, state
        assert state["interval_seconds"] == interval, state
        assert state["last_sequence"] == sequence, state
        assert state["name"] == name, state

    def find_update():
        with read_transaction(engine) as conn:
            row = conn.execute(
                text("""
                    SELECT event_id, processing_status,
                           attempts, last_error
                    FROM inbox_events
                    WHERE source_system = 'tpu-oracle-sync'
                      AND source_entity = 'servers'
                      AND source_record_id = :record_id
                      AND source_sequence = 7
                    ORDER BY received_at DESC
                    LIMIT 1
                """),
                {"record_id": SOURCE_ID},
            ).mappings().one_or_none()

        return dict(row) if row else None

    try:
        with httpx.Client(
            timeout=5,
            trust_env=False,
        ) as client:
            wait_api(client)

            if phase == "seed":
                assert read_resource(engine) is None, (
                    "Лабораторная база должна быть пустой"
                )

                event = make_event(
                    6,
                    1,
                    {
                        "name": "Сервер до обновления",
                        "target": "https://1.1.1.1/health",
                        "check_type": "HTTP",
                        "active": True,
                        "port": None,
                        "interval_seconds": 60,
                        "timeout_seconds": 5.0,
                        "expected_http_codes": [
                            {"start": 200, "end": 299}
                        ],
                        "up_after": 2,
                        "down_after": 3,
                        "heartbeat_interval_seconds": 300,
                    },
                )

                event_id = send_event(client, event)
                wait_processed(engine, event_id)

                check_resource(
                    1, 60, 6, "Сервер до обновления"
                )

                print("SEED INSERT: PROCESSED", flush=True)

            elif phase == "outage":
                check_resource(
                    1, 60, 6, "Сервер до обновления"
                )

                event = make_event(
                    7,
                    3,
                    {
                        "name": "Обновлённый сервер Docker"
                    },
                )

                send_event(client, event)

                deadline = time.monotonic() + 20

                while time.monotonic() < deadline:
                    row = find_update()

                    if row is not None:
                        if row["processing_status"] == "FAILED":
                            raise RuntimeError(
                                f"Неожиданный FAILED: {row}"
                            )

                        if (
                            row["processing_status"] == "PENDING"
                            and row["attempts"] >= 1
                            and row["last_error"]
                            == "EntityApiTransportError"
                        ):
                            break

                    time.sleep(0.2)
                else:
                    raise RuntimeError(
                        "Не дождались RETRY после отказа API"
                    )

                check_resource(
                    1, 60, 6, "Сервер до обновления"
                )

                print(
                    "SNAPSHOT OUTAGE: RETRY",
                    flush=True,
                )
                print(
                    "SQLITE UNCHANGED: OK",
                    flush=True,
                )

            elif phase == "recover":
                row = find_update()

                assert row is not None, (
                    "Событие UPDATE не найдено"
                )

                result = wait_processed(
                    engine,
                    row["event_id"],
                )

                assert result["attempts"] >= 2

                check_resource(
                    2,
                    90,
                    7,
                    "Обновлённый сервер Docker",
                )

                print(
                    "SNAPSHOT RECOVERY: PROCESSED",
                    flush=True,
                )
                print(
                    "SQLITE UPDATED: version=2, sequence=7",
                    flush=True,
                )

            else:
                raise ValueError(
                    f"Неизвестный этап: {phase}"
                )

    finally:
        engine.dispose()


def run_docker_test() -> None:
    """Создаёт и удаляет изолированный Docker-проект."""

    tls_directory = os.environ.get(
        "ORACLE_LAB_TLS_DIR", ""
    )

    tls_path = Path(tls_directory)

    if (
        not tls_path.is_absolute()
        or not all(
            (tls_path / filename).is_file()
            for filename in (
                "ca.crt",
                "server.crt",
                "server.key",
            )
        )
    ):
        raise SystemExit(
            "Сначала настрой ORACLE_LAB_TLS_DIR "
            "с действительными сертификатами"
        )

    subprocess.run(
        [
            "openssl", "x509",
            "-in", str(tls_path / "server.crt"),
            "-noout", "-checkend", "600",
        ],
        check=True,
    )

    project = "tpu-a25-neg-" + uuid4().hex[:8]
    api_container = project + "-api"

    env = os.environ.copy()

    env.update({
        "ORACLE_LAB_SNAPSHOT_TOKEN": (
            secrets.token_urlsafe(32)
        ),
        "MONITORING_TPU_SYNC_TOKEN": (
            secrets.token_urlsafe(32)
        ),
        "MONITORING_EVENT_TOKEN": (
            secrets.token_urlsafe(32)
        ),
        "MONITORING_ORACLE_ENABLED": "true",
        "MONITORING_ORACLE_ALLOWED_HOSTS": "1.1.1.1",
        "MONITORING_ORACLE_ALLOWED_PORTS": "443",
        "ORACLE_LAB_SNAPSHOT_SEQUENCE": "7",
        "ORACLE_LAB_SNAPSHOT_INTERVAL_SECONDS": "90",
        "MONITORING_ORACLE_SNAPSHOT_RECORDS_PATH": "",
        "MONITORING_ORACLE_SNAPSHOT_CONTRACT_PATH": "",
    })

    compose = [
        "docker", "compose",
        "-p", project,
        "-f", "compose.yaml",
        "-f", "compose.oracle-lab.yaml",
    ]

    def docker(*arguments):
        subprocess.run(
            [*compose, *arguments],
            env=env,
            check=True,
        )

    def phase(name):
        print(f"\n=== A25.7: {name} ===", flush=True)

        docker(
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "-e",
            f"A25_E2E_API_URL=http://{api_container}:8000",
            "api",
            "python",
            "-m",
            "scripts.a25_negative_e2e",
            name,
        )

    try:
        print("Docker project:", project, flush=True)

        docker("config", "--quiet")

        docker(
            "up", "-d", "--build",
            "oracle-snapshot",
            "oracle-worker",
        )

        docker(
            "run", "-d",
            "--no-deps",
            "--name", api_container,
            "api",
        )

        phase("seed")

        # Имитируем полный отказ Entity API.
        docker("stop", "oracle-snapshot")

        phase("outage")

        # Возвращаем Entity API.
        docker("start", "oracle-snapshot")

        # Worker самостоятельно повторит обработку.
        phase("recover")

        print(
            "\nA25.7.1 OUTAGE + RECOVERY: PASSED",
            flush=True,
        )

    except subprocess.CalledProcessError:
        print(
            "\nДиагностика Oracle Worker:",
            flush=True,
        )

        subprocess.run(
            [
                *compose,
                "logs",
                "--tail", "40",
                "oracle-worker",
                "oracle-snapshot",
            ],
            env=env,
            check=False,
        )

        raise

    finally:
        subprocess.run(
            [
                "docker", "rm", "-f",
                api_container,
            ],
            check=False,
            capture_output=True,
        )

        subprocess.run(
            [
                *compose,
                "--profile", "oracle-lab",
                "down", "-v",
                "--remove-orphans",
            ],
            env=env,
            check=False,
        )


if __name__ == "__main__":
    if len(sys.argv) == 1:
        run_docker_test()

    elif (
        len(sys.argv) == 2
        and sys.argv[1] in {
            "seed",
            "outage",
            "recover",
        }
    ):
        run_phase(sys.argv[1])

    else:
        raise SystemExit(
            "Использование: python -m "
            "scripts.a25_negative_e2e "
            "[seed|outage|recover]"
        )
