"""Учебный Entity API для проверки Oracle Sync в Docker.

Не подключается к инфраструктуре ТПУ.
Возвращает только одну заранее определённую тестовую запись.
"""

from __future__ import annotations

import os
import re

from hmac import compare_digest

from fastapi import (
    FastAPI,
    Header,
    HTTPException,
    Query,
)


RECORD_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


def create_snapshot_app(
    *,
    token: str | None = None,
    record_id: str | None = None,
) -> FastAPI:
    configured_token = (
        token
        if token is not None
        else os.environ.get("ORACLE_LAB_SNAPSHOT_TOKEN", "")
    )

    configured_record_id = (
        record_id
        if record_id is not None
        else os.environ.get(
            "ORACLE_LAB_RECORD_ID",
            "docker-lab-snapshot-001",
        )
    )

    if not RECORD_ID_PATTERN.fullmatch(configured_record_id):
        raise ValueError("Некорректный тестовый record_id")

    application = FastAPI(
        title="Oracle Sync - Lab Entity API",
        version="0.1.0",
    )

    @application.get("/health")
    def health():
        return {"status": "ok"}

    @application.get(
        "/mock/tpu/entity/v1/entities/{entity_id}/records"
    )
    def get_records(
        entity_id: str,
        source_record_id: str | None = None,
        limit: int = Query(default=50, ge=1, le=100),
        authorization: str | None = Header(default=None),
    ):
        if not configured_token:
            raise HTTPException(
                status_code=503,
                detail="Lab token not configured",
            )

        scheme, _, supplied_token = (
            authorization or ""
        ).partition(" ")

        if (
            scheme.lower() != "bearer"
            or not compare_digest(
                supplied_token,
                configured_token,
            )
        ):
            raise HTTPException(
                status_code=401,
                detail="Unauthorized",
            )

        if entity_id != "servers":
            raise HTTPException(
                status_code=404,
                detail="Entity not found",
            )

        if source_record_id != configured_record_id:
            return {
                "items": [],
                "next_cursor": None,
            }

        return {
            "items": [
                {
                    "source_record_id": configured_record_id,
                    "source_sequence": 2,
                    "name": "Обновлённый сервер Docker",
                    "target": "https://1.1.1.1/health",
                    "check_type": "HTTP",
                    "active": True,
                    "port": None,
                    "interval_seconds": 120,
                    "timeout_seconds": 5.0,
                    "expected_http_codes": [
                        {"start": 200, "end": 299}
                    ],
                    "up_after": 2,
                    "down_after": 3,
                    "heartbeat_interval_seconds": 300,
                }
            ],
            "next_cursor": None,
        }

    return application


app = create_snapshot_app()
