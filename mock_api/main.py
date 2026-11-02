
"""Тестовый HTTP-получатель событий мониторинга ТПУ."""

from hmac import compare_digest
from threading import Lock
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Response, status

from app.config import AppSettings
from shared.schemas import StatusEventPayload


def create_mock_app(settings: AppSettings | None = None) -> FastAPI:
    settings = settings if settings is not None else AppSettings()

    app = FastAPI(
        title="ТПУ — Mock Events API",
        version="0.1.0",
    )

    # Для локальной демонстрации храним события в памяти.
    received: dict[UUID, StatusEventPayload] = {}
    lock = Lock()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post(
        "/api/v1/status/events",
        status_code=status.HTTP_202_ACCEPTED,
    )
    def receive_event(
        event: StatusEventPayload,
        response: Response,
        authorization: str | None = Header(default=None),
    ):
        # Токен берётся из конфигурации, а не из исходного кода.
        configured = settings.event_token

        if configured is None or not configured.get_secret_value().strip():
            raise HTTPException(
                status_code=503,
                detail="Mock API token is not configured",
            )

        scheme, _, token = (authorization or "").partition(" ")

        if (
            scheme.lower() != "bearer"
            or not compare_digest(
                token,
                configured.get_secret_value(),
            )
        ):
            raise HTTPException(
                status_code=401,
                detail="Unauthorized",
                headers={"WWW-Authenticate": "Bearer"},
            )

        with lock:
            previous = received.get(event.event_id)

            if previous is not None:
                if previous != event:
                    raise HTTPException(
                        status_code=409,
                        detail="event_id already used",
                    )

                # Точное повторение не создаёт новое событие.
                response.status_code = 200
                return {
                    "event_id": str(event.event_id),
                    "result": "duplicate",
                }

            received[event.event_id] = event

        return {
            "event_id": str(event.event_id),
            "result": "accepted",
        }

    return app


app = create_mock_app()
