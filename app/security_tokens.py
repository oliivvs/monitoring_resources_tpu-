
"""Раздельная Bearer-аутентификация внешних интеграций.

EVENT_TOKEN отправителя ОКО намеренно не используется в этих обработчиках.
"""
from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import AppSettings

_read_bearer = HTTPBearer(auto_error=False, scheme_name="ReadBearer")
_sync_bearer = HTTPBearer(auto_error=False, scheme_name="TpuSyncBearer")


def get_settings() -> AppSettings:
    """Отдельная зависимость, чтобы тесты не читали реальный .env."""
    return AppSettings()


def _require_token(
    credentials: HTTPAuthorizationCredentials | None,
    configured_token: str | None,
) -> None:
    # Fail-closed: если токен не задан, интеграция недоступна.
    if not configured_token or not configured_token.strip():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Токен интеграции не настроен",
        )

    if (
        credentials is None
        or credentials.scheme.lower() != "bearer"
        or not secrets.compare_digest(credentials.credentials, configured_token)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверные данные авторизации",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_read_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(_read_bearer),
    settings: AppSettings = Depends(get_settings),
) -> None:
    token = settings.read_token.get_secret_value() if settings.read_token else None
    _require_token(credentials, token)


def require_tpu_sync_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(_sync_bearer),
    settings: AppSettings = Depends(get_settings),
) -> None:
    token = settings.tpu_sync_token.get_secret_value() if settings.tpu_sync_token else None
    _require_token(credentials, token)
