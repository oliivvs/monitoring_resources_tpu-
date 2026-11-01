
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel

from app.config import AppSettings


# Настройки приложения
settings = AppSettings()


# Модель ответа проверки доступности
class HealthResponse(BaseModel):
    status: Literal["ok"]


# Создание FastAPI-приложения
app = FastAPI(
    title=settings.app_name,
    description="Сервис мониторинга сетевых ресурсов ТПУ",
    version="0.1.0",
)


@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["Служебные"],
    summary="Проверка доступности API",
)
def health() -> HealthResponse:
    return HealthResponse(status="ok")
