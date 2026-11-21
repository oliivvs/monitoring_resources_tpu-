
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel

from app.config import AppSettings
from app.routes.resources import router as resources_router
from app.routes.statuses import router as statuses_router
from app.routes.tpu_events import router as tpu_events_router

settings = AppSettings()


class HealthResponse(BaseModel):
    status: Literal["ok"]


app = FastAPI(
    title=settings.app_name,
    description="Сервис мониторинга сетевых ресурсов ТПУ",
    version="0.2.0",
)


@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["Служебные"],
    summary="Проверка доступности API",
)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


app.include_router(resources_router)
app.include_router(statuses_router)
app.include_router(tpu_events_router)
