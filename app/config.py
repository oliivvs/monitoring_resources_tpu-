
from pydantic import AnyHttpUrl, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Все токены читаются из окружения; никогда не выводить в логи."""
    model_config = SettingsConfigDict(
        env_prefix="MONITORING_",
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    database_url: str = "sqlite:///monitoring.db"
    app_name: str = "Мониторинг ресурсов ТПУ"
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    app_log_level: str = Field(
        default="info", pattern=r"^(debug|info|warning|error|critical)$"
    )

    # Исходящий Push к ОКО (уже существует в текущем MVP).
    event_post_url: AnyHttpUrl | None = None
    event_token: SecretStr | None = None
    event_timeout_seconds: float = Field(default=5.0, gt=0, le=60)

    # Новые, независимые секреты для входящих интеграций.
    read_token: SecretStr | None = None
    tpu_sync_token: SecretStr | None = None
