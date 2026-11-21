
# N24. Полная локальная проверка проекта в Windows PowerShell.
# Запуск из корня репозитория:
# .\scripts\test_full_project.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

$oldDbUrl = [Environment]::GetEnvironmentVariable(
    "MONITORING_DATABASE_URL",
    "Process"
)

$tempDir = $null

Push-Location $projectRoot

try {
    Write-Host "=== 1. Проверка синтаксиса Python ==="

    $directories = @(
        "app",
        "checks",
        "db",
        "integrations",
        "migrations",
        "mock_api",
        "mock_entity_api",
        "scripts",
        "services",
        "shared",
        "tests"
    )

    & python -m compileall -q @directories

    if ($LASTEXITCODE -ne 0) {
        throw "Ошибка проверки синтаксиса Python"
    }

    Write-Host "PASS: синтаксис"

    Write-Host "=== 2. Все тесты pytest ==="

    & python -m pytest -q

    if ($LASTEXITCODE -ne 0) {
        throw "Автоматические тесты не прошли"
    }

    Write-Host "PASS: pytest"

    Write-Host "=== 3. Проверка HTTP и Outbox ==="

    & python -m scripts.local_smoke

    if ($LASTEXITCODE -ne 0) {
        throw "Ошибка локального HTTP / Outbox"
    }

    Write-Host "PASS: локальный HTTP и Outbox"

    Write-Host "=== 4. Проверка Oracle Sync API ==="

    & python -m scripts.demo_artemiy

    if ($LASTEXITCODE -ne 0) {
        throw "Ошибка демонстрации Oracle Sync"
    }

    Write-Host "PASS: демонстрация Oracle Sync"

    Write-Host "=== 5. Проверка миграций SQLite ==="

    $tempDir = Join-Path $env:TEMP (
        "tpu_validation_" +
        [guid]::NewGuid().ToString("N")
    )

    New-Item -ItemType Directory -Path $tempDir |
        Out-Null

    $testDb = Join-Path $tempDir "test.sqlite"

    $env:MONITORING_DATABASE_URL = (
        "sqlite:///" + ($testDb -replace '\\', '/')
    )

    Write-Host "Тестовая база: $testDb"

    & python -m alembic upgrade head

    if ($LASTEXITCODE -ne 0) {
        throw "Миграции завершились с ошибкой"
    }

    & python -m alembic current

    if ($LASTEXITCODE -ne 0) {
        throw "Не удалось проверить версию миграции"
    }

    # Не используем python -c с многострочным кодом.
    # Windows PowerShell может неправильно передать кавычки.
    # Вместо этого создаём временный Python-файл.

    $verifyScript = Join-Path $tempDir "verify_sqlite.py"

    $verifyCode = @'
import os
import sqlite3

url = os.environ["MONITORING_DATABASE_URL"]

if not url.startswith("sqlite:///"):
    raise RuntimeError("Ожидался URL SQLite")

path = url.removeprefix("sqlite:///")

required = {
    "resources",
    "resource_config_versions",
    "http_expected_ranges",
    "resource_states",
    "check_results",
    "status_transitions",
    "outbox_events",
    "delivery_attempts",
    "inbox_events",
    "resource_source_mappings",
    "resource_source_sequences",
}

conn = sqlite3.connect(path)

try:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }

    missing = required - tables

    if missing:
        raise RuntimeError(
            f"Отсутствуют таблицы: {missing}"
        )

    version = conn.execute(
        "SELECT version_num FROM alembic_version"
    ).fetchone()[0]

    if version != "005_source_sequence":
        raise RuntimeError(
            f"Неверная версия миграции: {version}"
        )

    integrity = conn.execute(
        "PRAGMA integrity_check"
    ).fetchone()[0]

    if integrity != "ok":
        raise RuntimeError(
            f"Ошибка целостности: {integrity}"
        )

    foreign_key_errors = conn.execute(
        "PRAGMA foreign_key_check"
    ).fetchall()

    if foreign_key_errors:
        raise RuntimeError(
            f"Нарушены внешние ключи: {foreign_key_errors}"
        )

    print(
        "PASS: миграции 001-005, "
        "11 предметных таблиц, "
        "целостность SQLite"
    )

finally:
    conn.close()
'@

    Set-Content `
        -LiteralPath $verifyScript `
        -Value $verifyCode `
        -Encoding UTF8

    & python $verifyScript

    if ($LASTEXITCODE -ne 0) {
        throw "Ошибка проверки целостности SQLite"
    }

    Write-Host ""
    Write-Host "PASS: все локальные проверки завершены"
}
finally {
    # Всегда восстанавливаем исходную настройку БД.
    [Environment]::SetEnvironmentVariable(
        "MONITORING_DATABASE_URL",
        $oldDbUrl,
        "Process"
    )

    # Удаляем только созданную скриптом временную папку.
    if (
        $null -ne $tempDir -and
        (Test-Path -LiteralPath $tempDir)
    ) {
        Remove-Item `
            -LiteralPath $tempDir `
            -Recurse `
            -Force
    }

    Pop-Location
}
