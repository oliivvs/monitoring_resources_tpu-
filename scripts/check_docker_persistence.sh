#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

echo "1/4 Запускаем новый E2E-тест..."

output="$(docker compose exec -T scheduler python -m scripts.docker_e2e)"
printf '%s\n' "$output"

# Получаем ID созданного тестового ресурса.
resource_id="$(
    printf '%s\n' "$output" |
    sed -n 's/^Тестовый ресурс: //p' |
    tail -n 1
)"

if [[ ! "$resource_id" =~ ^a09-e2e-[0-9a-f]{8}$ ]]; then
    echo "Не удалось определить ID тестового ресурса" >&2
    exit 1
fi

# Читаем результат именно нашего теста.
snapshot() {
    docker compose exec -T \
        -e A09_RESOURCE_ID="$resource_id" \
        api python - <<'PY'
import json
import os

from sqlalchemy import text
from app.database import get_engine

resource_id = os.environ["A09_RESOURCE_ID"]

with get_engine().connect() as conn:
    resource = conn.execute(
        text("""
            SELECT
                r.id,
                r.active,
                s.current_status,
                (
                    SELECT COUNT(*)
                    FROM check_results
                    WHERE resource_id = r.id
                ) AS checks
            FROM resources r
            JOIN resource_states s
                ON s.resource_id = r.id
            WHERE r.id = :rid
        """),
        {"rid": resource_id},
    ).mappings().one()

    event = conn.execute(
        text("""
            SELECT
                event_id,
                delivery_status,
                attempts
            FROM outbox_events
            WHERE resource_id = :rid
            ORDER BY created_at DESC
            LIMIT 1
        """),
        {"rid": resource_id},
    ).mappings().one()

    http_code = conn.execute(
        text("""
            SELECT http_status_code
            FROM delivery_attempts
            WHERE event_id = :event_id
            ORDER BY attempt_number DESC
            LIMIT 1
        """),
        {"event_id": event["event_id"]},
    ).scalar_one()

result = {
    "resource_id": resource["id"],
    "active": resource["active"],
    "status": resource["current_status"],
    "checks": resource["checks"],
    "event_id": event["event_id"],
    "delivery": event["delivery_status"],
    "attempts": event["attempts"],
    "http_code": http_code,
}

assert result["active"] == 0
assert result["status"] == "UP"
assert result["checks"] >= 2
assert result["delivery"] == "SENT"
assert result["attempts"] >= 1
assert result["http_code"] == 202

print(json.dumps(result, sort_keys=True))
PY
}

echo "2/4 Сохраняем состояние до перезапуска..."
before="$(snapshot)"

echo "3/4 Пересоздаём контейнеры..."
docker compose down
docker compose up -d

echo "4/4 Проверяем состояние после перезапуска..."
after="$(snapshot)"

if [[ "$before" != "$after" ]]; then
    echo "PERSISTENCE FAILED: данные изменились" >&2
    exit 1
fi

echo "PERSISTENCE OK: SQLite сохранила тестовый ресурс"
echo "Ресурс: $resource_id"
