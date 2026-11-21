
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p test_reports

echo "=== 1. Проверка синтаксиса ==="
python3 -m compileall -q \
  app checks db integrations migrations \
  mock_api mock_entity_api scripts services shared tests

echo "=== 2. Все автоматические тесты ==="
python3 -m pytest -q \
  --junitxml=test_reports/pytest_all.xml \
  | tee test_reports/pytest_all.txt

echo "=== 3. Локальный HTTP и Outbox ==="
python3 -m scripts.local_smoke \
  | tee test_reports/local_smoke.txt

echo "=== 4. Oracle Sync и защищённый GET ==="
python3 -m scripts.demo_artemiy \
  | tee test_reports/artemiy_demo.txt

echo "=== 5. Миграции и целостность SQLite ==="

TMP_DIR="$(mktemp -d)"
PROJECT=""

cleanup() {
  if [[ -n "$PROJECT" ]]; then
    docker compose -p "$PROJECT" down -v \
      --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

export MONITORING_DATABASE_URL="sqlite:///$TMP_DIR/test.sqlite"

python3 -m alembic upgrade head \
  | tee test_reports/alembic.txt

python3 - <<'PY'
import os
import sqlite3

path = os.environ["MONITORING_DATABASE_URL"].removeprefix("sqlite:///")
conn = sqlite3.connect(path)

tables = {
    row[0]
    for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
}

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
}

assert required <= tables, f"Отсутствуют: {required - tables}"
assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
assert not conn.execute("PRAGMA foreign_key_check").fetchall()
assert conn.execute(
    "SELECT version_num FROM alembic_version"
).fetchone()[0] == "003_tpu_inbox"

print("PASS: 10 таблиц, внешние ключи, миграции")
conn.close()
PY

if [[ "${1:-}" == "--docker" ]]; then
  echo "=== 6. Docker: сквозное тестирование ==="

  PROJECT="tpu-test-$(python3 -c 'import secrets;print(secrets.token_hex(4))')"

  export MONITORING_EVENT_TOKEN="$(python3 -c 'import secrets;print(secrets.token_urlsafe(32))')"
  export MONITORING_READ_TOKEN="$(python3 -c 'import secrets;print(secrets.token_urlsafe(32))')"
  export MONITORING_TPU_SYNC_TOKEN="$(python3 -c 'import secrets;print(secrets.token_urlsafe(32))')"

  unset MONITORING_DATABASE_URL

  docker compose -p "$PROJECT" up -d --build

  docker compose -p "$PROJECT" exec -T scheduler \
    python -m scripts.docker_e2e \
    | tee test_reports/docker_e2e.txt

  STATE_QUERY="import sqlite3; c=sqlite3.connect('/data/monitoring.db'); print(c.execute(\"SELECT count(*) FROM outbox_events WHERE delivery_status='SENT'\").fetchone()[0]); c.close()"

  BEFORE="$(docker compose -p "$PROJECT" exec -T api python -c "$STATE_QUERY")"

  echo "=== 7. Docker: сохранность после перезапуска ==="
  docker compose -p "$PROJECT" restart api sender scheduler

  AFTER="$(docker compose -p "$PROJECT" exec -T api python -c "$STATE_QUERY")"

  [[ "$BEFORE" == "$AFTER" ]]
  [[ "$AFTER" =~ ^[0-9]+$ ]]
  [[ "$AFTER" -ge 1 ]]

  echo "PASS: после перезапуска доставленных событий $AFTER" \
    | tee test_reports/docker_persistence.txt
fi

echo "PASS: все выбранные проверки завершились"
echo "Отчёты находятся в test_reports/"
