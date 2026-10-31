-- Мониторинг ресурсов ТПУ. Схема v2 для SQLite.
-- Применять в транзакции; PRAGMA foreign_keys=ON на каждом соединении.
-- Моделирование: одна проверяемая служба = один resource_id;
-- версии конфигураций и текущее состояние отделены от идентичности ресурса.

CREATE TABLE IF NOT EXISTS resources (
    id TEXT PRIMARY KEY CHECK (length(trim(id)) BETWEEN 1 AND 128),
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 255),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resource_config_versions (
    id INTEGER PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id) ON DELETE RESTRICT,
    version_number INTEGER NOT NULL CHECK (version_number > 0),
    target TEXT NOT NULL CHECK (length(trim(target)) > 0),
    check_type TEXT NOT NULL CHECK (check_type IN ('HTTP','TCP')),
    port INTEGER,
    interval_seconds INTEGER NOT NULL DEFAULT 60 CHECK (interval_seconds >= 1),
    timeout_seconds REAL NOT NULL DEFAULT 5 CHECK (timeout_seconds > 0),
    up_after INTEGER NOT NULL DEFAULT 2 CHECK (up_after >= 1),
    down_after INTEGER NOT NULL DEFAULT 3 CHECK (down_after >= 1),
    heartbeat_interval_seconds INTEGER NOT NULL DEFAULT 300 CHECK (heartbeat_interval_seconds >= 1),
    effective_at TEXT NOT NULL,
    retired_at TEXT,
    UNIQUE(resource_id, version_number),
    UNIQUE(id, resource_id),
    CHECK (check_type <> 'TCP' OR port BETWEEN 1 AND 65535),
    CHECK (check_type <> 'HTTP' OR port IS NULL),
    CHECK (retired_at IS NULL OR retired_at > effective_at)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_config_per_resource
    ON resource_config_versions(resource_id) WHERE retired_at IS NULL;

CREATE TABLE IF NOT EXISTS http_expected_ranges (
    config_version_id INTEGER NOT NULL REFERENCES resource_config_versions(id) ON DELETE RESTRICT,
    code_start INTEGER NOT NULL CHECK (code_start BETWEEN 100 AND 599),
    code_end INTEGER NOT NULL CHECK (code_end BETWEEN 100 AND 599 AND code_end >= code_start),
    PRIMARY KEY (config_version_id, code_start, code_end)
);

CREATE TABLE IF NOT EXISTS resource_states (
    resource_id TEXT PRIMARY KEY REFERENCES resources(id) ON DELETE RESTRICT,
    config_version_id INTEGER NOT NULL,
    current_status TEXT NOT NULL DEFAULT 'UNKNOWN'
        CHECK (current_status IN ('UP','DOWN','UNKNOWN')),
    ok_count INTEGER NOT NULL DEFAULT 0 CHECK (ok_count >= 0),
    fail_count INTEGER NOT NULL DEFAULT 0 CHECK (fail_count >= 0),
    last_checked_at TEXT,
    status_changed_at TEXT,
    last_heartbeat_enqueued_at TEXT,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    updated_at TEXT NOT NULL,
    FOREIGN KEY (config_version_id, resource_id)
      REFERENCES resource_config_versions(id, resource_id) ON DELETE RESTRICT,
    CHECK (ok_count = 0 OR fail_count = 0)
);

CREATE TABLE IF NOT EXISTS check_results (
    id INTEGER PRIMARY KEY,
    resource_id TEXT NOT NULL,
    config_version_id INTEGER NOT NULL,
    checked_at TEXT NOT NULL,
    success INTEGER NOT NULL CHECK (success IN (0,1)),
    response_time_ms REAL CHECK (response_time_ms >= 0),
    http_status_code INTEGER CHECK (http_status_code BETWEEN 100 AND 599),
    error_message TEXT,
    resulting_status TEXT NOT NULL CHECK (resulting_status IN ('UP','DOWN','UNKNOWN')),
    FOREIGN KEY (config_version_id, resource_id)
      REFERENCES resource_config_versions(id, resource_id) ON DELETE RESTRICT,
    UNIQUE (resource_id, config_version_id, checked_at),
    UNIQUE (id, resource_id, resulting_status)
);
CREATE INDEX IF NOT EXISTS idx_checks_resource_time
    ON check_results(resource_id, checked_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS status_transitions (
    id INTEGER PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id) ON DELETE RESTRICT,
    check_result_id INTEGER NOT NULL UNIQUE,
    previous_status TEXT NOT NULL CHECK (previous_status IN ('UP','DOWN','UNKNOWN')),
    new_status TEXT NOT NULL CHECK (new_status IN ('UP','DOWN','UNKNOWN')),
    changed_at TEXT NOT NULL,
    UNIQUE (id, resource_id, new_status),
    FOREIGN KEY (check_result_id, resource_id, new_status)
      REFERENCES check_results(id, resource_id, resulting_status) ON DELETE RESTRICT,
    CHECK (previous_status <> new_status)
);
CREATE INDEX IF NOT EXISTS idx_status_transitions_resource
    ON status_transitions(resource_id, changed_at DESC);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id) ON DELETE RESTRICT,
    status_transition_id INTEGER UNIQUE,
    event_type TEXT NOT NULL CHECK (event_type IN ('STATUS_CHANGED','HEARTBEAT')),
    status TEXT NOT NULL CHECK (status IN ('UP','DOWN','UNKNOWN')),
    checked_at TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    delivery_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (delivery_status IN ('PENDING','SENT','FAILED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TEXT,
    delivered_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_until TEXT,
    FOREIGN KEY (status_transition_id, resource_id, status)
      REFERENCES status_transitions(id, resource_id, new_status) ON DELETE RESTRICT,
    CHECK (event_type <> 'STATUS_CHANGED' OR status_transition_id IS NOT NULL),
    CHECK (event_type <> 'HEARTBEAT' OR status_transition_id IS NULL),
    CHECK (delivery_status <> 'SENT' OR delivered_at IS NOT NULL),
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL)),
    CHECK (json_extract(payload_json, '$.event_id') = event_id),
    CHECK (json_extract(payload_json, '$.resource_id') = resource_id),
    CHECK (json_extract(payload_json, '$.status') = status),
    CHECK (json_extract(payload_json, '$.event_type') = event_type),
    CHECK (json_extract(payload_json, '$.checked_at') = checked_at)
);
CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON outbox_events(delivery_status, next_attempt_at, lease_until);
CREATE INDEX IF NOT EXISTS idx_outbox_resource
    ON outbox_events(resource_id, created_at DESC);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES outbox_events(event_id) ON DELETE RESTRICT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    attempted_at TEXT NOT NULL,
    http_status_code INTEGER CHECK (http_status_code BETWEEN 100 AND 599),
    outcome TEXT NOT NULL CHECK (outcome IN ('SENT','RETRY','FAILED')),
    error_message TEXT,
    UNIQUE(event_id, attempt_number)
);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_event
    ON delivery_attempts(event_id, attempt_number DESC);

-- Защита внешнего контракта: идентификатор события и JSON никогда не меняются.
CREATE TRIGGER IF NOT EXISTS immutable_outbox_payload
BEFORE UPDATE OF event_id, payload_json, event_type, resource_id, status, checked_at
ON outbox_events
BEGIN
  SELECT RAISE(ABORT, 'Immutable outbox event fields');
END;

-- Защита на уровне БД: даже прямой INSERT не привяжет результат старой
-- версии к текущему состоянию. Исторические записи не удаляются.
CREATE TRIGGER IF NOT EXISTS reject_stale_check_config
BEFORE INSERT ON check_results
BEGIN
  SELECT CASE WHEN NOT EXISTS (
    SELECT 1 FROM resource_states AS s
    JOIN resources AS r ON r.id=s.resource_id
    WHERE s.resource_id=NEW.resource_id
      AND s.config_version_id=NEW.config_version_id
      AND r.active=1
  ) THEN RAISE(ABORT, 'Stale or inactive check configuration') END;
END;
