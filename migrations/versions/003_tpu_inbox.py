
"""Oracle Sync: persistent Inbox and source/resource mapping tables.

Revision ID: 003_tpu_inbox
Revises: 002_check_token

No existing resources, check history, Outbox or delivery records are modified.
"""
from alembic import op

revision = "003_tpu_inbox"
down_revision = "002_check_token"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE inbox_events (
            event_id TEXT PRIMARY KEY,
            source_system TEXT NOT NULL,
            source_entity TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            operation TEXT NOT NULL CHECK (operation IN ('INSERT','UPDATE','DELETE')),
            source_sequence INTEGER CHECK (source_sequence IS NULL OR source_sequence >= 0),
            occurred_at TEXT NOT NULL,
            payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
            payload_hash TEXT NOT NULL CHECK (length(payload_hash) = 64),
            received_at TEXT NOT NULL,
            processed_at TEXT,
            processing_status TEXT NOT NULL DEFAULT 'PENDING'
                CHECK (processing_status IN ('PENDING','PROCESSING','PROCESSED','FAILED')),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            last_error TEXT
        )
    """)
    op.execute("""
        CREATE INDEX idx_inbox_processing_due
        ON inbox_events(processing_status, received_at)
    """)
    op.execute("""
        CREATE TABLE resource_source_mappings (
            id INTEGER PRIMARY KEY,
            source_system TEXT NOT NULL,
            source_entity TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            resource_id TEXT NOT NULL REFERENCES resources(id) ON DELETE RESTRICT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_system, source_entity, source_record_id, resource_id)
        )
    """)
    op.execute("""
        CREATE INDEX idx_resource_source_lookup
        ON resource_source_mappings(source_system, source_entity, source_record_id)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE resource_source_mappings")
    op.execute("DROP TABLE inbox_events")
