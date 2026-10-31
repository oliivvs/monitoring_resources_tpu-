"""Reject late checks from retired configurations at database boundary.

Revision ID: 002_check_token
Revises: 001_initial
"""
from alembic import op

revision = "002_check_token"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade():
    # v2 initial migration accidentally created a trigger named
    # immutable_outbox_payloadBEFORE (without BEFORE keyword).
    # Recreate it with the intended BEFORE UPDATE timing/name.
    op.execute("DROP TRIGGER IF EXISTS immutable_outbox_payloadBEFORE")
    op.execute("DROP TRIGGER IF EXISTS immutable_outbox_payload")
    op.execute("""
        CREATE TRIGGER immutable_outbox_payload
        BEFORE UPDATE OF event_id, payload_json, event_type, resource_id, status, checked_at
        ON outbox_events
        BEGIN
            SELECT RAISE(ABORT, 'Immutable outbox event fields');
        END
    """)
    op.execute("""
        CREATE TRIGGER reject_stale_check_config
        BEFORE INSERT ON check_results
        BEGIN
          SELECT CASE WHEN NOT EXISTS (
            SELECT 1 FROM resource_states AS s
            JOIN resources AS r ON r.id=s.resource_id
            WHERE s.resource_id=NEW.resource_id
              AND s.config_version_id=NEW.config_version_id
              AND r.active=1
          ) THEN RAISE(ABORT, 'Stale or inactive check configuration') END;
        END
    """)


def downgrade():
    op.execute("DROP TRIGGER IF EXISTS reject_stale_check_config")
    # Preserve intended outbox immutability even if this migration is undone.
