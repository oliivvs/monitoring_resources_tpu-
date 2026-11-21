
"""Временные метки повторов и токен захвата для Oracle Sync Inbox.

Revision ID: 004_inbox_worker
Revises: 003_tpu_inbox
"""

from alembic import op

revision = "004_inbox_worker"
down_revision = "003_tpu_inbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE inbox_events ADD COLUMN next_attempt_at TEXT"
    )
    op.execute(
        "ALTER TABLE inbox_events ADD COLUMN lease_expires_at TEXT"
    )
    op.execute(
        "ALTER TABLE inbox_events ADD COLUMN claim_token TEXT"
    )

    op.execute("""
        CREATE INDEX idx_inbox_worker_ready
        ON inbox_events(
            processing_status,
            next_attempt_at,
            lease_expires_at,
            received_at
        )
    """)


def downgrade() -> None:
    op.execute("DROP INDEX idx_inbox_worker_ready")
    op.execute(
        "ALTER TABLE inbox_events DROP COLUMN claim_token"
    )
    op.execute(
        "ALTER TABLE inbox_events DROP COLUMN lease_expires_at"
    )
    op.execute(
        "ALTER TABLE inbox_events DROP COLUMN next_attempt_at"
    )
