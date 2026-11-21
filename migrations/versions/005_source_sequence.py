
"""Защита от устаревших событий Oracle Sync.

Revision ID: 005_source_sequence
Revises: 004_inbox_worker
"""

from alembic import op

revision = "005_source_sequence"
down_revision = "004_inbox_worker"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE resource_source_sequences (
            source_system TEXT NOT NULL,
            source_entity TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            last_sequence INTEGER NOT NULL
                CHECK (last_sequence >= 0),
            last_event_id TEXT NOT NULL,
            last_operation TEXT NOT NULL
                CHECK (
                    last_operation IN (
                        'INSERT', 'UPDATE', 'DELETE'
                    )
                ),
            updated_at TEXT NOT NULL,
            PRIMARY KEY (
                source_system,
                source_entity,
                source_record_id
            )
        )
    """)


def downgrade() -> None:
    op.execute(
        "DROP TABLE resource_source_sequences"
    )
