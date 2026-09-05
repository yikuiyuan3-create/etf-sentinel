"""Add canonical record hashes for news and scheduled-event facts.

Revision ID: 0006_fact_record_integrity
Revises: 0005_alert_lineage
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0006_fact_record_integrity"
down_revision = "0005_alert_lineage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table_name in ("news_events", "scheduled_events"):
        columns = {column["name"] for column in inspector.get_columns(table_name)}
        if "record_hash" not in columns:
            with op.batch_alter_table(table_name) as batch:
                batch.add_column(sa.Column("record_hash", sa.String(length=64), nullable=True))
        # A migration cannot honestly attest legacy structured content.  The
        # deterministic fixture ingestion path may backfill only after comparing
        # every canonical field with the freshly generated source record.


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table_name in ("scheduled_events", "news_events"):
        columns = {column["name"] for column in inspector.get_columns(table_name)}
        if "record_hash" in columns:
            with op.batch_alter_table(table_name) as batch:
                batch.drop_column("record_hash")
