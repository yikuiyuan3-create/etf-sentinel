"""Add current-signal lineage fields.

Revision ID: 0003_signal_lineage
Revises: 0002_scheduled_events
"""

import sqlalchemy as sa

from alembic import op

revision = "0003_signal_lineage"
down_revision = "0002_scheduled_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_columns = {column["name"] for column in inspector.get_columns("signals")}
    with op.batch_alter_table("signals") as batch:
        if "recorded_at" not in existing_columns:
            batch.add_column(sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=True))
        if "is_current" not in existing_columns:
            batch.add_column(sa.Column("is_current", sa.Boolean(), nullable=True))
    if "recorded_at" not in existing_columns:
        op.execute(sa.text("UPDATE signals SET recorded_at = CURRENT_TIMESTAMP"))
    if "is_current" not in existing_columns:
        op.execute(sa.text("UPDATE signals SET is_current = true"))
    with op.batch_alter_table("signals") as batch:
        if "recorded_at" not in existing_columns:
            batch.alter_column("recorded_at", existing_type=sa.DateTime(), nullable=False)
        if "is_current" not in existing_columns:
            batch.alter_column("is_current", existing_type=sa.Boolean(), nullable=False)
    existing_indexes = {index["name"] for index in sa.inspect(bind).get_indexes("signals")}
    if "ix_signals_recorded_at" not in existing_indexes:
        op.create_index("ix_signals_recorded_at", "signals", ["recorded_at"])
    if "ix_signals_is_current" not in existing_indexes:
        op.create_index("ix_signals_is_current", "signals", ["is_current"])


def downgrade() -> None:
    existing_columns = {
        column["name"] for column in sa.inspect(op.get_bind()).get_columns("signals")
    }
    with op.batch_alter_table("signals") as batch:
        if "is_current" in existing_columns:
            batch.drop_index("ix_signals_is_current")
            batch.drop_column("is_current")
        if "recorded_at" in existing_columns:
            batch.drop_index("ix_signals_recorded_at")
            batch.drop_column("recorded_at")
