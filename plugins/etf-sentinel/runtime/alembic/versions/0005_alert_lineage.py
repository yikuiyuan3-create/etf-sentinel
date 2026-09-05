"""Add fail-closed source lineage and content integrity to alerts.

Revision ID: 0005_alert_lineage
Revises: 0004_scheduled_event_lineage
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0005_alert_lineage"
down_revision = "0004_scheduled_event_lineage"
branch_labels = None
depends_on = None


_COLUMNS: dict[str, sa.Column] = {
    "provider_code": sa.Column("provider_code", sa.String(length=80), nullable=True),
    "source_object_type": sa.Column("source_object_type", sa.String(length=40), nullable=True),
    "source_object_id": sa.Column("source_object_id", sa.String(length=80), nullable=True),
    "content_hash": sa.Column("content_hash", sa.String(length=64), nullable=True),
}

_INDEXES = {
    "ix_alerts_provider_code": "provider_code",
    "ix_alerts_source_object_id": "source_object_id",
}


def upgrade() -> None:
    bind = op.get_bind()
    existing_columns = {column["name"] for column in sa.inspect(bind).get_columns("alerts")}
    missing = [name for name in _COLUMNS if name not in existing_columns]
    if missing:
        with op.batch_alter_table("alerts") as batch:
            for name in missing:
                batch.add_column(_COLUMNS[name])

    # Historical alert text cannot be cryptographically re-attested during a
    # schema migration.  Link what can be linked, mark everything else as
    # unverified, and leave content_hash NULL so display remains fail-closed.
    op.execute(
        sa.text(
            """
            UPDATE alerts
               SET source_object_type = CASE
                       WHEN signal_id IS NOT NULL THEN 'LegacyUnverifiedSignal'
                       WHEN alert_type = 'SYSTEM_HEALTH' THEN 'LegacyUnverifiedSystem'
                       ELSE 'LegacyUnverified'
                   END,
                   source_object_id = CASE
                       WHEN signal_id IS NOT NULL THEN signal_id
                       ELSE NULL
                   END
             WHERE source_object_type IS NULL
                OR source_object_type = ''
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE alerts
               SET provider_code = (
                   SELECT data_snapshots.provider_code
                     FROM signals
                     JOIN data_snapshots
                       ON data_snapshots.id = signals.data_snapshot_id
                    WHERE signals.id = alerts.signal_id
               )
             WHERE signal_id IS NOT NULL
               AND provider_code IS NULL
            """
        )
    )
    columns = {column["name"]: column for column in sa.inspect(bind).get_columns("alerts")}
    if columns["source_object_type"]["nullable"]:
        with op.batch_alter_table("alerts") as batch:
            batch.alter_column(
                "source_object_type",
                existing_type=sa.String(length=40),
                nullable=False,
            )

    existing_indexes = {index["name"] for index in sa.inspect(bind).get_indexes("alerts")}
    for index_name, column_name in _INDEXES.items():
        if index_name not in existing_indexes:
            op.create_index(index_name, "alerts", [column_name])


def downgrade() -> None:
    bind = op.get_bind()
    existing_indexes = {index["name"] for index in sa.inspect(bind).get_indexes("alerts")}
    for index_name in _INDEXES:
        if index_name in existing_indexes:
            op.drop_index(index_name, table_name="alerts")
    existing_columns = {column["name"] for column in sa.inspect(bind).get_columns("alerts")}
    with op.batch_alter_table("alerts") as batch:
        for name in reversed(list(_COLUMNS)):
            if name in existing_columns:
                batch.drop_column(name)
