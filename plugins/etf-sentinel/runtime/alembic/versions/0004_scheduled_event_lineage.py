"""Add complete point-in-time lineage to scheduled events.

Revision ID: 0004_scheduled_event_lineage
Revises: 0003_signal_lineage
"""

from __future__ import annotations

import hashlib

import sqlalchemy as sa

from alembic import op

revision = "0004_scheduled_event_lineage"
down_revision = "0003_signal_lineage"
branch_labels = None
depends_on = None


_REQUIRED_COLUMNS: dict[str, sa.Column] = {
    "event_time": sa.Column("event_time", sa.DateTime(timezone=True), nullable=True),
    "published_at": sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    "first_seen_at": sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
    "available_at": sa.Column("available_at", sa.DateTime(timezone=True), nullable=True),
    "as_of": sa.Column("as_of", sa.DateTime(timezone=True), nullable=True),
    "ingested_at": sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=True),
    "revision": sa.Column("revision", sa.String(length=40), nullable=True),
    "timezone": sa.Column("timezone", sa.String(length=64), nullable=True),
    "currency": sa.Column("currency", sa.String(length=3), nullable=True),
    "latency_class": sa.Column("latency_class", sa.String(length=40), nullable=True),
    "quality_flags": sa.Column("quality_flags", sa.JSON(), nullable=True),
    "raw_payload_hash": sa.Column("raw_payload_hash", sa.String(length=64), nullable=True),
    "is_current": sa.Column("is_current", sa.Boolean(), nullable=True),
}

_NON_NULL_TYPES: dict[str, sa.TypeEngine] = {
    "event_time": sa.DateTime(timezone=True),
    "published_at": sa.DateTime(timezone=True),
    "first_seen_at": sa.DateTime(timezone=True),
    "available_at": sa.DateTime(timezone=True),
    "as_of": sa.DateTime(timezone=True),
    "ingested_at": sa.DateTime(timezone=True),
    "revision": sa.String(length=40),
    "timezone": sa.String(length=64),
    "latency_class": sa.String(length=40),
    "quality_flags": sa.JSON(),
    "raw_payload_hash": sa.String(length=64),
    "is_current": sa.Boolean(),
}

_INDEXES = {
    "ix_scheduled_events_event_time": "event_time",
    "ix_scheduled_events_first_seen_at": "first_seen_at",
    "ix_scheduled_events_available_at": "available_at",
    "ix_scheduled_events_is_current": "is_current",
}


def upgrade() -> None:
    bind = op.get_bind()
    existing_columns = {
        column["name"] for column in sa.inspect(bind).get_columns("scheduled_events")
    }
    missing_columns = [name for name in _REQUIRED_COLUMNS if name not in existing_columns]
    if missing_columns:
        with op.batch_alter_table("scheduled_events") as batch:
            for name in missing_columns:
                batch.add_column(_REQUIRED_COLUMNS[name])

        op.execute(
            sa.text(
                """
                UPDATE scheduled_events
                   SET event_time = scheduled_at,
                       published_at = first_announced_at,
                       first_seen_at = first_announced_at,
                       available_at = first_announced_at,
                       as_of = first_announced_at,
                       ingested_at = created_at,
                       revision = 'legacy-v1',
                       timezone = 'UTC',
                       latency_class = 'LEGACY_MIGRATED',
                       is_current = true
                """
            )
        )
        event_table = sa.table(
            "scheduled_events",
            sa.column("id", sa.String()),
            sa.column("revision", sa.String()),
            sa.column("quality_flags", sa.JSON()),
            sa.column("raw_payload_hash", sa.String()),
        )
        ids = bind.execute(sa.select(event_table.c.id)).scalars().all()
        for event_id in ids:
            digest = hashlib.sha256(f"legacy-scheduled-event:{event_id}".encode()).hexdigest()
            bind.execute(
                event_table.update()
                .where(event_table.c.id == event_id)
                .values(
                    revision=f"legacy-{digest[:16]}",
                    quality_flags=["MIGRATED_LINEAGE_REVIEW_REQUIRED"],
                    raw_payload_hash=digest,
                )
            )

        with op.batch_alter_table("scheduled_events") as batch:
            for name, column_type in _NON_NULL_TYPES.items():
                batch.alter_column(name, existing_type=column_type, nullable=False)

    existing_indexes = {index["name"] for index in sa.inspect(bind).get_indexes("scheduled_events")}
    for index_name, column_name in _INDEXES.items():
        if index_name not in existing_indexes:
            op.create_index(index_name, "scheduled_events", [column_name])

    unique_constraints = sa.inspect(bind).get_unique_constraints("scheduled_events")
    target_constraint = "uq_scheduled_events_provider_event_revision"
    if not any(constraint.get("name") == target_constraint for constraint in unique_constraints):
        naming_convention = {"uq": "uq_%(table_name)s_%(column_0_name)s"}
        with op.batch_alter_table("scheduled_events", naming_convention=naming_convention) as batch:
            for constraint in unique_constraints:
                columns = set(constraint.get("column_names") or [])
                if columns == {"event_code", "scheduled_at"}:
                    constraint_name = constraint.get("name") or "uq_scheduled_events_event_code"
                    batch.drop_constraint(constraint_name, type_="unique")
            batch.create_unique_constraint(
                target_constraint,
                ["provider_code", "event_code", "revision"],
            )


def downgrade() -> None:
    bind = op.get_bind()
    unique_constraints = sa.inspect(bind).get_unique_constraints("scheduled_events")
    if any(
        constraint.get("name") == "uq_scheduled_events_provider_event_revision"
        for constraint in unique_constraints
    ):
        with op.batch_alter_table("scheduled_events") as batch:
            batch.drop_constraint("uq_scheduled_events_provider_event_revision", type_="unique")
            batch.create_unique_constraint(
                "uq_scheduled_events_event_code_scheduled_at",
                ["event_code", "scheduled_at"],
            )
    existing_indexes = {index["name"] for index in sa.inspect(bind).get_indexes("scheduled_events")}
    for index_name in _INDEXES:
        if index_name in existing_indexes:
            op.drop_index(index_name, table_name="scheduled_events")
    existing_columns = {
        column["name"] for column in sa.inspect(bind).get_columns("scheduled_events")
    }
    with op.batch_alter_table("scheduled_events") as batch:
        for name in reversed(list(_REQUIRED_COLUMNS)):
            if name in existing_columns:
                batch.drop_column(name)
