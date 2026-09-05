"""Add configurable scheduled event calendar.

Revision ID: 0002_scheduled_events
Revises: 0001_initial
"""

from alembic import op
from etf_sentinel.models import ScheduledEvent

revision = "0002_scheduled_events"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ScheduledEvent.__table__.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    ScheduledEvent.__table__.drop(bind=op.get_bind(), checkfirst=True)
