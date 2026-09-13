"""Add optimistic-concurrency revision to draft catalogs.

Revision ID: 20260913_0015
Revises: 20260913_0014
Create Date: 2026-09-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260913_0015"
down_revision = "20260913_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("catalogs") as batch:
        batch.add_column(sa.Column("revision", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    with op.batch_alter_table("catalogs") as batch:
        batch.drop_column("revision")
