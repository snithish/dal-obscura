"""Add optimistic-concurrency revision to runtime settings."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260913_0017"
down_revision = "20260913_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("cell_runtime_settings") as batch:
        batch.add_column(sa.Column("revision", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    with op.batch_alter_table("cell_runtime_settings") as batch:
        batch.drop_column("revision")
