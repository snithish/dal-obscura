"""Add optimistic-concurrency revision to authentication providers."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260913_0018"
down_revision = "20260913_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("auth_providers") as batch:
        batch.add_column(sa.Column("revision", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    with op.batch_alter_table("auth_providers") as batch:
        batch.drop_column("revision")
