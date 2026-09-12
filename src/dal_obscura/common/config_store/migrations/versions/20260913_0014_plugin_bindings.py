"""Persist immutable plugin identities alongside published generations.

Revision ID: 20260913_0014
Revises: 20260912_0013
Create Date: 2026-09-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260913_0014"
down_revision = "20260912_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("published_catalogs") as batch:
        batch.add_column(sa.Column("plugin_id", sa.String(length=128), nullable=True))
        batch.add_column(sa.Column("plugin_revision", sa.BigInteger(), nullable=True))
    with op.batch_alter_table("published_assets") as batch:
        batch.add_column(sa.Column("catalog_plugin_id", sa.String(length=128), nullable=True))
        batch.add_column(sa.Column("format_plugin_id", sa.String(length=128), nullable=True))
        batch.add_column(sa.Column("plugin_revision", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("published_assets") as batch:
        batch.drop_column("plugin_revision")
        batch.drop_column("format_plugin_id")
        batch.drop_column("catalog_plugin_id")
    with op.batch_alter_table("published_catalogs") as batch:
        batch.drop_column("plugin_revision")
        batch.drop_column("plugin_id")

