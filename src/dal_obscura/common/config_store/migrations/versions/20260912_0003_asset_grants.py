"""Add explicit asset capability grants.

Revision ID: 20260912_0003
Revises: 20260912_0002
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_0003"
down_revision = "20260912_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "asset_grants",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("principal", sa.Text(), nullable=False),
        sa.Column("capability", sa.String(length=24), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["asset_id"], ["assets.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "principal", "capability"),
    )
    op.create_index(
        "ix_asset_grants_asset_principal",
        "asset_grants",
        ["asset_id", "principal"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_asset_grants_asset_principal", table_name="asset_grants")
    op.drop_table("asset_grants")
