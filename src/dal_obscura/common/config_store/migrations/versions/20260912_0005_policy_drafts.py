"""Add revisioned personal policy drafts.

Revision ID: 20260912_0005
Revises: 20260912_0004
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_0005"
down_revision = "20260912_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "asset_policy_drafts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("author_principal", sa.Text(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("base_policy_version", sa.BigInteger(), nullable=False),
        sa.Column("rules_json", sa.JSON(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("discarded_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["asset_id"], ["assets.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "author_principal"),
    )
    op.create_index(
        "ix_asset_policy_drafts_author",
        "asset_policy_drafts",
        ["author_principal"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_asset_policy_drafts_author", table_name="asset_policy_drafts")
    op.drop_table("asset_policy_drafts")
