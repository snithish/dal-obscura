"""Add idempotent publication operations.

Revision ID: 20260912_0008
Revises: 20260912_0007
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_0008"
down_revision = "20260912_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "publication_operations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("actor_principal", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("result_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["asset_id"], ["assets.id"]),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "actor_principal", "idempotency_key"),
    )
    op.create_index(
        "ix_publication_operations_created",
        "publication_operations",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_publication_operations_created", table_name="publication_operations")
    op.drop_table("publication_operations")
