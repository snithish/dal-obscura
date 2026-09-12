"""Add stable schema field identities and typed paths.

Revision ID: 20260912_0011
Revises: 20260912_0010
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_0011"
down_revision = "20260912_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("asset_schema_fields") as batch:
        batch.add_column(sa.Column("field_id", sa.String(length=128), nullable=True))
        batch.add_column(sa.Column("path_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("asset_schema_fields") as batch:
        batch.drop_column("path_json")
        batch.drop_column("field_id")
