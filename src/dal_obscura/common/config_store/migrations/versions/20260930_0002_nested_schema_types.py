"""Preserve complete nested Arrow types in schema admission.

Revision ID: 20260930_0002
Revises: 20260930_0001
"""

import sqlalchemy as sa
from alembic import op

revision = "20260930_0002"
down_revision = "20260930_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("asset_schema_fields") as batch:
        batch.alter_column("type", existing_type=sa.String(120), type_=sa.Text(), nullable=False)


def downgrade() -> None:
    # A downgrade fails on PostgreSQL if a type exceeds the old limit; never
    # silently truncate admitted types and weaken schema-drift checks.
    with op.batch_alter_table("asset_schema_fields") as batch:
        batch.alter_column("type", existing_type=sa.Text(), type_=sa.String(120), nullable=False)
