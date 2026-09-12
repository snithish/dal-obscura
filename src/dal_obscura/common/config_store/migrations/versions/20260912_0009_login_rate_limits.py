"""Add durable login abuse windows.

Revision ID: 20260912_0009
Revises: 20260912_0008
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_0009"
down_revision = "20260912_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "login_rate_limits",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("client_key_hash", sa.String(length=64), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("blocked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("client_key_hash"),
    )
    op.create_index(
        "ix_login_rate_limits_blocked_until",
        "login_rate_limits",
        ["blocked_until"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_login_rate_limits_blocked_until", table_name="login_rate_limits")
    op.drop_table("login_rate_limits")
