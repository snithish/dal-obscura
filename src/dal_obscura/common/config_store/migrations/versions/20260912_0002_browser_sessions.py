"""Add opaque browser sessions.

Revision ID: 20260912_0002
Revises: 20260626_0001
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_0002"
down_revision = "20260626_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "browser_sessions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("principal", sa.Text(), nullable=False),
        sa.Column("groups_json", sa.JSON(), nullable=False),
        sa.Column("platform_admin", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_hash"),
    )
    op.create_index(
        "ix_browser_sessions_expires_at",
        "browser_sessions",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_browser_sessions_principal",
        "browser_sessions",
        ["principal"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_browser_sessions_principal", table_name="browser_sessions")
    op.drop_index("ix_browser_sessions_expires_at", table_name="browser_sessions")
    op.drop_table("browser_sessions")
