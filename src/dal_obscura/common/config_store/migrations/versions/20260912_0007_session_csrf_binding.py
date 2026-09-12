"""Bind browser CSRF secrets to server sessions.

Revision ID: 20260912_0007
Revises: 20260912_0006
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_0007"
down_revision = "20260912_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "browser_sessions",
        sa.Column("csrf_hash", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("browser_sessions", "csrf_hash")
