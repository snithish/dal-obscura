"""Scope browser sessions by the validated identity-provider issuer.

Revision ID: 20260912_0010
Revises: 20260912_0009
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_0010"
down_revision = "20260912_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("browser_sessions", sa.Column("issuer", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("browser_sessions", "issuer")
