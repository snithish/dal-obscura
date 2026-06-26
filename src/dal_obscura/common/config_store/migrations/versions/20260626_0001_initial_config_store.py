"""initial config store schema

Revision ID: 20260626_0001
Revises:
Create Date: 2026-06-26
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

from dal_obscura.common.config_store.orm import Base

revision = "20260626_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = set(inspector.get_table_names())
    if "cell_runtime_settings" in existing_tables:
        runtime_columns = {
            column["name"] for column in inspector.get_columns("cell_runtime_settings")
        }
        if "max_ticket_exchanges" not in runtime_columns:
            op.add_column(
                "cell_runtime_settings",
                sa.Column(
                    "max_ticket_exchanges",
                    sa.Integer(),
                    nullable=False,
                    server_default="1",
                ),
            )
        Base.metadata.create_all(bind=bind)
        return

    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    Base.metadata.drop_all(bind=op.get_bind())
