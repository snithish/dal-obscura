"""Add append-only control-plane audit events.

Revision ID: 20260912_0006
Revises: 20260912_0005
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_0006"
down_revision = "20260912_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=True),
        sa.Column("actor_principal", sa.Text(), nullable=False),
        sa.Column("action", sa.String(length=96), nullable=False),
        sa.Column("resource_type", sa.String(length=48), nullable=False),
        sa.Column("resource_id", sa.Text(), nullable=False),
        sa.Column("outcome", sa.String(length=24), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("correlation_id", sa.String(length=96), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_audit_events_cell_created",
        "audit_events",
        ["cell_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_audit_events_tenant_created",
        "audit_events",
        ["tenant_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_audit_events_resource",
        "audit_events",
        ["resource_type", "resource_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_audit_events_resource", table_name="audit_events")
    op.drop_index("ix_audit_events_tenant_created", table_name="audit_events")
    op.drop_index("ix_audit_events_cell_created", table_name="audit_events")
    op.drop_table("audit_events")
