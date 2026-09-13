"""Index audit events for tenant-scoped keyset pagination.

Revision ID: 20260913_0016
Revises: 20260913_0015
Create Date: 2026-09-13
"""

from __future__ import annotations

from alembic import op

revision = "20260913_0016"
down_revision = "20260913_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_audit_events_workspace_keyset",
        "audit_events",
        ["cell_id", "tenant_id", "created_at", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_audit_events_workspace_keyset", table_name="audit_events")
