"""Backfill deterministic identities for legacy admitted schema fields.

Revision ID: 20260912_0013
Revises: 20260912_0012
Create Date: 2026-09-12
"""

from __future__ import annotations

import hashlib
import json

import sqlalchemy as sa
from alembic import op

revision = "20260912_0013"
down_revision = "20260912_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    table = sa.table(
        "asset_schema_fields",
        sa.column("id"),
        sa.column("name", sa.Text()),
        sa.column("field_id", sa.String(length=128)),
        sa.column("path_json", sa.JSON()),
    )
    rows = bind.execute(
        sa.select(table.c.id, table.c.name, table.c.field_id, table.c.path_json)
    ).mappings()
    for row in rows:
        path = row["path_json"]
        if not isinstance(path, list) or not path:
            path = [str(row["name"])]
        field_id = row["field_id"]
        if not isinstance(field_id, str) or not field_id.strip():
            encoded = json.dumps(path, separators=(",", ":"), ensure_ascii=False)
            field_id = "legacy:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:32]
        bind.execute(
            table.update()
            .where(table.c.id == row["id"])
            .values(field_id=field_id, path_json=path)
        )


def downgrade() -> None:
    # The identity columns belong to 0011; retain their values when rolling back
    # this data-only migration so a downgrade never destroys policy metadata.
    pass
