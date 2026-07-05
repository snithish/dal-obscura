"""initial config store schema

Revision ID: 20260626_0001
Revises:
Create Date: 2026-06-26
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260626_0001"
down_revision = None
branch_labels = None
depends_on = None


def _create_tenants() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(length=120), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )


def _create_cells() -> None:
    op.create_table(
        "cells",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("region", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )


def _create_cell_tenants() -> None:
    op.create_table(
        "cell_tenants",
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("shard_key", sa.String(length=120), nullable=False),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("cell_id", "tenant_id"),
    )


def _create_cell_runtime_settings() -> None:
    op.create_table(
        "cell_runtime_settings",
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("ticket_ttl_seconds", sa.Integer(), nullable=False),
        sa.Column("max_tickets", sa.Integer(), nullable=False),
        sa.Column("max_ticket_exchanges", sa.Integer(), nullable=False),
        sa.Column("path_rules_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.PrimaryKeyConstraint("cell_id"),
    )


def _create_catalogs() -> None:
    op.create_table(
        "catalogs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("module", sa.Text(), nullable=False),
        sa.Column("options_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cell_id", "tenant_id", "name"),
    )


def _create_assets() -> None:
    op.create_table(
        "assets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_id", sa.Uuid(), nullable=False),
        sa.Column("target", sa.Text(), nullable=False),
        sa.Column("backend", sa.String(length=48), nullable=False),
        sa.Column("table_identifier", sa.Text(), nullable=True),
        sa.Column("options_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["catalog_id"], ["catalogs.id"]),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cell_id", "tenant_id", "catalog_id", "target"),
    )


def _create_asset_owners() -> None:
    op.create_table(
        "asset_owners",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("principal", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["asset_id"], ["assets.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "principal"),
    )
    op.create_index(
        op.f("ix_asset_owners_asset_id"),
        "asset_owners",
        ["asset_id"],
        unique=False,
    )


def _create_asset_schema_fields() -> None:
    op.create_table(
        "asset_schema_fields",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("type", sa.String(length=120), nullable=False),
        sa.Column("nullable", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(["asset_id"], ["assets.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "name"),
    )
    op.create_index(
        op.f("ix_asset_schema_fields_asset_id"),
        "asset_schema_fields",
        ["asset_id"],
        unique=False,
    )


def _create_policy_rules() -> None:
    op.create_table(
        "policy_rules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("effect", sa.String(length=16), nullable=False),
        sa.Column("principals_json", sa.JSON(), nullable=False),
        sa.Column("when_json", sa.JSON(), nullable=False),
        sa.Column("columns_json", sa.JSON(), nullable=False),
        sa.Column("masks_json", sa.JSON(), nullable=False),
        sa.Column("row_filter_sql", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["asset_id"], ["assets.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "ordinal"),
    )


def _create_auth_providers() -> None:
    op.create_table(
        "auth_providers",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("module", sa.Text(), nullable=False),
        sa.Column("args_json", sa.JSON(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cell_id", "ordinal"),
    )


def _create_config_publications() -> None:
    op.create_table(
        "config_publications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("manifest_hash", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_config_publications_cell_id"),
        "config_publications",
        ["cell_id"],
        unique=False,
    )


def _create_active_publications() -> None:
    op.create_table(
        "active_publications",
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("publication_id", sa.Uuid(), nullable=False),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.ForeignKeyConstraint(["publication_id"], ["config_publications.id"]),
        sa.PrimaryKeyConstraint("cell_id"),
    )


def _create_published_cell_runtime() -> None:
    op.create_table(
        "published_cell_runtime",
        sa.Column("publication_id", sa.Uuid(), nullable=False),
        sa.Column("auth_chain_json", sa.JSON(), nullable=False),
        sa.Column("ticket_json", sa.JSON(), nullable=False),
        sa.Column("path_rules_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["publication_id"], ["config_publications.id"]),
        sa.PrimaryKeyConstraint("publication_id"),
    )


def _create_published_catalogs() -> None:
    op.create_table(
        "published_catalogs",
        sa.Column("publication_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("catalog", sa.String(length=160), nullable=False),
        sa.Column("config_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["publication_id"], ["config_publications.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("publication_id", "tenant_id", "catalog"),
    )


def _create_published_assets() -> None:
    op.create_table(
        "published_assets",
        sa.Column("publication_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("catalog", sa.String(length=160), nullable=False),
        sa.Column("target", sa.Text(), nullable=False),
        sa.Column("backend", sa.String(length=48), nullable=False),
        sa.Column("compiled_config_json", sa.JSON(), nullable=False),
        sa.Column("policy_version", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(["publication_id"], ["config_publications.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("publication_id", "tenant_id", "catalog", "target"),
    )


def _create_active_published_assets() -> None:
    op.create_table(
        "active_published_assets",
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("catalog", sa.String(length=160), nullable=False),
        sa.Column("target", sa.Text(), nullable=False),
        sa.Column("publication_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.ForeignKeyConstraint(["publication_id"], ["config_publications.id"]),
        sa.PrimaryKeyConstraint("cell_id", "tenant_id", "catalog", "target"),
    )
    op.create_index(
        "ix_active_published_assets_publication",
        "active_published_assets",
        ["publication_id"],
        unique=False,
    )


def _create_data_plane_tickets() -> None:
    op.create_table(
        "data_plane_tickets",
        sa.Column("ticket_id", sa.Uuid(), nullable=False),
        sa.Column("cell_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("catalog", sa.Text(), nullable=True),
        sa.Column("target", sa.Text(), nullable=False),
        sa.Column("principal_id", sa.Text(), nullable=False),
        sa.Column("policy_version", sa.BigInteger(), nullable=False),
        sa.Column("expires_at", sa.Integer(), nullable=False),
        sa.Column("max_exchanges", sa.Integer(), nullable=False),
        sa.Column("exchange_count", sa.Integer(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_exchanged_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["cell_id"], ["cells.id"]),
        sa.PrimaryKeyConstraint("ticket_id"),
    )
    op.create_index(
        "ix_data_plane_tickets_cell_expires",
        "data_plane_tickets",
        ["cell_id", "expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_data_plane_tickets_cell_ticket",
        "data_plane_tickets",
        ["cell_id", "ticket_id"],
        unique=False,
    )


TABLE_CREATORS = (
    ("tenants", _create_tenants),
    ("cells", _create_cells),
    ("cell_tenants", _create_cell_tenants),
    ("cell_runtime_settings", _create_cell_runtime_settings),
    ("catalogs", _create_catalogs),
    ("assets", _create_assets),
    ("asset_owners", _create_asset_owners),
    ("asset_schema_fields", _create_asset_schema_fields),
    ("policy_rules", _create_policy_rules),
    ("auth_providers", _create_auth_providers),
    ("config_publications", _create_config_publications),
    ("active_publications", _create_active_publications),
    ("published_cell_runtime", _create_published_cell_runtime),
    ("published_catalogs", _create_published_catalogs),
    ("published_assets", _create_published_assets),
    ("active_published_assets", _create_active_published_assets),
    ("data_plane_tickets", _create_data_plane_tickets),
)


def upgrade() -> None:
    """Creates the initial config-store schema.

    Example:
        ```python
        upgrade()
        ```
    """

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

    for table_name, create_table in TABLE_CREATORS:
        if table_name not in existing_tables:
            create_table()


def downgrade() -> None:
    """Drops the initial config-store schema.

    Example:
        ```python
        downgrade()
        ```
    """

    op.drop_index(
        "ix_active_published_assets_publication",
        table_name="active_published_assets",
    )
    op.drop_table("active_published_assets")
    op.drop_index("ix_data_plane_tickets_cell_ticket", table_name="data_plane_tickets")
    op.drop_index("ix_data_plane_tickets_cell_expires", table_name="data_plane_tickets")
    op.drop_table("data_plane_tickets")
    op.drop_table("published_assets")
    op.drop_table("published_catalogs")
    op.drop_table("published_cell_runtime")
    op.drop_table("active_publications")
    op.drop_index(
        op.f("ix_config_publications_cell_id"),
        table_name="config_publications",
    )
    op.drop_table("config_publications")
    op.drop_table("auth_providers")
    op.drop_table("policy_rules")
    op.drop_index(op.f("ix_asset_schema_fields_asset_id"), table_name="asset_schema_fields")
    op.drop_table("asset_schema_fields")
    op.drop_index(op.f("ix_asset_owners_asset_id"), table_name="asset_owners")
    op.drop_table("asset_owners")
    op.drop_table("assets")
    op.drop_table("catalogs")
    op.drop_table("cell_runtime_settings")
    op.drop_table("cell_tenants")
    op.drop_table("cells")
    op.drop_table("tenants")
