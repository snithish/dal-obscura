"""Initial schema for the generation-free live configuration model.

Revision ID: 20261003_0001
Revises: None

This is the only supported baseline. Development databases must be recreated.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20261003_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("actor_principal", sa.Text(), nullable=False),
        sa.Column("action", sa.String(length=96), nullable=False),
        sa.Column("resource_type", sa.String(length=48), nullable=False),
        sa.Column("resource_id", sa.Text(), nullable=False),
        sa.Column("outcome", sa.String(length=24), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("correlation_id", sa.String(length=96), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_audit_events_resource", "audit_events", ["resource_type", "resource_id"], unique=False
    )
    op.create_index(
        "ix_audit_events_workspace_keyset", "audit_events", ["created_at", "id"], unique=False
    )
    op.create_table(
        "auth_providers",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("module", sa.Text(), nullable=False),
        sa.Column("args_json", sa.JSON(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ordinal"),
    )
    op.create_table(
        "browser_sessions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("csrf_hash", sa.String(length=64), nullable=True),
        sa.Column("principal", sa.Text(), nullable=False),
        sa.Column("issuer", sa.Text(), nullable=True),
        sa.Column("groups_json", sa.JSON(), nullable=False),
        sa.Column("platform_admin", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_hash"),
    )
    op.create_index(
        "ix_browser_sessions_expires_at", "browser_sessions", ["expires_at"], unique=False
    )
    op.create_index(
        "ix_browser_sessions_principal", "browser_sessions", ["principal"], unique=False
    )
    op.create_table(
        "catalogs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("plugin_id", sa.Text(), nullable=False),
        sa.Column("options_json", sa.JSON(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index("ix_catalogs_workspace_name", "catalogs", ["name"], unique=False)
    op.create_table(
        "data_plane_tickets",
        sa.Column("ticket_id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("catalog", sa.Text(), nullable=True),
        sa.Column("target", sa.Text(), nullable=False),
        sa.Column("principal_id", sa.Text(), nullable=False),
        sa.Column("policy_version", sa.BigInteger(), nullable=False),
        sa.Column("expires_at", sa.Integer(), nullable=False),
        sa.Column("max_exchanges", sa.Integer(), nullable=False),
        sa.Column("exchange_count", sa.Integer(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_exchanged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("ticket_id"),
    )
    op.create_index(
        "ix_data_plane_tickets_asset_expires",
        "data_plane_tickets",
        ["asset_id", "expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_data_plane_tickets_expires", "data_plane_tickets", ["expires_at"], unique=False
    )
    op.create_index(
        "ix_data_plane_tickets_ticket", "data_plane_tickets", ["ticket_id"], unique=False
    )
    op.create_table(
        "login_rate_limits",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("client_key_hash", sa.String(length=64), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("blocked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("client_key_hash"),
    )
    op.create_index(
        "ix_login_rate_limits_blocked_until", "login_rate_limits", ["blocked_until"], unique=False
    )
    op.create_table(
        "login_transactions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("state_hash", sa.String(length=64), nullable=False),
        sa.Column("nonce_hash", sa.String(length=64), nullable=False),
        sa.Column("code_verifier", sa.String(length=128), nullable=False),
        sa.Column("redirect_uri", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("state_hash"),
    )
    op.create_index(
        "ix_login_transactions_expires_at", "login_transactions", ["expires_at"], unique=False
    )
    op.create_table(
        "runtime_settings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("ticket_ttl_seconds", sa.Integer(), nullable=False),
        sa.Column("max_tickets", sa.Integer(), nullable=False),
        sa.Column("max_ticket_exchanges", sa.Integer(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("path_rules_json", sa.JSON(), nullable=False),
        sa.CheckConstraint("id = 1", name="ck_runtime_settings_singleton"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "workspace",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("auth_provider_revision", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("id = 1", name="ck_workspace_singleton"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "assets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("catalog_id", sa.Uuid(), nullable=False),
        sa.Column("target", sa.Text(), nullable=False),
        sa.Column("backend", sa.String(length=48), nullable=False),
        sa.Column("table_identifier", sa.Text(), nullable=True),
        sa.Column("options_json", sa.JSON(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("policy_revision", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["catalog_id"],
            ["catalogs.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("catalog_id", "target"),
    )
    op.create_index(op.f("ix_assets_catalog_id"), "assets", ["catalog_id"], unique=False)
    op.create_index("ix_assets_workspace_catalog", "assets", ["catalog_id"], unique=False)
    op.create_index("ix_assets_workspace_target", "assets", ["target", "id"], unique=False)
    op.create_table(
        "asset_grants",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("principal", sa.Text(), nullable=False),
        sa.Column("capability", sa.String(length=24), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["asset_id"],
            ["assets.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "principal", "capability"),
    )
    op.create_index(
        "ix_asset_grants_asset_principal", "asset_grants", ["asset_id", "principal"], unique=False
    )
    op.create_table(
        "asset_owners",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("principal", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["asset_id"],
            ["assets.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "principal"),
    )
    op.create_index(op.f("ix_asset_owners_asset_id"), "asset_owners", ["asset_id"], unique=False)
    op.create_table(
        "asset_schema_fields",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("field_id", sa.String(length=128), nullable=False),
        sa.Column("path_json", sa.JSON(), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("nullable", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["asset_id"],
            ["assets.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "name"),
    )
    op.create_index(
        op.f("ix_asset_schema_fields_asset_id"), "asset_schema_fields", ["asset_id"], unique=False
    )
    op.create_table(
        "policy_rules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("asset_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("effect", sa.String(length=16), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("principals_json", sa.JSON(), nullable=False),
        sa.Column("when_json", sa.JSON(), nullable=False),
        sa.Column("columns_json", sa.JSON(), nullable=False),
        sa.Column("masks_json", sa.JSON(), nullable=False),
        sa.Column("row_filter_sql", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["asset_id"],
            ["assets.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "ordinal"),
    )
    op.create_index(op.f("ix_policy_rules_asset_id"), "policy_rules", ["asset_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_policy_rules_asset_id"), table_name="policy_rules")
    op.drop_table("policy_rules")
    op.drop_index(op.f("ix_asset_schema_fields_asset_id"), table_name="asset_schema_fields")
    op.drop_table("asset_schema_fields")
    op.drop_index(op.f("ix_asset_owners_asset_id"), table_name="asset_owners")
    op.drop_table("asset_owners")
    op.drop_index("ix_asset_grants_asset_principal", table_name="asset_grants")
    op.drop_table("asset_grants")
    op.drop_index("ix_assets_workspace_target", table_name="assets")
    op.drop_index("ix_assets_workspace_catalog", table_name="assets")
    op.drop_index(op.f("ix_assets_catalog_id"), table_name="assets")
    op.drop_table("assets")
    op.drop_table("workspace")
    op.drop_table("runtime_settings")
    op.drop_index("ix_login_transactions_expires_at", table_name="login_transactions")
    op.drop_table("login_transactions")
    op.drop_index("ix_login_rate_limits_blocked_until", table_name="login_rate_limits")
    op.drop_table("login_rate_limits")
    op.drop_index("ix_data_plane_tickets_ticket", table_name="data_plane_tickets")
    op.drop_index("ix_data_plane_tickets_expires", table_name="data_plane_tickets")
    op.drop_index("ix_data_plane_tickets_asset_expires", table_name="data_plane_tickets")
    op.drop_table("data_plane_tickets")
    op.drop_index("ix_catalogs_workspace_name", table_name="catalogs")
    op.drop_table("catalogs")
    op.drop_index("ix_browser_sessions_principal", table_name="browser_sessions")
    op.drop_index("ix_browser_sessions_expires_at", table_name="browser_sessions")
    op.drop_table("browser_sessions")
    op.drop_table("auth_providers")
    op.drop_index("ix_audit_events_workspace_keyset", table_name="audit_events")
    op.drop_index("ix_audit_events_resource", table_name="audit_events")
    op.drop_table("audit_events")
