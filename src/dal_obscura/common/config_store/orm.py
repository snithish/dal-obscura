"""SQLAlchemy ORM records for the control-plane configuration store.

Example:
    ```python
    from dal_obscura.common.config_store.orm import Base

    Base.metadata.create_all(engine)
    ```
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON


class Base(DeclarativeBase):
    """Declarative base for all configuration-store ORM records."""


def utcnow() -> datetime:
    """Returns a timezone-aware UTC timestamp for ORM defaults."""

    return datetime.now(timezone.utc)


class TenantRecord(Base):
    """Workspace tenant row."""

    __tablename__ = "tenants"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CellRecord(Base):
    """Data-plane cell row."""

    __tablename__ = "cells"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    region: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CellTenantRecord(Base):
    """Assignment row connecting one tenant to one data-plane cell."""

    __tablename__ = "cell_tenants"

    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), primary_key=True)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), primary_key=True)
    shard_key: Mapped[str] = mapped_column(String(120), nullable=False, default="default")


class CellRuntimeSettingsRecord(Base):
    """Draft runtime settings for a data-plane cell."""

    __tablename__ = "cell_runtime_settings"

    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), primary_key=True)
    ticket_ttl_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    max_tickets: Mapped[int] = mapped_column(Integer, nullable=False)
    max_ticket_exchanges: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    path_rules_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )


class CatalogRecord(Base):
    """Draft catalog configuration row."""

    __tablename__ = "catalogs"
    __table_args__ = (
        UniqueConstraint("cell_id", "tenant_id", "name"),
        Index("ix_catalogs_workspace_name", "cell_id", "tenant_id", "name"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), nullable=False, index=True)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    module: Mapped[str] = mapped_column(Text, nullable=False)
    options_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)


class AssetRecord(Base):
    """Draft asset configuration row."""

    __tablename__ = "assets"
    __table_args__ = (
        UniqueConstraint("cell_id", "tenant_id", "catalog_id", "target"),
        Index("ix_assets_workspace_target", "cell_id", "tenant_id", "target", "id"),
        Index("ix_assets_workspace_catalog", "cell_id", "tenant_id", "catalog_id"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), nullable=False, index=True)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False, index=True)
    catalog_id: Mapped[UUID] = mapped_column(ForeignKey("catalogs.id"), nullable=False, index=True)
    target: Mapped[str] = mapped_column(Text, nullable=False)
    backend: Mapped[str] = mapped_column(String(48), nullable=False)
    table_identifier: Mapped[str | None] = mapped_column(Text, nullable=True)
    options_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)


class AssetOwnerRecord(Base):
    """Ordered owner principal row for an asset."""

    __tablename__ = "asset_owners"
    __table_args__ = (UniqueConstraint("asset_id", "principal"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    asset_id: Mapped[UUID] = mapped_column(ForeignKey("assets.id"), nullable=False, index=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    principal: Mapped[str] = mapped_column(Text, nullable=False)


class AssetGrantRecord(Base):
    """Explicit capability grant for one asset principal."""

    __tablename__ = "asset_grants"
    __table_args__ = (
        UniqueConstraint("asset_id", "principal", "capability"),
        Index("ix_asset_grants_asset_principal", "asset_id", "principal"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    asset_id: Mapped[UUID] = mapped_column(ForeignKey("assets.id"), nullable=False)
    principal: Mapped[str] = mapped_column(Text, nullable=False)
    capability: Mapped[str] = mapped_column(String(24), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class AssetSchemaFieldRecord(Base):
    """Schema field metadata row for an asset."""

    __tablename__ = "asset_schema_fields"
    __table_args__ = (UniqueConstraint("asset_id", "name"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    asset_id: Mapped[UUID] = mapped_column(ForeignKey("assets.id"), nullable=False, index=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    type: Mapped[str] = mapped_column(String(120), nullable=False)
    nullable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class PolicyRuleRecord(Base):
    """Policy rule row attached to an asset draft."""

    __tablename__ = "policy_rules"
    __table_args__ = (UniqueConstraint("asset_id", "ordinal"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    asset_id: Mapped[UUID] = mapped_column(ForeignKey("assets.id"), nullable=False, index=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    effect: Mapped[str] = mapped_column(String(16), nullable=False)
    principals_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    when_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    columns_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    masks_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    row_filter_sql: Mapped[str | None] = mapped_column(Text, nullable=True)


class AuthProviderRecord(Base):
    """Draft authentication provider row for a cell."""

    __tablename__ = "auth_providers"
    __table_args__ = (UniqueConstraint("cell_id", "ordinal"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    module: Mapped[str] = mapped_column(Text, nullable=False)
    args_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class BrowserSessionRecord(Base):
    """Opaque, revocable browser session record.

    The provider access token is never stored in this table.  The browser only
    receives the random session secret; the database stores its SHA-256 digest.
    """

    __tablename__ = "browser_sessions"
    __table_args__ = (
        UniqueConstraint("token_hash"),
        Index("ix_browser_sessions_expires_at", "expires_at"),
        Index("ix_browser_sessions_principal", "principal"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    principal: Mapped[str] = mapped_column(Text, nullable=False)
    groups_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    platform_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class LoginTransactionRecord(Base):
    """Short-lived one-time OIDC authorization transaction."""

    __tablename__ = "login_transactions"
    __table_args__ = (
        UniqueConstraint("state_hash"),
        Index("ix_login_transactions_expires_at", "expires_at"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    state_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    nonce_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    code_verifier: Mapped[str] = mapped_column(String(128), nullable=False)
    redirect_uri: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ConfigPublicationRecord(Base):
    """Immutable publication manifest row."""

    __tablename__ = "config_publications"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), nullable=False, index=True)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="published")
    manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ActivePublicationRecord(Base):
    """Pointer to the currently active publication for a cell."""

    __tablename__ = "active_publications"

    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), primary_key=True)
    publication_id: Mapped[UUID] = mapped_column(
        ForeignKey("config_publications.id"),
        nullable=False,
    )
    activated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class PublishedCellRuntimeRecord(Base):
    """Runtime settings captured inside an immutable publication."""

    __tablename__ = "published_cell_runtime"

    publication_id: Mapped[UUID] = mapped_column(
        ForeignKey("config_publications.id"),
        primary_key=True,
    )
    auth_chain_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    ticket_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    path_rules_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )


class PublishedCatalogRecord(Base):
    """Catalog configuration captured inside an immutable publication."""

    __tablename__ = "published_catalogs"

    publication_id: Mapped[UUID] = mapped_column(
        ForeignKey("config_publications.id"),
        primary_key=True,
    )
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), primary_key=True)
    catalog: Mapped[str] = mapped_column(String(160), primary_key=True)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class PublishedAssetRecord(Base):
    """Asset policy and backend configuration captured inside a publication."""

    __tablename__ = "published_assets"

    publication_id: Mapped[UUID] = mapped_column(
        ForeignKey("config_publications.id"),
        primary_key=True,
    )
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), primary_key=True)
    catalog: Mapped[str] = mapped_column(String(160), primary_key=True)
    target: Mapped[str] = mapped_column(Text, primary_key=True)
    backend: Mapped[str] = mapped_column(String(48), nullable=False)
    compiled_config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    policy_version: Mapped[int] = mapped_column(BigInteger, nullable=False)


class ActivePublishedAssetRecord(Base):
    """Pointer to the active published version for one asset."""

    __tablename__ = "active_published_assets"
    __table_args__ = (Index("ix_active_published_assets_publication", "publication_id"),)

    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), primary_key=True)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), primary_key=True)
    catalog: Mapped[str] = mapped_column(String(160), primary_key=True)
    target: Mapped[str] = mapped_column(Text, primary_key=True)
    publication_id: Mapped[UUID] = mapped_column(
        ForeignKey("config_publications.id"),
        nullable=False,
    )


class DataPlaneTicketRecord(Base):
    """Durable ticket exchange row used by data-plane ticket stores."""

    __tablename__ = "data_plane_tickets"
    __table_args__ = (
        Index("ix_data_plane_tickets_cell_ticket", "cell_id", "ticket_id"),
        Index("ix_data_plane_tickets_cell_expires", "cell_id", "expires_at"),
    )

    ticket_id: Mapped[UUID] = mapped_column(primary_key=True)
    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), nullable=False)
    tenant_id: Mapped[str] = mapped_column(Text, nullable=False)
    catalog: Mapped[str | None] = mapped_column(Text, nullable=True)
    target: Mapped[str] = mapped_column(Text, nullable=False)
    principal_id: Mapped[str] = mapped_column(Text, nullable=False)
    policy_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    expires_at: Mapped[int] = mapped_column(Integer, nullable=False)
    max_exchanges: Mapped[int] = mapped_column(Integer, nullable=False)
    exchange_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_exchanged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
