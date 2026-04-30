from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON


class Base(DeclarativeBase):
    pass


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TenantRecord(Base):
    __tablename__ = "tenants"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CellRecord(Base):
    __tablename__ = "cells"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    region: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class CellTenantRecord(Base):
    __tablename__ = "cell_tenants"

    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), primary_key=True)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), primary_key=True)
    shard_key: Mapped[str] = mapped_column(String(120), nullable=False, default="default")


class CellRuntimeSettingsRecord(Base):
    __tablename__ = "cell_runtime_settings"

    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), primary_key=True)
    ticket_ttl_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    max_tickets: Mapped[int] = mapped_column(Integer, nullable=False)
    path_rules_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )


class CatalogRecord(Base):
    __tablename__ = "catalogs"
    __table_args__ = (UniqueConstraint("cell_id", "tenant_id", "name"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), nullable=False)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    module: Mapped[str] = mapped_column(Text, nullable=False)
    options_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)


class AssetRecord(Base):
    __tablename__ = "assets"
    __table_args__ = (UniqueConstraint("cell_id", "tenant_id", "catalog_id", "target"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), nullable=False)
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    catalog_id: Mapped[UUID] = mapped_column(ForeignKey("catalogs.id"), nullable=False)
    target: Mapped[str] = mapped_column(Text, nullable=False)
    backend: Mapped[str] = mapped_column(String(48), nullable=False)
    table_identifier: Mapped[str | None] = mapped_column(Text, nullable=True)
    options_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)


class PolicyRuleRecord(Base):
    __tablename__ = "policy_rules"
    __table_args__ = (UniqueConstraint("asset_id", "ordinal"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    asset_id: Mapped[UUID] = mapped_column(ForeignKey("assets.id"), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    effect: Mapped[str] = mapped_column(String(16), nullable=False)
    principals_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    when_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    columns_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    masks_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    row_filter_sql: Mapped[str | None] = mapped_column(Text, nullable=True)


class AuthProviderRecord(Base):
    __tablename__ = "auth_providers"
    __table_args__ = (UniqueConstraint("cell_id", "ordinal"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    module: Mapped[str] = mapped_column(Text, nullable=False)
    args_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class ConfigPublicationRecord(Base):
    __tablename__ = "config_publications"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), nullable=False, index=True)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="published")
    manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ActivePublicationRecord(Base):
    __tablename__ = "active_publications"

    cell_id: Mapped[UUID] = mapped_column(ForeignKey("cells.id"), primary_key=True)
    publication_id: Mapped[UUID] = mapped_column(
        ForeignKey("config_publications.id"),
        nullable=False,
    )
    activated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class PublishedCellRuntimeRecord(Base):
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
    __tablename__ = "published_catalogs"

    publication_id: Mapped[UUID] = mapped_column(
        ForeignKey("config_publications.id"),
        primary_key=True,
    )
    tenant_id: Mapped[UUID] = mapped_column(ForeignKey("tenants.id"), primary_key=True)
    catalog: Mapped[str] = mapped_column(String(160), primary_key=True)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class PublishedAssetRecord(Base):
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
    policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
