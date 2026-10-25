"""Catalog configuration commands, capture and provider discovery."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, TypedDict, cast
from uuid import UUID

from dal_obscura_plugin_api import CatalogConfig, DiscoveryPage

from dal_obscura.application.governance.catalogs.discovery import DiscoveryCursors
from dal_obscura.application.governance.catalogs.validation import (
    validate_admitted_catalog_options,
    validate_catalog_options,
)
from dal_obscura.application.governance.errors import (
    AuthorizationFailure,
    ConfigurationConflictError,
    ValidationFailure,
)
from dal_obscura.application.governance.ports import (
    CatalogBrowser,
    GovernancePluginRegistry,
    GovernanceUnitOfWork,
    GovernanceUnitOfWorkFactory,
)
from dal_obscura.application.governance.values import CatalogDiscovery
from dal_obscura.application.governance.workspace.service import required_workspace
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def _catalog_revision(catalog: dict[str, object]) -> int:
    raw_revision = catalog.get("revision", 0)
    if isinstance(raw_revision, bool):
        return 0
    if isinstance(raw_revision, (int, str)):
        try:
            return max(0, int(raw_revision))
        except ValueError:
            return 0
    return 0


def capture_catalog_configuration(
    catalog: dict[str, object],
    name: str,
    *,
    egress_allowlist: tuple[str, ...] = (),
    plugin_registry: GovernancePluginRegistry | None = None,
) -> CatalogDiscovery:
    options = cast(dict[str, Any], catalog["options"])
    plugin_id = str(catalog["plugin_id"])
    validate_admitted_catalog_options(plugin_id, options, plugin_registry)
    validate_catalog_options(options, egress_allowlist=egress_allowlist)
    try:
        config = CatalogConfig(
            plugin_id=plugin_id,
            instance_id=name,
            revision=_catalog_revision(catalog),
            options=options,
        )
    except ValueError as exc:
        raise ValidationFailure("Catalog configuration is invalid") from exc
    return CatalogDiscovery(UUID(str(catalog["id"])), config)


def ensure_catalog_configuration_current(
    current: dict[str, object], captured: CatalogDiscovery
) -> None:
    revision = _catalog_revision(current)
    if UUID(str(current["id"])) != captured.catalog_id or revision != captured.config.revision:
        raise ConfigurationConflictError(
            "Catalog changed during discovery; restart discovery", current_revision=revision
        )


MAX_CATALOG_PUBLICATION_SIZE = 100


def list_workspace_catalogs(
    store: GovernanceUnitOfWork,
) -> list[dict[str, object]]:
    """Lists catalogs configured in the default workspace."""

    context = store.workspace.get_workspace()
    if context is None:
        return []
    return store.catalogs.list_workspace_catalogs()


def capture_catalog_discovery(
    store: GovernanceUnitOfWork,
    name: str,
    *,
    egress_allowlist: tuple[str, ...] = (),
    plugin_registry: GovernancePluginRegistry | None = None,
) -> CatalogDiscovery:
    """Capture immutable configuration; the route releases this transaction before IO."""

    required_workspace(store)
    catalog = store.catalogs.get_workspace_catalog(name)
    return capture_catalog_configuration(
        catalog, name, egress_allowlist=egress_allowlist, plugin_registry=plugin_registry
    )


def ensure_catalog_discovery_current(
    store: GovernanceUnitOfWork, captured: CatalogDiscovery
) -> None:
    current = store.catalogs.get_workspace_catalog(captured.config.instance_id)
    ensure_catalog_configuration_current(current, captured)


def catalog_discovery_tables(
    store: GovernanceUnitOfWork,
    captured: CatalogDiscovery,
    page: DiscoveryPage,
) -> list[dict[str, object]]:
    """Decorate a bounded page using a fresh catalog revision and indexed asset lookup."""

    from dal_obscura_plugin_api.identifiers import identifier_name

    ensure_catalog_discovery_current(store, captured)
    names = [identifier_name(identifier) for identifier in page.entries]
    governed = store.catalogs.governed_table_identifiers(
        catalog_id=captured.catalog_id, identifiers=names
    )
    return [
        {
            "backend": captured.config.plugin_id,
            "name": name,
            "table_identifier": name,
            "target": name,
            "governed": name in governed,
        }
        for name in names
    ]


def diagnose_workspace_catalog(
    captured: CatalogDiscovery,
    *,
    session_key: str,
    browser: CatalogBrowser,
) -> dict[str, object]:
    """Sample one page; a continuation means the total count is unknown."""

    from dal_obscura_plugin_api.identifiers import identifier_name

    checked_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    result: dict[str, object] = {"catalog": captured.config.instance_id, "checked_at": checked_at}
    try:
        page = browser.discover(
            captured,
            session_key=session_key,
            limit=10,
        )
    except ValidationFailure:
        return {**result, "status": "unavailable", "message": "Catalog discovery failed"}
    return {
        **result,
        "status": "ready",
        "message": "Catalog discovery succeeded",
        "table_count": len(page.entries) if page.continuation is None else None,
        "sample_tables": [identifier_name(identifier) for identifier in page.entries],
    }


def upsert_workspace_catalog(
    store: GovernanceUnitOfWork,
    name: str,
    plugin_id: str,
    options: dict[str, Any],
    egress_allowlist: tuple[str, ...] = (),
    *,
    expected_revision: int | None = None,
    actor_principal: str = "system",
    plugin_registry: GovernancePluginRegistry | None = None,
    correlation_id: str | None = None,
) -> dict[str, str]:
    """Creates or updates a workspace catalog definition."""

    validate_admitted_catalog_options(plugin_id, options, plugin_registry)
    validate_catalog_options(options, egress_allowlist=egress_allowlist)
    store.workspace.ensure_workspace()
    catalog_id = store.catalogs.upsert_catalog(
        name=name, plugin_id=plugin_id, options=options, expected_revision=expected_revision
    )
    store.audit.record_workspace_audit_event(
        actor_principal=actor_principal,
        action="workspace.catalog.update",
        resource_type="catalog",
        resource_id=str(catalog_id),
        details={"name": name, "plugin_id": plugin_id, "option_keys": sorted(options)},
        correlation_id=correlation_id,
    )
    return {"id": str(catalog_id), "name": name}


class CatalogPublicationInput(TypedDict):
    plugin_id: str
    options: dict[str, Any]
    expected_revision: int | None


class CatalogsCommands:
    def __init__(
        self,
        work_factory: GovernanceUnitOfWorkFactory,
        *,
        browser: CatalogBrowser,
        plugin_registry: GovernancePluginRegistry | None,
        discovery_cursors: DiscoveryCursors,
        egress_allowlist: tuple[str, ...] = (),
    ) -> None:
        self._work_factory = work_factory
        self._browser = browser
        self._plugin_registry = plugin_registry
        self._discovery_cursors = discovery_cursors
        self._egress_allowlist = egress_allowlist

    def list_workspace_catalogs(
        self, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> list[dict[str, object]]:
        _require_admin(actor)
        with self._work_factory() as work:
            return list_workspace_catalogs(work)

    def upsert_workspace_catalog(
        self,
        name: str,
        plugin_id: str,
        options: dict[str, Any],
        *,
        expected_revision: int | None = None,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> dict[str, str]:
        _require_admin(actor)
        with self._work_factory() as work:
            result = upsert_workspace_catalog(
                work,
                name=name,
                plugin_id=plugin_id,
                options=options,
                egress_allowlist=self._egress_allowlist,
                expected_revision=expected_revision,
                actor_principal=actor.identity_key(),
                plugin_registry=self._plugin_registry,
                correlation_id=correlation_id,
            )
            work.commit()
            return result

    def publish_workspace_catalogs(
        self,
        catalogs: dict[str, CatalogPublicationInput],
        *,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> list[dict[str, str]]:
        _require_admin(actor)
        # Every catalog and audit record commits together, in deterministic lock order.
        with self._work_factory() as work:
            results = [
                upsert_workspace_catalog(
                    work,
                    name=name,
                    plugin_id=catalogs[name]["plugin_id"],
                    options=catalogs[name]["options"],
                    egress_allowlist=self._egress_allowlist,
                    expected_revision=catalogs[name]["expected_revision"],
                    actor_principal=actor.identity_key(),
                    plugin_registry=self._plugin_registry,
                    correlation_id=correlation_id,
                )
                for name in sorted(catalogs)
            ]
            work.commit()
            return results

    def discover_workspace_catalog_tables(
        self,
        name: str,
        *,
        limit: int = 100,
        cursor: str | None = None,
        actor: ControlPlaneActor,
        correlation_id: str | None,
    ) -> dict[str, object]:
        _require_admin(actor)
        with self._work_factory() as work:
            captured = capture_catalog_discovery(
                work,
                name,
                egress_allowlist=self._egress_allowlist,
                plugin_registry=self._plugin_registry,
            )
        identity = actor.identity_key()
        scope = (str(captured.catalog_id), captured.config.revision, identity, limit)
        continuation = self._discovery_cursors.resolve(cursor, scope=scope) if cursor else None
        page = self._browser.discover(
            captured, session_key=identity, continuation=continuation, limit=limit
        )
        next_cursor = (
            self._discovery_cursors.issue(page.continuation, scope=scope)
            if page.continuation is not None
            else None
        )
        with self._work_factory() as work:
            tables = catalog_discovery_tables(work, captured, page)
        return {
            "catalog": name,
            "revision": captured.config.revision,
            "tables": tables,
            "next_cursor": next_cursor,
        }

    def diagnose_workspace_catalog(
        self, name: str, *, actor: ControlPlaneActor, correlation_id: str | None
    ) -> dict[str, object]:
        _require_admin(actor)
        with self._work_factory() as work:
            captured = capture_catalog_discovery(
                work,
                name,
                egress_allowlist=self._egress_allowlist,
                plugin_registry=self._plugin_registry,
            )
        result = diagnose_workspace_catalog(
            captured, session_key=actor.identity_key(), browser=self._browser
        )
        with self._work_factory() as work:
            ensure_catalog_discovery_current(work, captured)
        return result


def _require_admin(actor: ControlPlaneActor) -> None:
    if not actor.platform_admin:
        raise AuthorizationFailure("Platform admin required")
