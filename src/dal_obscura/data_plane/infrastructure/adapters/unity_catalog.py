from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import httpx

from dal_obscura.common.catalog.ports import CatalogPlugin, CatalogTableDescriptor
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer


@dataclass(frozen=True)
class UnityTable:
    full_name: str
    table_type: str
    data_source_format: str
    storage_location: str | None


class UnityCatalogClient:
    """Small Unity Catalog REST client covering OSS and Databricks URL shapes."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str | None = None,
        timeout_seconds: float = 10.0,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._base_url = _normalize_base_url(base_url)
        self._client = http_client or httpx.Client(timeout=timeout_seconds)
        self._headers = {"authorization": f"Bearer {token}"} if token else {}

    def get_table(self, full_name: str) -> dict[str, Any]:
        response = self._client.get(
            f"{self._base_url}/tables/{full_name}",
            headers=self._headers,
        )
        response.raise_for_status()
        return _object(response.json())

    def temporary_table_credentials(self, full_name: str) -> dict[str, Any] | None:
        response = self._client.post(
            f"{self._base_url}/temporary-table-credentials",
            headers=self._headers,
            json={"table_id": full_name, "operation": "READ"},
        )
        if response.status_code in {404, 405, 501}:
            return None
        response.raise_for_status()
        return _object(response.json())


class UnityCatalog(CatalogPlugin):
    """Catalog resolver backed by Unity Catalog table metadata."""

    def __init__(
        self,
        name: str,
        options: dict[str, Any],
        targets: dict[str, object],
        path_enforcer: PathRuleEnforcer | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._name = name
        self.options = options
        self.targets = targets
        self._path_enforcer = path_enforcer
        self._client = UnityCatalogClient(
            base_url=str(options["base_url"]),
            token=_optional_str(options.get("token")),
            timeout_seconds=float(options.get("timeout_seconds", 10.0)),
            http_client=http_client,
        )

    @property
    def name(self) -> str:
        return self._name

    def describe_table(self, target: str) -> CatalogTableDescriptor:
        """Resolves Unity Catalog table metadata to a provider-neutral descriptor."""
        target_config = self.targets.get(target)
        if target_config is not None:
            return _descriptor_from_target_config(self.name, target, target_config)

        full_name = _full_name(target, self.options.get("uc_catalog"))
        table = _unity_table(self._client.get_table(full_name))
        _ensure_readable_table(target, table)
        storage_location = table.storage_location
        if storage_location is None:
            raise ValueError(f"Unity Catalog target {target!r} has no storage_location")

        storage_options = dict(_object(self.options.get("storage_options", {})))
        credential_mode = str(self.options.get("credential_mode", "both")).lower()
        if credential_mode in {"both", "uc_temp"}:
            temporary = self._client.temporary_table_credentials(full_name)
            if temporary is not None:
                storage_options.update(_storage_options_from_temporary_credentials(temporary))
            elif credential_mode == "uc_temp":
                raise ValueError(
                    f"Unity Catalog temporary credentials are unavailable for {target!r}"
                )

        backend = _backend_from_uc_format(table.data_source_format)
        return CatalogTableDescriptor(
            catalog_name=self.name,
            requested_target=target,
            provider_id=backend,
            table_identifier=storage_location,
            location=storage_location,
            storage_options=storage_options,
        )


def _normalize_base_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/api/2.1/unity-catalog"):
        return normalized
    return f"{normalized}/api/2.1/unity-catalog"


def _full_name(target: str, uc_catalog: object) -> str:
    configured_catalog = _optional_str(uc_catalog)
    if configured_catalog is None:
        return target
    if target.count(".") == 2:
        return target
    return f"{configured_catalog}.{target}"


def _unity_table(raw: dict[str, Any]) -> UnityTable:
    return UnityTable(
        full_name=str(raw.get("full_name") or raw.get("name") or ""),
        table_type=str(raw.get("table_type") or "MANAGED").upper(),
        data_source_format=str(raw.get("data_source_format") or "").upper(),
        storage_location=_optional_str(raw.get("storage_location")),
    )


def _ensure_readable_table(target: str, table: UnityTable) -> None:
    if table.table_type in {"VIEW", "MATERIALIZED_VIEW", "STREAMING_TABLE"}:
        raise ValueError(f"Unity Catalog target {target!r} is not a readable table")


def _backend_from_uc_format(value: str) -> str:
    backend = value.strip().lower()
    if backend in {"delta", "parquet", "csv", "json", "orc", "avro", "text"}:
        return backend
    raise ValueError(f"Unsupported Unity Catalog table format {value!r}")


def _storage_options_from_temporary_credentials(raw: dict[str, Any]) -> dict[str, object]:
    credentials = _object(raw.get("aws_temp_credentials") or raw.get("aws_temp_credentials_json"))
    if credentials:
        return {
            "AWS_ACCESS_KEY_ID": credentials.get("access_key_id"),
            "AWS_SECRET_ACCESS_KEY": credentials.get("secret_access_key"),
            "AWS_SESSION_TOKEN": credentials.get("session_token"),
        }
    return {}


def _descriptor_from_target_config(
    catalog_name: str,
    requested_target: str,
    target_config: object,
) -> CatalogTableDescriptor:
    backend = _optional_str(getattr(target_config, "backend", None)) or _optional_str(
        getattr(target_config, "format", None)
    )
    table = _optional_str(getattr(target_config, "table", None)) or requested_target
    options = _object(getattr(target_config, "options", {}))
    storage_options = _object(options.pop("storage_options", {}))
    provider_id = (backend or "iceberg").lower()
    return CatalogTableDescriptor(
        catalog_name=catalog_name,
        requested_target=requested_target,
        provider_id=provider_id,
        table_identifier=table,
        location=table if provider_id in _PATH_PROVIDERS else None,
        options=options,
        storage_options=storage_options,
    )


def _object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value).copy()
    return {}


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


_PATH_PROVIDERS = frozenset({"delta", "parquet", "csv", "json", "orc", "avro", "text"})
