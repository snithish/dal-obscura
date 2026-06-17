from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import httpx

from dal_obscura.common.catalog.ports import (
    CatalogPlugin,
    CatalogTableDescriptor,
    CatalogTableListing,
)
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

    def list_tables(self, *, catalog_name: str, schema_name: str) -> list[dict[str, Any]]:
        response = self._client.get(
            f"{self._base_url}/tables",
            headers=self._headers,
            params={"catalog_name": catalog_name, "schema_name": schema_name},
        )
        response.raise_for_status()
        payload = response.json()
        tables = payload.get("tables", []) if isinstance(payload, dict) else []
        return [dict(item) for item in tables if isinstance(item, dict)]

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
        path_enforcer: PathRuleEnforcer | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._name = name
        self.options = dict(options)
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

    def list_tables(self) -> list[CatalogTableListing]:
        uc_catalog = _optional_str(self.options.get("uc_catalog"))
        if uc_catalog is None:
            raise ValueError("Unity Catalog table listing requires 'uc_catalog' option")
        listings: list[CatalogTableListing] = []
        for schema in _schemas(self.options):
            for raw in self._client.list_tables(catalog_name=uc_catalog, schema_name=schema):
                table = _unity_table(raw)
                try:
                    provider_id = _backend_from_uc_format(table.data_source_format)
                except ValueError:
                    provider_id = "unknown"
                listings.append(
                    CatalogTableListing(
                        name=_relative_unity_name(table.full_name, uc_catalog),
                        provider_id=provider_id,
                        table_identifier=table.storage_location
                        or _relative_unity_name(table.full_name, uc_catalog),
                    )
                )
        return sorted(listings, key=lambda item: item.name)


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


def _schemas(options: dict[str, Any]) -> list[str]:
    schemas = options.get("schemas", options.get("schema_names"))
    if isinstance(schemas, list):
        return [schema for item in schemas if (schema := str(item).strip())]
    schema = _optional_str(options.get("schema"))
    return [schema] if schema else []


def _relative_unity_name(full_name: str, uc_catalog: str) -> str:
    prefix = f"{uc_catalog}."
    if full_name.startswith(prefix):
        return full_name[len(prefix) :]
    return full_name


def _object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value).copy()
    return {}


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
