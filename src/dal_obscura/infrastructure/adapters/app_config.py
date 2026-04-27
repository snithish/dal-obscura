from __future__ import annotations

import importlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from dal_obscura.application.ports.identity import IdentityPort
from dal_obscura.infrastructure.adapters.identity_default import AuthConfig, DefaultIdentityAdapter
from dal_obscura.infrastructure.adapters.secret_providers import SecretProvider


class _StrictModel(BaseModel):
    """Base model used by config schemas to reject unknown keys."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class _SecretProviderConfig(_StrictModel):
    module: str = Field(min_length=1)
    args: dict[str, Any] = Field(default_factory=dict)

    @field_validator("module")
    @classmethod
    def _validate_module(cls, value: str) -> str:
        module = value.strip()
        if "." not in module:
            raise ValueError("Secret provider module must be a fully qualified class path")
        return module


class _SecretKeyRef(_StrictModel):
    key: str = Field(min_length=1)


class _TicketInputConfig(_StrictModel):
    ttl_seconds: int = Field(gt=0)
    max_tickets: int = Field(gt=0)
    secret: _SecretKeyRef


class _AuthProviderInputConfig(_StrictModel):
    module: str = Field(min_length=1)
    args: dict[str, Any] = Field(default_factory=dict)

    @field_validator("module")
    @classmethod
    def _validate_module(cls, value: str) -> str:
        module = value.strip()
        if "." not in module:
            raise ValueError("Auth provider module must be a fully qualified class path")
        return module


class _LegacyAuthInputConfig(_StrictModel):
    jwt_secret: _SecretKeyRef
    jwt_issuer: str | None = None
    jwt_audience: str | None = None


class LoggingConfig(_StrictModel):
    level: str = "INFO"
    json_output: bool = Field(default=True, alias="json")

    @field_validator("level")
    @classmethod
    def _validate_level(cls, value: str) -> str:
        level = value.strip()
        if not level:
            raise ValueError("Logging level must be non-empty")
        return level.upper()


class AppTicketConfig(_StrictModel):
    ttl_seconds: int = Field(gt=0)
    max_tickets: int = Field(gt=0)
    secret: str = Field(min_length=1)


class AppAuthConfig(_StrictModel):
    identity_provider: Any


class AppConfig(_StrictModel):
    """Top-level runtime configuration loaded from app.yaml."""

    location: str
    catalog_file: Path
    policy_file: Path
    ticket: AppTicketConfig
    auth: AppAuthConfig
    logging: LoggingConfig


def load_app_config(path: str | Path) -> AppConfig:
    """Loads and validates app config from a YAML file."""
    app_path = Path(path)
    raw = _load_yaml_object(app_path, "App config")

    _validate_top_level_keys(raw)

    location = _read_non_empty_string(raw, "location", default="grpc://0.0.0.0:8815")
    catalog_file_raw = _read_non_empty_string(raw, "catalog_file", default="catalogs.yaml")
    policy_file_raw = _read_non_empty_string(raw, "policy_file", default="policies.yaml")

    try:
        provider_config = _SecretProviderConfig.model_validate(_require_key(raw, "secret_provider"))
        ticket_input = _TicketInputConfig.model_validate(_require_key(raw, "ticket"))
        logging = LoggingConfig.model_validate(raw.get("logging", {}))
    except ValidationError as exc:
        raise ValueError(f"Invalid app config: {exc}") from exc

    provider = _load_secret_provider(provider_config)
    auth_provider = _load_identity_provider(_require_key(raw, "auth"), provider)
    base_dir = app_path.parent
    catalog_file = _resolve_relative_path(base_dir, catalog_file_raw)
    policy_file = _resolve_relative_path(base_dir, policy_file_raw)

    ticket_secret = _resolve_secret(provider, ticket_input.secret.key, "ticket.secret")

    return AppConfig.model_validate(
        {
            "location": location,
            "catalog_file": catalog_file,
            "policy_file": policy_file,
            "ticket": {
                "ttl_seconds": ticket_input.ttl_seconds,
                "max_tickets": ticket_input.max_tickets,
                "secret": ticket_secret,
            },
            "auth": {"identity_provider": auth_provider},
            "logging": logging.model_dump(by_alias=True),
        }
    )


def _load_yaml_object(path: Path, label: str) -> dict[str, object]:
    """Loads a YAML object document from disk."""
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError(f"{label} must be .yaml or .yml")
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must contain a YAML object")
    return raw


_ALLOWED_TOP_LEVEL_KEYS = {
    "location",
    "catalog_file",
    "policy_file",
    "secret_provider",
    "ticket",
    "auth",
    "logging",
}


def _validate_top_level_keys(raw: dict[str, object]) -> None:
    """Ensures the app config does not include unknown top-level keys."""
    unknown = sorted(set(raw) - _ALLOWED_TOP_LEVEL_KEYS)
    if unknown:
        raise ValueError(f"Invalid app config: unknown field(s): {', '.join(unknown)}")


def _require_key(raw: dict[str, object], key: str) -> object:
    """Reads a required top-level key from the raw app config."""
    if key not in raw:
        raise ValueError(f"Invalid app config: missing required field {key!r}")
    return raw[key]


def _read_non_empty_string(raw: dict[str, object], key: str, *, default: str) -> str:
    """Reads a non-empty string field from the top-level app config."""
    value = raw.get(key, default)
    if not isinstance(value, str):
        raise ValueError(f"Invalid app config: field {key!r} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"Invalid app config: field {key!r} must be non-empty")
    return normalized


def _load_secret_provider(config: _SecretProviderConfig) -> SecretProvider:
    """Dynamically imports and instantiates the configured secret provider."""
    module_path = config.module
    provider_cls = _load_class(module_path, "Secret provider")
    if not issubclass(provider_cls, SecretProvider):
        raise ValueError(f"Secret provider {module_path!r} must inherit from SecretProvider")

    try:
        provider = provider_cls(**config.args)
    except Exception as exc:
        raise ValueError(f"Failed to instantiate secret provider {module_path!r}: {exc}") from exc
    return provider


def _load_identity_provider(raw_auth: object, provider: SecretProvider) -> IdentityPort:
    """Loads the configured identity provider, including legacy JWT config."""
    if not isinstance(raw_auth, Mapping):
        raise ValueError("Invalid app config: field 'auth' must be an object")
    if "module" in raw_auth:
        try:
            auth_config = _AuthProviderInputConfig.model_validate(raw_auth)
        except ValidationError as exc:
            raise ValueError(f"Invalid app config: {exc}") from exc
        args = cast(dict[str, Any], _resolve_secret_refs(provider, auth_config.args, "auth.args"))
        return _load_identity_provider_from_module(auth_config.module, args)

    try:
        legacy = _LegacyAuthInputConfig.model_validate(raw_auth)
    except ValidationError as exc:
        raise ValueError(f"Invalid app config: {exc}") from exc
    jwt_secret = _resolve_secret(provider, legacy.jwt_secret.key, "auth.jwt_secret")
    return DefaultIdentityAdapter(
        AuthConfig(
            jwt_secret=jwt_secret,
            jwt_issuer=legacy.jwt_issuer,
            jwt_audience=legacy.jwt_audience,
        )
    )


def _load_identity_provider_from_module(module_path: str, args: dict[str, Any]) -> IdentityPort:
    """Dynamically imports and instantiates the configured identity provider."""
    provider_cls = _load_class(module_path, "Auth provider")
    try:
        identity_provider = provider_cls(**args)
    except Exception as exc:
        raise ValueError(f"Failed to instantiate auth provider {module_path!r}: {exc}") from exc
    authenticate = getattr(identity_provider, "authenticate", None)
    if not callable(authenticate):
        raise ValueError(f"Auth provider {module_path!r} must define authenticate(request)")
    return cast(IdentityPort, identity_provider)


def _load_class(module_path: str, label: str) -> type:
    """Dynamically imports a class from a fully qualified class path."""
    module_name, class_name = module_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise ValueError(f"Failed to import {label.lower()} module {module_name!r}: {exc}") from exc

    provider_cls = getattr(module, class_name, None)
    if provider_cls is None:
        raise ValueError(f"{label} class {class_name!r} not found in module {module_name!r}")
    if not isinstance(provider_cls, type):
        raise ValueError(f"{label} {module_path!r} must be a class")
    return provider_cls


def _resolve_secret_refs(provider: SecretProvider, value: object, field_name: str) -> object:
    """Recursively resolves secret reference objects inside provider args."""
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        secret_key = mapping.get("key")
        if set(mapping) == {"key"} and isinstance(secret_key, str):
            return _resolve_secret(provider, secret_key, field_name)
        return {
            str(key): _resolve_secret_refs(provider, nested, f"{field_name}.{key}")
            for key, nested in mapping.items()
        }
    if isinstance(value, list):
        return [
            _resolve_secret_refs(provider, item, f"{field_name}[{index}]")
            for index, item in enumerate(value)
        ]
    return value


def _resolve_relative_path(base_dir: Path, value: str) -> Path:
    """Resolves relative paths against the app config file directory."""
    path = Path(value)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _resolve_secret(provider: SecretProvider, key: str, field_name: str) -> str:
    """Resolves a secret through the configured provider."""
    value = provider.get_secret(key)
    if value is None or value == "":
        raise ValueError(
            f"Secret {key!r} for {field_name} could not be resolved by "
            f"{provider.__class__.__name__}"
        )
    return value
