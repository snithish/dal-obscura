"""Validation and redaction for the supported identity-provider contract."""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite
from typing import Any, cast
from urllib.parse import urlsplit

from dal_obscura.control_plane.application.errors import ValidationFailure

OIDC_IDENTITY_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
)

_SUPPORTED_ARGUMENTS = frozenset(
    {
        "issuer",
        "audience",
        "jwks_url",
        "algorithms",
        "subject_claim",
        "group_claims",
        "attribute_claims",
        "leeway_seconds",
        "jwks_refresh_interval_seconds",
        "max_jwks_keys",
    }
)
_SAFE_ALGORITHMS = frozenset({"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"})
_MAX_CLAIM_PATH_LENGTH = 256
_MAX_CLAIM_ENTRIES = 32
_REDACTED_ARGUMENTS = frozenset(
    {
        "access_token",
        "api_key",
        "client_secret",
        "credential",
        "credentials",
        "password",
        "passwd",
        "private_key",
        "secret",
        "token",
        "jwks",
        "jwks_file",
    }
)


def validate_auth_provider_payloads(providers: list[dict[str, Any]]) -> None:
    """Validates the OIDC provider chain before it is persisted or published."""

    if len(providers) > 16:
        raise ValidationFailure("Authentication provider chain exceeds 16 entries")
    ordinals: set[int] = set()
    for index, raw in enumerate(providers, start=1):
        module = raw.get("module")
        if module != OIDC_IDENTITY_MODULE:
            raise ValidationFailure(
                "Unsupported identity provider; only built-in OIDC is supported"
            )
        ordinal = raw.get("ordinal")
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or not 1 <= ordinal <= 1_000:
            raise ValidationFailure(f"Authentication provider {index} ordinal must be 1-1000")
        if ordinal in ordinals:
            raise ValidationFailure(f"Authentication provider ordinal {ordinal} is duplicated")
        ordinals.add(ordinal)
        enabled = raw.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValidationFailure(f"Authentication provider {index} enabled must be a boolean")
        args = raw.get("args", {})
        if not isinstance(args, Mapping):
            raise ValidationFailure(f"Authentication provider {index} args must be an object")
        _validate_oidc_args(cast(Mapping[str, object], args), index=index)


def redact_auth_provider(provider: Mapping[str, object]) -> dict[str, object]:
    """Returns a browser-safe provider record, redacting legacy sensitive rows."""

    result = {
        key: provider[key]
        for key in ("id", "ordinal", "module", "args", "enabled", "revision")
        if key in provider
    }
    args = provider.get("args", {})
    if isinstance(args, Mapping):
        result["args"] = _redact_mapping(cast(Mapping[str, object], args))
    return result


def _validate_oidc_args(args: Mapping[str, object], *, index: int) -> None:  # noqa: C901
    if "jwks" in args or "jwks_file" in args:
        raise ValidationFailure(
            f"Authentication provider {index} cannot persist static JWKS material"
        )
    unknown = sorted(
        str(key)
        for key, value in args.items()
        if str(key) not in _SUPPORTED_ARGUMENTS
        and not (value == "[redacted]" and str(key).lower() in _REDACTED_ARGUMENTS)
    )
    if unknown:
        raise ValidationFailure(
            f"Authentication provider {index} has unsupported arguments: {', '.join(unknown)}"
        )
    issuer = args.get("issuer")
    if not isinstance(issuer, str) or not issuer.strip():
        raise ValidationFailure(f"Authentication provider {index} requires a non-empty issuer")
    _validate_endpoint(issuer, label=f"Authentication provider {index} issuer")
    jwks_url = args.get("jwks_url")
    if jwks_url is not None:
        if not isinstance(jwks_url, str) or not jwks_url.strip():
            raise ValidationFailure(f"Authentication provider {index} jwks_url must be a URL")
        _validate_endpoint(jwks_url, label=f"Authentication provider {index} jwks_url")
    algorithms = args.get("algorithms")
    if algorithms is not None:
        if not isinstance(algorithms, (list, tuple)) or not algorithms:
            raise ValidationFailure(f"Authentication provider {index} algorithms must be non-empty")
        if any(not isinstance(item, str) or item not in _SAFE_ALGORITHMS for item in algorithms):
            raise ValidationFailure(
                f"Authentication provider {index} algorithms contain an unsupported value"
            )
    audience = args.get("audience")
    if audience is not None and (
        not isinstance(audience, str | list | tuple)
        or (isinstance(audience, str) and not audience.strip())
        or (
            isinstance(audience, (list, tuple))
            and (
                not audience
                or any(not isinstance(item, str) or not item.strip() for item in audience)
            )
        )
    ):
        raise ValidationFailure(
            f"Authentication provider {index} audience must be text or a non-empty list"
        )
    subject_claim = args.get("subject_claim", "sub")
    _validate_claim_path(subject_claim, label=f"Authentication provider {index} subject_claim")
    group_claims = args.get("group_claims")
    if group_claims is not None:
        if (
            not isinstance(group_claims, (list, tuple))
            or len(group_claims) > _MAX_CLAIM_ENTRIES
            or any(not isinstance(item, str) for item in group_claims)
        ):
            raise ValidationFailure(
                f"Authentication provider {index} group_claims must be a bounded list"
            )
        for item in group_claims:
            _validate_claim_path(item, label=f"Authentication provider {index} group_claim")
    attribute_claims = args.get("attribute_claims")
    if attribute_claims is not None:
        if (
            not isinstance(attribute_claims, Mapping)
            or len(attribute_claims) > _MAX_CLAIM_ENTRIES
            or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in attribute_claims.items()
            )
        ):
            raise ValidationFailure(
                f"Authentication provider {index} attribute_claims must be a bounded mapping"
            )
        for key, value in attribute_claims.items():
            _validate_claim_path(key, label=f"Authentication provider {index} attribute name")
            _validate_claim_path(value, label=f"Authentication provider {index} attribute claim")
    leeway = args.get("leeway_seconds", 0)
    if isinstance(leeway, bool) or not isinstance(leeway, int) or not 0 <= leeway <= 300:
        raise ValidationFailure(f"Authentication provider {index} leeway_seconds must be 0-300")
    refresh = args.get("jwks_refresh_interval_seconds", 30)
    if (
        isinstance(refresh, bool)
        or not isinstance(refresh, (int, float))
        or not isfinite(refresh)
        or not 0 < refresh <= 86_400
    ):
        raise ValidationFailure(
            f"Authentication provider {index} jwks_refresh_interval_seconds is invalid"
        )
    max_keys = args.get("max_jwks_keys", 256)
    if isinstance(max_keys, bool) or not isinstance(max_keys, int) or not 1 <= max_keys <= 4_096:
        raise ValidationFailure(f"Authentication provider {index} max_jwks_keys must be 1-4096")


def _validate_claim_path(value: object, *, label: str) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > _MAX_CLAIM_PATH_LENGTH
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)
        or any(not part.strip() for part in value.split("."))
    ):
        raise ValidationFailure(f"{label} must be a bounded dotted claim path")


def _validate_endpoint(value: str, *, label: str) -> None:
    try:
        parsed = urlsplit(value.strip())
        hostname = parsed.hostname
        # Accessing ``port`` validates bracketed hosts and numeric port ranges.
        _ = parsed.port
    except ValueError as exc:
        raise ValidationFailure(f"{label} must be a valid HTTP(S) URL") from exc
    if parsed.scheme not in {"http", "https"} or not hostname:
        raise ValidationFailure(f"{label} must use an HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValidationFailure(f"{label} must not contain credentials, query data, or a fragment")


def _redact_mapping(args: Mapping[str, object]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in args.items():
        name = str(key).lower()
        if name in _REDACTED_ARGUMENTS:
            result[str(key)] = "[redacted]"
        elif isinstance(value, Mapping):
            result[str(key)] = _redact_mapping(cast(Mapping[str, object], value))
        elif isinstance(value, list):
            result[str(key)] = [
                _redact_mapping(cast(Mapping[str, object], item))
                if isinstance(item, Mapping)
                else item
                for item in value
            ]
        else:
            result[str(key)] = value
    return result
