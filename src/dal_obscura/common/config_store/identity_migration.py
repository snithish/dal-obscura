"""Offline conversion of legacy federated identity keys.

The runtime never falls back to this parser. Operators preview the conversion,
review unresolved values, then apply one transaction during maintenance mode.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.orm import (
    AssetGrantRecord,
    AssetOwnerRecord,
    AssetPolicyDraftRecord,
    AuditEventRecord,
    AuthProviderRecord,
    DataPlaneTicketRecord,
    PolicyRuleRecord,
    PublicationOperationRecord,
)
from dal_obscura.common.identity import encode_federated_group, encode_federated_identity


class IdentityMigrationError(ValueError):
    """Raised when persisted identity data cannot be mapped unambiguously."""


@dataclass(frozen=True)
class IdentityMigrationReport:
    converted: int
    unresolved: tuple[str, ...]
    ambiguous: tuple[str, ...]
    by_table: dict[str, int]

    @property
    def safe_to_apply(self) -> bool:
        return not self.unresolved and not self.ambiguous

    def to_dict(self) -> dict[str, object]:
        return {
            "converted": self.converted,
            "unresolved": list(self.unresolved),
            "ambiguous": list(self.ambiguous),
            "by_table": dict(self.by_table),
            "safe_to_apply": self.safe_to_apply,
        }


def inspect_identity_keys(session: Session) -> IdentityMigrationReport:
    """Preview legacy identity mappings without modifying the session."""

    issuers = _configured_issuers(session)
    values = list(_identity_values(session))
    converted = 0
    unresolved: set[str] = set()
    ambiguous: set[str] = set()
    by_table: Counter[str] = Counter()
    for table, value in values:
        result = _convert(value, issuers)
        if result is None:
            continue
        if isinstance(result, IdentityMigrationError):
            if "ambiguous" in str(result):
                ambiguous.add(value)
            else:
                unresolved.add(value)
            continue
        if result != value:
            converted += 1
            by_table[table] += 1
    return IdentityMigrationReport(
        converted=converted,
        unresolved=tuple(sorted(unresolved)),
        ambiguous=tuple(sorted(ambiguous)),
        by_table=dict(sorted(by_table.items())),
    )


def apply_identity_key_migration(session: Session) -> IdentityMigrationReport:
    """Convert all known legacy keys in one caller-owned transaction."""

    report = inspect_identity_keys(session)
    if not report.safe_to_apply:
        raise IdentityMigrationError(
            "Identity migration has unresolved or ambiguous mappings; review the preview first"
        )
    issuers = _configured_issuers(session)
    by_table: Counter[str] = Counter()
    converted = 0
    for table, value, setter in _mutable_identity_values(session):
        mapped = _convert(value, issuers)
        if isinstance(mapped, IdentityMigrationError):
            raise mapped
        if mapped is not None and mapped != value:
            setter(mapped)
            converted += 1
            by_table[table] += 1
    session.flush()
    return IdentityMigrationReport(
        converted=converted,
        unresolved=(),
        ambiguous=(),
        by_table=dict(sorted(by_table.items())),
    )


def _configured_issuers(session: Session) -> tuple[str, ...]:
    values: set[str] = set()
    for provider in session.scalars(select(AuthProviderRecord)):
        args = provider.args_json if isinstance(provider.args_json, dict) else {}
        for key in ("issuer", "authority"):
            value = args.get(key)
            if isinstance(value, str) and value:
                values.add(value)
    return tuple(sorted(values, key=lambda item: (-len(item), item)))


def _identity_values(session: Session):
    for row in session.scalars(select(AssetOwnerRecord)):
        yield "asset_owners", row.principal
    for row in session.scalars(select(AssetGrantRecord)):
        yield "asset_grants", row.principal
    for row in session.scalars(select(AssetPolicyDraftRecord)):
        yield "asset_policy_drafts", row.author_principal
    for row in session.scalars(select(AuditEventRecord)):
        yield "audit_events", row.actor_principal
    for row in session.scalars(select(PublicationOperationRecord)):
        yield "publication_operations", row.actor_principal
    for row in session.scalars(select(DataPlaneTicketRecord)):
        yield "data_plane_tickets", row.principal_id
    for row in session.scalars(select(PolicyRuleRecord)):
        for principal in row.principals_json:
            yield "policy_rules", principal


def _mutable_identity_values(session: Session):
    for row in session.scalars(select(AssetOwnerRecord)):
        yield "asset_owners", row.principal, lambda value, row=row: setattr(row, "principal", value)
    for row in session.scalars(select(AssetGrantRecord)):
        yield "asset_grants", row.principal, lambda value, row=row: setattr(row, "principal", value)
    for row in session.scalars(select(AssetPolicyDraftRecord)):
        yield (
            "asset_policy_drafts",
            row.author_principal,
            lambda value, row=row: setattr(row, "author_principal", value),
        )
    for row in session.scalars(select(AuditEventRecord)):
        yield (
            "audit_events",
            row.actor_principal,
            lambda value, row=row: setattr(row, "actor_principal", value),
        )
    for row in session.scalars(select(PublicationOperationRecord)):
        yield (
            "publication_operations",
            row.actor_principal,
            lambda value, row=row: setattr(row, "actor_principal", value),
        )
    for row in session.scalars(select(DataPlaneTicketRecord)):
        yield (
            "data_plane_tickets",
            row.principal_id,
            lambda value, row=row: setattr(row, "principal_id", value),
        )
    for row in session.scalars(select(PolicyRuleRecord)):
        principals = list(row.principals_json)
        for index, principal in enumerate(principals):
            yield (
                "policy_rules",
                principal,
                lambda value, row=row, index=index, principals=principals: _set_policy_principal(
                    row, index, principals, value
                ),
            )


def _set_policy_principal(
    row: PolicyRuleRecord, index: int, principals: list[str], value: str
) -> None:
    principals[index] = value
    row.principals_json = principals


def _convert(value: str, issuers: tuple[str, ...]) -> str | IdentityMigrationError | None:
    if not isinstance(value, str) or "|" not in value:
        return None
    # Local actors intentionally retain their legacy unscoped representation;
    # only values that look like federated issuer-prefixed keys are candidates.
    if value.startswith(("local|", "group:local|")):
        return None
    matches: list[tuple[str, str, bool]] = []
    for configured in issuers:
        variants = (
            (configured, configured.rstrip("/")) if configured.endswith("/") else (configured,)
        )
        for issuer in variants:
            prefix = issuer + "|"
            if value.startswith(prefix):
                matches.append((configured, value[len(prefix) :], issuer == configured))
                break
    if len(matches) != 1:
        reason = "ambiguous" if len(matches) > 1 else "unresolved"
        return IdentityMigrationError(f"{reason} legacy identity key {value!r}")
    issuer, subject, exact_issuer = matches[0]
    if exact_issuer and ("%7C" in subject or "%25" in subject):
        # Already-canonical escaped values are safe to leave in place. A
        # legacy value using the slash-stripped issuer and escapes is
        # indistinguishable from a literal encoded subject and must be
        # reapproved rather than guessed.
        return value
    if not exact_issuer and ("%7C" in subject or "%25" in subject):
        return IdentityMigrationError(f"ambiguous legacy identity key {value!r}")
    if subject.startswith("group:"):
        return encode_federated_group(issuer, subject[6:])
    return encode_federated_identity(issuer, subject)
