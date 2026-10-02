"""Dependencies and explicit transaction scope for one control command."""

from dataclasses import dataclass

from sqlalchemy.orm import Session

from dal_obscura.sources.plugins import PluginRegistry
from dal_obscura.sources.secrets import SecretProvider


@dataclass(frozen=True)
class ControlContext:
    session: Session
    catalog_egress_allowlist: tuple[str, ...] = ()
    plugin_registry: PluginRegistry | None = None
    secret_provider: SecretProvider | None = None
