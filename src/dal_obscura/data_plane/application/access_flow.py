from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

from dal_obscura.data_plane.application.ports.authorization import AuthorizationPort
from dal_obscura.data_plane.application.ports.catalog import CatalogRegistryPort
from dal_obscura.data_plane.application.ports.identity import IdentityPort
from dal_obscura.data_plane.application.ports.masking import MaskingPort
from dal_obscura.data_plane.application.ports.row_transform import RowTransformPort
from dal_obscura.data_plane.application.ports.ticket_codec import TicketCodecPort
from dal_obscura.data_plane.application.ports.ticket_store import TicketStorePort


def _epoch_seconds() -> int:
    return int(time.time())


def _nonce() -> str:
    return os.urandom(16).hex()


def _ticket_id() -> str:
    return str(uuid4())


@dataclass(frozen=True)
class AccessFlow:
    identity: IdentityPort
    authorizer: AuthorizationPort
    catalog_registry: CatalogRegistryPort
    masking: MaskingPort
    row_transform: RowTransformPort | None
    ticket_codec: TicketCodecPort
    ticket_store: TicketStorePort
    ticket_ttl_seconds: int
    max_tickets: int
    max_ticket_exchanges: int
    now: Callable[[], int] = _epoch_seconds
    nonce_factory: Callable[[], str] = _nonce
    ticket_id_factory: Callable[[], str] = _ticket_id
