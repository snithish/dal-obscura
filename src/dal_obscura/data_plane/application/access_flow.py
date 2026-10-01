"""Shared dependency bundle for data-plane read planning and fetch execution.

Example:
    ```python
    flow = AccessFlow(
        identity=identity,
        access_context=access_context,
        masking=masking,
        row_transform=row_transform,
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=32,
        max_ticket_exchanges=1,
    )
    ```
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

from dal_obscura.data_plane.application.ports.access_context import AccessContextPort
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
    """Immutable dependencies and runtime settings for one data-plane request flow.

    Example:
        ```python
        result = plan_read(flow, request, auth_request)
        ```
    """

    identity: IdentityPort
    access_context: AccessContextPort | None
    masking: MaskingPort
    row_transform: RowTransformPort | None
    ticket_codec: TicketCodecPort
    ticket_store: TicketStorePort
    ticket_ttl_seconds: int
    max_tickets: int
    max_ticket_exchanges: int
    max_ticket_payload_bytes: int = 16 * 1024 * 1024
    max_stream_seconds: int = 300
    now: Callable[[], int] = _epoch_seconds
    nonce_factory: Callable[[], str] = _nonce
    ticket_id_factory: Callable[[], str] = _ticket_id
