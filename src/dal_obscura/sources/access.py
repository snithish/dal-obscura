"""Atomic binding of executable source and authorization for one request."""

from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from dal_obscura.policy.authorization import AuthorizationPort
from dal_obscura.sources.contracts import Source


class AccessContextUnavailable(RuntimeError):
    """Planning cannot obtain a provider within its configured resource budget."""


@dataclass(frozen=True)
class AccessContext:
    table_format: Source
    authorizer: AuthorizationPort
    schema: pa.Schema


class AccessContextPort(Protocol):
    def open(self, catalog: str | None, target: str) -> AbstractContextManager[AccessContext]:
        """Bind source, policy and admission consistently; lease provider until exit."""
        ...
