from collections.abc import Iterable, Mapping, Sequence
from typing import Protocol

import pyarrow as pa

from dal_obscura.application.reads.sources import ScanTask
from dal_obscura.kernel.identity.authentication import AuthenticationRequest
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.projection import CompiledProjection
from dal_obscura.kernel.policy.filters import RowFilter
from dal_obscura.kernel.policy.models import MaskRule


class ProjectionCompiler(Protocol):
    def compile(
        self, schema: pa.Schema, columns: Sequence[str], masks: Mapping[str, MaskRule]
    ) -> CompiledProjection: ...


class RowTransformPort(Protocol):
    def apply_filters_and_masks_stream(
        self,
        batches: Iterable[pa.RecordBatch],
        projection: CompiledProjection,
        row_filter: RowFilter | None,
    ) -> Iterable[pa.RecordBatch]: ...


class IdentityPort(Protocol):
    """Authenticates a caller from normalized request context."""

    def authenticate(self, request: AuthenticationRequest) -> Principal: ...


class ScanTaskCodec(Protocol):
    def encode(self, task: ScanTask) -> str: ...
    def decode(self, payload: str) -> ScanTask: ...
