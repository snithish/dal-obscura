from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from importlib import metadata
from typing import Any, cast

from dal_obscura.domain.query_planning.models import (
    BackendDescriptor,
    BackendReference,
    BoundBackendTarget,
    Plan,
    ReadSpec,
)
from dal_obscura.infrastructure.adapters.file_backend import DuckDBFileBackend
from dal_obscura.infrastructure.adapters.iceberg_backend import IcebergBackend
from dal_obscura.infrastructure.adapters.implementation_base import (
    BackendImplementation,
    BackendRegistration,
)

_BACKEND_PLUGIN_GROUP = "dal_obscura.backend_implementations"


@dataclass(frozen=True)
class _RuntimeSnapshot:
    """Immutable backend registry for a single generation."""

    generation: int
    backends: Mapping[str, BackendImplementation]


class DynamicBackendRegistry:
    """Stores generation-bound backend implementations and looks them up by reference."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._backend_factories: dict[str, BackendRegistration] = {}
        self._snapshots: dict[int, _RuntimeSnapshot] = {}
        self._current_generation = 0

        self._register_builtin_backends()
        self._register_entrypoints()
        self.reload()

    @property
    def current_generation(self) -> int:
        """Generation attached to newly bound backend references."""
        with self._lock:
            return self._current_generation

    def register_backend(self, backend_id: str, implementation: BackendRegistration) -> None:
        """Registers a backend factory under the ID stored in backend references."""
        with self._lock:
            self._backend_factories[backend_id] = implementation

    def reload(self) -> int:
        """Publishes a new backend snapshot and increments the active generation."""
        with self._lock:
            self._current_generation += 1
            generation = self._current_generation
            self._snapshots[generation] = _RuntimeSnapshot(
                generation=generation,
                backends={
                    backend_id: _coerce_backend_implementation(implementation)
                    for backend_id, implementation in self._backend_factories.items()
                },
            )
            return generation

    def unload_generation(self, generation: int) -> None:
        """Drops an old generation once tickets for it can no longer be used."""
        with self._lock:
            if generation == self._current_generation:
                raise ValueError("Cannot unload the current backend generation")
            self._snapshots.pop(generation, None)

    def bind_descriptor(self, descriptor: BackendDescriptor) -> BoundBackendTarget:
        """Binds a resolved descriptor to the current backend generation."""
        reference = BackendReference(
            backend_id=descriptor.backend_id,
            generation=self.current_generation,
        )
        implementation = self._backend_for_reference(reference)
        binding = implementation.bind(descriptor)
        return BoundBackendTarget(
            dataset_identity=descriptor.dataset_identity,
            backend=reference,
            binding=binding,
        )

    def schema_for(self, bound_target: BoundBackendTarget) -> Any:
        """Delegates schema lookup for a bound dataset."""
        implementation = self._backend_for_reference(bound_target.backend)
        return implementation.get_schema(bound_target)

    def plan_for(
        self, bound_target: BoundBackendTarget, columns: Iterable[str], max_tickets: int
    ) -> Plan:
        """Delegates plan creation for a bound dataset."""
        implementation = self._backend_for_reference(bound_target.backend)
        return implementation.plan(bound_target, columns, max_tickets)

    def read_spec_for(self, backend: BackendReference, read_payload: bytes) -> ReadSpec:
        """Reads ticket metadata using the generation-bound backend implementation."""
        implementation = self._backend_for_reference(backend)
        return implementation.read_spec(read_payload)

    def read_stream_for(self, backend: BackendReference, read_payload: bytes) -> Iterable[Any]:
        """Executes a read stream using the generation-bound backend implementation."""
        implementation = self._backend_for_reference(backend)
        return implementation.read_stream(read_payload)

    def _backend_for_reference(self, reference: BackendReference) -> BackendImplementation:
        """Looks up the backend instance referenced by a bound target or ticket."""
        with self._lock:
            snapshot = self._snapshots.get(reference.generation)
            if snapshot is None:
                raise ValueError(f"Backend generation {reference.generation} is unavailable")
            backend = snapshot.backends.get(reference.backend_id)
            if backend is None:
                raise ValueError(
                    f"Backend {reference.backend_id!r} is unavailable in generation "
                    f"{reference.generation}"
                )
            return backend

    def _register_builtin_backends(self) -> None:
        """Registers the built-in backends available without plugins."""
        self._backend_factories["iceberg"] = IcebergBackend
        self._backend_factories["duckdb_file"] = DuckDBFileBackend

    def _register_entrypoints(self) -> None:
        """Loads optional backend plugins from Python entry points."""
        for entrypoint in _entry_points_for_group(_BACKEND_PLUGIN_GROUP):
            self.register_backend(entrypoint.name, entrypoint.load())


def _coerce_backend_implementation(candidate: BackendRegistration) -> BackendImplementation:
    """Instantiates or validates a backend implementation registered by the runtime."""
    if isinstance(candidate, BackendImplementation):
        return candidate
    if isinstance(candidate, type):
        if issubclass(candidate, BackendImplementation):
            return candidate()
        raise TypeError(
            "Backend implementation must provide bind(), get_schema(), plan(), "
            "read_spec(), and read_stream()"
        )
    try:
        created = cast(Callable[[], BackendImplementation], candidate)()
    except TypeError as exc:
        raise TypeError(
            "Backend implementation must provide bind(), get_schema(), plan(), "
            "read_spec(), and read_stream()"
        ) from exc
    if isinstance(created, BackendImplementation):
        return created
    raise TypeError(
        "Backend implementation must provide bind(), get_schema(), plan(), "
        "read_spec(), and read_stream()"
    )


def _entry_points_for_group(group: str):
    """Handles `importlib.metadata.entry_points()` API differences across Python versions."""
    discovered = metadata.entry_points()
    if hasattr(discovered, "select"):
        return list(discovered.select(group=group))
    return list(discovered.get(group, []))
