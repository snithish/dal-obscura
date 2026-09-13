from __future__ import annotations

from uuid import uuid4

from dal_obscura.control_plane.application.policy_version_service import (
    _catalogs_with_selected,
)
from dal_obscura.control_plane.domain.models import CatalogDraft, CompiledCatalog


def test_republishing_a_catalog_replaces_its_compiled_configuration() -> None:
    tenant_id = uuid4()
    current = CompiledCatalog(
        tenant_id=tenant_id,
        catalog="analytics",
        config={"module": "iceberg", "options": {"uri": "https://old.example"}},
    )
    selected = CatalogDraft(
        id=uuid4(),
        cell_id=uuid4(),
        tenant_id=tenant_id,
        name="analytics",
        module="iceberg.rest",
        options={"uri": "https://new.example"},
        revision=3,
    )

    result = _catalogs_with_selected([current], selected)

    assert len(result) == 1
    assert result[0].config == {
        "module": "iceberg.rest",
        "options": {"uri": "https://new.example"},
        "revision": 3,
    }
