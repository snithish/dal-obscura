from __future__ import annotations

from unittest.mock import Mock
from uuid import uuid4

import pytest

from dal_obscura.control_plane.application import asset_service
from dal_obscura.control_plane.application.errors import ValidationFailure


def test_owner_replacement_locks_before_read_and_write() -> None:
    store = Mock()
    store.list_asset_owners.return_value = []
    store.replace_asset_owners.return_value = ["user:owner"]
    asset_id = uuid4()

    assert asset_service.replace_asset_owners(
        store, asset_id, ["user:owner"], expected_revision=0
    ) == ["user:owner"]

    assert store.method_calls[:3] == [
        ("lock_asset_for_update", (asset_id,), {}),
        ("list_asset_owners", (asset_id,), {}),
        (
            "replace_asset_owners",
            (),
            {"asset_id": asset_id, "owners": ["user:owner"], "expected_revision": 0},
        ),
    ]


def test_grant_replacement_locks_before_write() -> None:
    store = Mock()
    store.replace_asset_grants.return_value = []
    asset_id = uuid4()

    assert asset_service.replace_asset_grants(store, asset_id, [], expected_revision=0) == []

    assert store.method_calls == [
        ("lock_asset_for_update", (asset_id,), {}),
        (
            "replace_asset_grants",
            (),
            {"asset_id": asset_id, "grants": [], "expected_revision": 0},
        ),
    ]


def test_schema_admission_replacement_locks_before_write() -> None:
    store = Mock()
    store.replace_asset_schema_fields.return_value = []
    asset_id = uuid4()

    assert asset_service.replace_asset_schema_fields(store, asset_id, [], expected_revision=0) == []

    assert store.method_calls == [
        ("lock_asset_for_update", (asset_id,), {}),
        (
            "replace_asset_schema_fields",
            (),
            {"asset_id": asset_id, "fields": [], "expected_revision": 0},
        ),
    ]


def test_schema_identity_input_errors_are_safe_validation_failures() -> None:
    store = Mock()
    store.replace_asset_schema_fields.side_effect = ValueError("field id is unsafe")

    with pytest.raises(ValidationFailure, match="field id is unsafe"):
        asset_service.replace_asset_schema_fields(store, uuid4(), [{"name": "id"}])
