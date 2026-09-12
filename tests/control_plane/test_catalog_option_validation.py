from __future__ import annotations

import pytest

from dal_obscura.control_plane.application.catalog_service import validate_catalog_options
from dal_obscura.control_plane.application.errors import ValidationFailure


def test_catalog_options_reject_non_finite_numbers() -> None:
    with pytest.raises(ValidationFailure, match="numbers must be finite"):
        validate_catalog_options({"properties": {"timeout": float("nan")}})
