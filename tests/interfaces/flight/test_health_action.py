from __future__ import annotations

import logging

import pyarrow.flight as flight
import pytest

from dal_obscura.data_plane.interfaces.flight.server import _health_payload


def test_health_payload_without_probe_preserves_process_liveness_contract():
    assert _health_payload(None, logging.getLogger("test")) == {
        "status": "ok",
        "service": "data-plane",
    }


def test_health_payload_includes_published_runtime_checks():
    payload = _health_payload(
        lambda: {
            "status": "ready",
            "checks": {"active_publication": "ok"},
            "publication_id": "pub-1",
        },
        logging.getLogger("test"),
    )

    assert payload == {
        "status": "ok",
        "service": "data-plane",
        "checks": {"active_publication": "ok"},
        "publication_id": "pub-1",
    }


def test_health_payload_fails_closed_when_runtime_is_not_ready():
    with pytest.raises(flight.FlightUnavailableError):
        _health_payload(
            lambda: {"status": "not_ready", "checks": {"runtime": "missing"}},
            logging.getLogger("test"),
        )
