"""Deliberately invalid provider output for trust-boundary tests."""

from dal_obscura_plugin_api import DiscoveryPage


def _unchecked_discovery_page(entries: object, continuation: object) -> DiscoveryPage:
    page = object.__new__(DiscoveryPage)
    object.__setattr__(page, "entries", entries)
    object.__setattr__(page, "continuation", continuation)
    return page
