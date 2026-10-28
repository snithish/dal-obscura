"""Immutable plugin artifact identities shared by admission and operator tools."""

from __future__ import annotations

import re
from typing import NamedTuple

_HEX_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


class PluginAdmissionError(ValueError):
    """Raised when installed plugin metadata is not explicitly admitted."""


class PluginLock(NamedTuple):
    """Immutable admitted artifact identity, in the version-one wire order."""

    distribution: str
    version: str
    api_version: str
    descriptor_digest: str
    artifact_digest: str

    @classmethod
    def from_json(cls, raw: object) -> PluginLock:
        """Decode the bounded five-string JSON lock without accepting tuple coercions."""
        if not isinstance(raw, list) or len(raw) != 5:
            raise PluginAdmissionError("Plugin lock entry must contain a five-part lock")
        values: list[str] = []
        for value in raw:
            if not isinstance(value, str) or not value:
                raise PluginAdmissionError("Plugin lock entry must contain a five-part lock")
            values.append(value)
        distribution, version, api_version, descriptor_digest, artifact_digest = values
        if any(
            any(ord(char) < 0x20 or ord(char) == 0x7F for char in value) or len(value) > 256
            for value in (distribution, version, api_version)
        ):
            raise PluginAdmissionError("Plugin lock identity values must be printable and bounded")
        if not _HEX_DIGEST.fullmatch(descriptor_digest) or not _HEX_DIGEST.fullmatch(
            artifact_digest
        ):
            raise PluginAdmissionError("Plugin lock digests must be lowercase SHA-256 values")
        return cls(distribution, version, api_version, descriptor_digest, artifact_digest)
