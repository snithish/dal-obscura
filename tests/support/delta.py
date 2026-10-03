"""Real Delta logs with portable deletion-vector fixtures, without service imports."""

import json
import struct
import uuid
import zlib
from pathlib import Path

from deltalake import DeltaTable

_Z85 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"


def _z85_encode(data: bytes) -> str:
    padded = data + b"\0" * (-len(data) % 4)
    result = []
    for offset in range(0, len(padded), 4):
        value = int.from_bytes(padded[offset : offset + 4], "big")
        digits = []
        for _ in range(5):
            value, remainder = divmod(value, 85)
            digits.append(_Z85[remainder])
        result.extend(reversed(digits))
    return "".join(result)


def install_deletion_vector(path: Path, indices, *, storage="i", corrupt=False):
    """Publish a valid merge-on-read commit, leaving the data file untouched.

    Fixture uses the protocol's portable Roaring64 layout with one small array
    container. Its expected row positions are supplied independently by tests.
    This is a writer fixture only, not a snapshot or DV reader implementation.
    """
    indices = sorted(set(indices))
    if not indices or not 0 <= indices[0] <= indices[-1] < 65_536 or len(indices) > 4096:
        raise ValueError("Fixture requires one nonempty Roaring array container")
    bitmap = struct.pack(
        "<IQII IHHI", 1681511377, 1, 0, 12346, 1, 0, len(indices) - 1, 16
    ) + struct.pack(f"<{len(indices)}H", *indices)
    log = path / "_delta_log"
    original = next(
        json.loads(line)["add"]
        for line in (log / "00000000000000000000.json").read_text().splitlines()
        if "add" in json.loads(line)
    )
    for commit in sorted(log.glob("*.json")):
        for line in commit.read_text().splitlines():
            action = json.loads(line).get("add")
            if action and action["path"] == original["path"]:
                original = action
    remove = {"path": original["path"], "deletionTimestamp": 0, "dataChange": True}
    if "deletionVector" in original:
        remove["deletionVector"] = original["deletionVector"]
    dv = {"storageType": storage, "sizeInBytes": len(bitmap), "cardinality": len(indices)}
    if storage == "i":
        dv["pathOrInlineDv"] = _z85_encode(bitmap)
    else:
        identifier = uuid.uuid4()
        location = path / f"deletion_vector_{identifier}.bin"
        checksum = zlib.crc32(bitmap) ^ int(corrupt)
        location.write_bytes(
            b"\x01" + struct.pack(">I", len(bitmap)) + bitmap + struct.pack(">I", checksum)
        )
        dv.update(
            offset=1,
            pathOrInlineDv=_z85_encode(identifier.bytes) if storage == "u" else location.as_uri(),
        )
    original["deletionVector"] = dv
    version = DeltaTable(path).version() + 1
    (log / f"{version:020d}.json").write_text(
        json.dumps({"remove": remove}) + "\n" + json.dumps({"add": original}) + "\n"
    )
    return dv
