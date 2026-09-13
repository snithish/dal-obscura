#!/usr/bin/env python3
"""Generate an immutable plugin lock from installed static descriptors."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterable
from contextlib import suppress
from importlib import metadata
from pathlib import Path
from typing import cast

from dal_obscura_plugin_api import PluginKind

from dal_obscura.common.plugin_api.registry import (
    ENTRY_POINT_GROUPS,
    PluginAdmissionError,
    build_plugin_lock,
    load_static_plugin_descriptor,
)


def parse_selection(value: str) -> tuple[PluginKind, str]:
    kind, separator, plugin_id = value.partition(":")
    if not separator or kind not in ENTRY_POINT_GROUPS or not plugin_id:
        raise PluginAdmissionError("plugin selection must be catalog:ID or table_format:ID")
    return cast(PluginKind, kind), plugin_id


def _entry_points() -> Iterable[tuple[PluginKind, metadata.EntryPoint]]:
    points = metadata.entry_points()
    for kind, group in ENTRY_POINT_GROUPS.items():
        selected = (
            points.select(group=group) if hasattr(points, "select") else points.get(group, ())
        )
        for entry in selected:
            yield kind, entry


def build_document(
    selections: Iterable[tuple[PluginKind, str]],
    *,
    entry_points: Iterable[tuple[PluginKind, metadata.EntryPoint]] | None = None,
) -> dict[str, object]:
    requested = tuple(selections)
    if not requested:
        raise PluginAdmissionError("at least one plugin selection is required")
    selected: dict[tuple[PluginKind, str], metadata.EntryPoint] = {}
    available = _entry_points() if entry_points is None else entry_points
    for kind, entry in available:
        key = (kind, str(entry.name))
        if key in selected:
            raise PluginAdmissionError(f"duplicate installed plugin ID: {kind}:{entry.name}")
        selected[key] = entry

    rows: list[dict[str, object]] = []
    seen: set[tuple[PluginKind, str]] = set()
    for kind, plugin_id in requested:
        key = (kind, plugin_id)
        if key in seen:
            raise PluginAdmissionError(f"duplicate requested plugin: {kind}:{plugin_id}")
        seen.add(key)
        entry = selected.get(key)
        if entry is None:
            raise PluginAdmissionError(f"selected plugin is not installed: {kind}:{plugin_id}")
        descriptor = load_static_plugin_descriptor(entry)
        lock = build_plugin_lock(kind, entry, descriptor)
        rows.append({"kind": kind, "plugin_id": plugin_id, "lock": list(lock)})
    rows.sort(key=lambda row: (str(row["kind"]), str(row["plugin_id"])))
    return {"version": 1, "plugins": rows}


def write_lock(path: Path, document: dict[str, object]) -> None:
    if path.exists() or path.is_symlink():
        raise PluginAdmissionError(f"refusing to overwrite existing plugin lock: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    if temporary.exists() or temporary.is_symlink():
        raise PluginAdmissionError(f"temporary plugin lock already exists: {temporary}")
    payload = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, path)
    except OSError as exc:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)
        raise PluginAdmissionError("could not write plugin lock") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--plugin",
        action="append",
        required=True,
        metavar="KIND:ID",
        help="repeat for each explicitly admitted catalog or table_format plugin",
    )
    args = parser.parse_args(argv)
    try:
        document = build_document(parse_selection(value) for value in args.plugin)
        write_lock(args.output, document)
    except PluginAdmissionError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"plugin lock written: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
