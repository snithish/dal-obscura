"""Lossless, passive Iceberg v2 file tasks; never serialize executable objects.

The field list is the pinned PyIceberg v2 manifest contract. Typed partition
values and integer-keyed metrics use a small explicit codec, without imports or
class names in the payload. Native read/delete semantics stay with PyIceberg.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

from pydantic import SerializeAsAny, TypeAdapter
from pyiceberg.expressions import BooleanExpression
from pyiceberg.manifest import DATA_FILE_TYPE, DataFile, DataFileContent, FileFormat
from pyiceberg.table import FileScanTask
from pyiceberg.typedef import Record

_FILE_FIELDS = tuple(field.name for field in DATA_FILE_TYPE[2].fields)
_EXPRESSION = TypeAdapter(SerializeAsAny[BooleanExpression])


def encode_scan_task(task: FileScanTask) -> dict[str, object]:
    if not isinstance(task, FileScanTask):
        raise ValueError("Invalid Iceberg task")
    return {
        "version": 1,
        "file": _encode_file(task.file),
        "delete_files": [
            _encode_file(file) for file in sorted(task.delete_files, key=lambda f: f.file_path)
        ],
        "residual": _encode_value(_EXPRESSION.dump_python(task.residual, mode="python")),
    }


def decode_scan_task(payload: object) -> FileScanTask:
    if not isinstance(payload, dict):
        raise ValueError("Invalid Iceberg task")
    payload = cast(dict[str, Any], payload)
    if (
        set(payload) != {"version", "file", "delete_files", "residual"}
        or type(payload["version"]) is not int
        or payload["version"] != 1
        or not isinstance(payload["delete_files"], list)
    ):
        raise ValueError("Invalid Iceberg task")
    try:
        return FileScanTask(
            _decode_file(payload["file"]),
            {_decode_file(file) for file in payload["delete_files"]},
            _EXPRESSION.validate_python(_decode_value(payload["residual"])),
        )
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise ValueError("Invalid Iceberg task") from exc


def _encode_file(file: DataFile) -> dict[str, object]:
    values = {name: _encode_value(getattr(file, name)) for name in _FILE_FIELDS}
    values["content"] = int(file.content)
    values["file_format"] = file.file_format.value
    values["spec_id"] = file.spec_id
    return values


def _decode_file(payload: object) -> DataFile:
    if not isinstance(payload, dict) or set(payload) != {*_FILE_FIELDS, "spec_id"}:
        raise ValueError("Invalid Iceberg file")
    payload = cast(dict[str, Any], payload)
    values = {name: _decode_value(payload[name]) for name in _FILE_FIELDS}
    if not isinstance(values["file_path"], str) or not values["file_path"]:
        raise ValueError("Invalid Iceberg file path")
    if not isinstance(values["partition"], Record):
        raise ValueError("Invalid Iceberg partition")
    for name in ("record_count", "file_size_in_bytes"):
        if type(values[name]) is not int or values[name] < 0:
            raise ValueError("Invalid Iceberg file size")
    if type(payload["spec_id"]) is not int or payload["spec_id"] < 0:
        raise ValueError("Invalid Iceberg partition spec")
    values["content"] = DataFileContent(values["content"])
    values["file_format"] = FileFormat(values["file_format"])
    file = DataFile.from_args(_table_format_version=2, **values)
    file.spec_id = payload["spec_id"]
    return file


def _encode_value(value: Any) -> object:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        return {"float": value.hex()}
    if isinstance(value, bytes):
        return {"bytes": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, UUID):
        return {"uuid": str(value)}
    for kind in (datetime, date, time):
        if isinstance(value, kind):
            return {kind.__name__: value.isoformat()}
    if isinstance(value, Record):
        return {"record": [_encode_value(value[index]) for index in range(len(value))]}
    if isinstance(value, Mapping):
        return {"map": [[_encode_value(key), _encode_value(item)] for key, item in value.items()]}
    if isinstance(value, set | frozenset):
        values = [_encode_value(item) for item in value]
        return {"set": sorted(values, key=lambda item: json.dumps(item, sort_keys=True))}
    if isinstance(value, list | tuple):
        return [_encode_value(item) for item in value]
    raise ValueError("Unsupported Iceberg task value")


def _decode_value(value: Any, depth: int = 0) -> Any:
    if depth > 32:
        raise ValueError("Iceberg task exceeds nesting limit")
    if value is None or type(value) in (str, bool, int):
        return value
    if isinstance(value, list):
        return [_decode_value(item, depth + 1) for item in value]
    if not isinstance(value, dict) or len(value) != 1:
        raise ValueError("Invalid Iceberg task value")
    tag, content = next(iter(cast(dict[str, Any], value).items()))
    if tag in {"map", "record", "set"}:
        return _decode_collection(tag, content, depth)
    if not isinstance(content, str):
        raise ValueError("Invalid Iceberg scalar")
    decoders = {
        "float": float.fromhex,
        "bytes": lambda text: base64.b64decode(text, validate=True),
        "decimal": Decimal,
        "uuid": UUID,
        "date": date.fromisoformat,
        "time": time.fromisoformat,
        "datetime": datetime.fromisoformat,
    }
    if tag not in decoders:
        raise ValueError("Unknown Iceberg task value tag")
    return decoders[tag](content)


def _decode_collection(tag: str, content: Any, depth: int) -> Any:
    if not isinstance(content, list):
        raise ValueError("Invalid Iceberg collection")
    if tag == "set":
        return {_decode_value(item, depth + 1) for item in content}
    if tag == "record":
        return Record(*(_decode_value(item, depth + 1) for item in content))
    result = {}
    for pair in content:
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError("Invalid Iceberg map entry")
        key = _decode_value(pair[0], depth + 1)
        if key in result:
            raise ValueError("Duplicate Iceberg map key")
        result[key] = _decode_value(pair[1], depth + 1)
    return result
