"""Immutable global plugin options with explicit instance precedence."""

from collections.abc import Mapping, Set
from dataclasses import dataclass
from typing import cast

from dal_obscura_plugin_api.contracts import _validate_options
from dal_obscura_plugin_api.tasks import _freeze_json, _mutable_json


@dataclass(frozen=True)
class PluginOptions:
    values: Mapping[str, Mapping[str, object]]

    def __post_init__(self) -> None:
        for options in self.values.values():
            _validate_options(options)
        object.__setattr__(self, "values", _freeze_json(self.values))

    @classmethod
    def from_conf(cls, values: Mapping[str, object], admitted: Set[str]) -> PluginOptions:
        grouped: dict[str, dict[str, object]] = {}
        for key, value in values.items():
            matches = [plugin for plugin in admitted if key.startswith(f"dal.plugins.{plugin}.")]
            if not matches:
                raise ValueError(f"Unknown plugin configuration namespace: {key}")
            plugin = cast(str, max(matches, key=len))
            option = key[len(f"dal.plugins.{plugin}.") :]
            if not option or any(ord(char) < 0x20 for char in option):
                raise ValueError("Plugin option name must be non-empty printable text")
            grouped.setdefault(plugin, {})[option] = value
        return cls(grouped)

    def to_json(self) -> dict[str, dict[str, object]]:
        return cast(dict[str, dict[str, object]], _mutable_json(self.values))

    def for_plugin(
        self, plugin_id: str, instance: Mapping[str, object] | None = None
    ) -> Mapping[str, object]:
        merged = {**self.values.get(plugin_id, {}), **(instance or {})}
        _validate_options(merged)
        return cast(Mapping[str, object], _freeze_json(merged))


def batch_rows(options: Mapping[str, object], *, default: int) -> int:
    if set(options) - {"batch_rows"}:
        raise ValueError("Format options support batch_rows only")
    value = options.get("batch_rows", default)
    if type(value) is not int or not 1 <= value <= 65_536:
        raise ValueError("batch_rows must be an integer between 1 and 65536")
    return value
