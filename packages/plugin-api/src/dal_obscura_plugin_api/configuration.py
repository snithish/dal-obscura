"""The bounded configuration field language shared by plugins and the host."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from math import isfinite
from typing import cast


class ConfigFieldType(StrEnum):
    STRING = "string"
    URI = "uri"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    NUMBER = "number"
    ENUM = "enum"
    SECRET_REFERENCE = "secret_reference"


_FIELD_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,127}\Z")


@dataclass(frozen=True, slots=True)
class ConfigField:
    name: str
    type: ConfigFieldType = ConfigFieldType.STRING
    required: bool = False
    secret: bool = False
    options: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _FIELD_NAME.fullmatch(self.name):
            raise ValueError("Plugin configuration field name is invalid")
        if self.type not in ConfigFieldType:
            raise ValueError("Plugin configuration field type is unsupported")
        object.__setattr__(self, "type", ConfigFieldType(self.type))
        if not isinstance(self.required, bool) or not isinstance(self.secret, bool):
            raise ValueError("Plugin configuration field flags must be boolean")
        if self.secret and self.type != "secret_reference":
            raise ValueError("Secret configuration fields must use secret_reference")
        if any(
            not isinstance(option, str) or not option or len(option) > 128
            for option in self.options
        ) or len(set(self.options)) != len(self.options):
            raise ValueError("Plugin enum choices must be unique bounded strings")
        if self.type == "enum" and not self.options:
            raise ValueError("Plugin enum fields require choices")
        if self.type != "enum" and self.options:
            raise ValueError("Only enum fields can declare choices")
        object.__setattr__(self, "options", tuple(self.options))
        object.__setattr__(self, "secret", self.type == "secret_reference")

    def validate(self, value: object) -> None:
        """Validate an unresolved option; secret authorization remains with the host."""
        if self.type in {"string", "uri"}:
            if isinstance(value, Mapping):
                _validate_secret_reference(value)
            elif not isinstance(value, str):
                raise ValueError("must be a string")
        if self.type == "boolean" and not isinstance(value, bool):
            raise ValueError("must be a boolean")
        if self.type == "integer" and (isinstance(value, bool) or not isinstance(value, int)):
            raise ValueError("must be an integer")
        if self.type == "number" and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or (isinstance(value, float) and not isfinite(value))
        ):
            raise ValueError("must be a finite number")
        if self.type == "enum" and value not in self.options:
            raise ValueError("must be one of the declared choices")
        if self.type == "secret_reference":
            _validate_secret_reference(value)


def _validate_secret_reference(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {"secret", "scope"}:
        raise ValueError("must be an explicit secret reference")
    reference = cast(Mapping[str, object], value)
    name, scope = reference.get("secret"), reference.get("scope")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("has an invalid secret reference")
    if not isinstance(scope, str) or not scope.strip() or len(scope) > 4096:
        raise ValueError("has an invalid secret scope")


def config_fields(schema: Mapping[str, object]) -> tuple[ConfigField, ...] | None:
    """Validate declared fields once, retaining absent-vs-empty schema semantics."""
    if "additionalProperties" in schema and not isinstance(schema["additionalProperties"], bool):
        raise ValueError("Plugin configuration additionalProperties must be boolean")
    if "fields" not in schema:
        if schema.get("defaults"):
            raise ValueError("Plugin configuration defaults require declared fields")
        return None if schema.get("additionalProperties", True) else ()
    raw_fields = schema["fields"]
    if not isinstance(raw_fields, (list, tuple)):
        raise ValueError("Plugin configuration fields must be a list")
    fields: dict[str, ConfigField] = {}
    for raw in raw_fields:
        if not isinstance(raw, Mapping):
            raise ValueError("Plugin configuration field must be an object")
        field = _field(cast(Mapping[str, object], raw))
        if field.name in fields:
            raise ValueError("Plugin configuration contains duplicate field names")
        fields[field.name] = field
    raw_defaults = schema.get("defaults", {})
    if not isinstance(raw_defaults, Mapping):
        raise ValueError("Plugin configuration defaults must be an object")
    defaults = cast(Mapping[str, object], raw_defaults)
    if set(defaults) - fields.keys():
        raise ValueError("Plugin configuration defaults must refer to declared fields")
    for name, value in defaults.items():
        try:
            fields[name].validate(value)
        except ValueError as exc:
            raise ValueError(f"Plugin configuration default {name!r} {exc}") from exc
    return tuple(fields.values())


def _field(raw: Mapping[str, object]) -> ConfigField:
    field_type = raw.get("type", "string")
    name = raw.get("name")
    required, secret = raw.get("required", False), raw.get("secret", False)
    options = raw.get("options", ())
    if not isinstance(name, str):
        raise ValueError("Plugin configuration field name is invalid")
    if not isinstance(field_type, str) or field_type not in ConfigFieldType:
        raise ValueError("Plugin configuration field type is unsupported")
    if not isinstance(required, bool) or not isinstance(secret, bool):
        raise ValueError("Plugin configuration field flags must be boolean")
    if not isinstance(options, (list, tuple)) or any(
        not isinstance(value, str) for value in options
    ):
        raise ValueError("Plugin enum choices must be strings")
    return ConfigField(
        name,
        ConfigFieldType(field_type),
        required,
        secret,
        tuple(cast(list[str] | tuple[str, ...], options)),
    )
