"""Shared input model and independent scalar oracle; no production mask compiler."""

import copy
import os
from typing import Any

import pyarrow as pa

PARTITIONS = int(os.environ.get("SCENARIO_PARTITIONS", "10"))
ROWS_PER_PARTITION = int(os.environ.get("SCENARIO_ROWS_PER_PARTITION", "1000002"))
SMOKE = os.environ.get("SCENARIO_SMOKE") == "1"
if PARTITIONS < 10 or (8 if SMOKE else 1000002) > ROWS_PER_PARTITION:
    raise ValueError(
        "Use >=10 partitions and >=1000002 physical rows per partition; "
        "SCENARIO_SMOKE=1 permits small development probes"
    )
ROWS = PARTITIONS * ROWS_PER_PARTITION
if ROWS > 15999999:
    raise ValueError(
        "Delta DV scan budget is 16000000 physical rows, including replacement/later append"
    )
LATER_ID = ROWS
BUCKET = "nested-scenarios"
TARGET = "retail.customers"
COLUMNS = ["id", "partition", "region", "profile", "events", "directory", "private_directory"]
CONTACT = pa.struct(
    [
        pa.field("email", pa.string()),
        pa.field(
            "phones",
            pa.list_(pa.struct([pa.field("number", pa.string()), pa.field("kind", pa.string())])),
        ),
    ]
)
SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("partition", pa.string()),
        pa.field("region", pa.string()),
        pa.field(
            "profile", pa.struct([pa.field("identity", pa.struct([pa.field("contact", CONTACT)]))])
        ),
        pa.field(
            "events",
            pa.list_(
                pa.struct(
                    [
                        pa.field("kind", pa.string()),
                        pa.field(
                            "participants",
                            pa.list_(
                                pa.struct(
                                    [pa.field("role", pa.string()), pa.field("contact", CONTACT)]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
        pa.field(
            "directory",
            pa.map_(
                pa.string(),
                pa.struct([pa.field("label", pa.string()), pa.field("contact", CONTACT)]),
            ),
        ),
        pa.field("private_directory", pa.map_(pa.string(), pa.string())),
    ]
)
MASKS = {
    "profile.identity.contact.email": {"type": "redact", "value": "[email]"},
    "profile.identity.contact.phones.$element.number": {"type": "redact", "value": "[phone]"},
    "events.$element.participants.$element.contact.email": {"type": "redact", "value": "[email]"},
    "events.$element.participants.$element.contact.phones.$element.number": {
        "type": "redact",
        "value": "[phone]",
    },
    "private_directory.$key": {"type": "null"},
    "directory.$value.contact.email": {"type": "redact", "value": "[email]"},
    "directory.$value.contact.phones.$element.number": {"type": "redact", "value": "[phone]"},
}


def row(index, *, updated=False):
    label = "updated" if updated else "original"
    contact = {
        "email": f"{label}-{index}@pii.test",
        "phones": [
            {"number": f"+1555{index:07d}", "kind": "work"},
            {"number": None, "kind": "missing"},
        ],
    }
    directory: list[tuple[str, dict[str, Any] | None]] = [
        ("primary", {"label": label, "contact": copy.deepcopy(contact)})
    ]
    result: dict[str, Any] = {
        "private_directory": [(f"private-{index}@pii.test", "confidential")],
        "id": index,
        "partition": str((index // ROWS_PER_PARTITION) % PARTITIONS),
        "region": "EU" if index % 2 == 0 else "US",
        "profile": {"identity": {"contact": copy.deepcopy(contact)}},
        "events": [
            {
                "kind": label,
                "participants": [{"role": "owner", "contact": copy.deepcopy(contact)}, None],
            }
        ],
        "directory": directory,
    }
    # Different container null/empty shapes, repeated throughout the fixture.
    if index % 8 == 0:
        result["directory"].append(("empty", None))
    if index % 8 == 3:
        result["events"] = None
    if index % 8 == 4:
        result["events"] = []
    if index % 8 == 5:
        result["directory"] = None
    if index % 8 == 6:
        result["directory"] = []
    if index % 8 == 7:
        result["profile"] = None
    return result


def expected_row(index, *, updated_id):
    value = row(index, updated=index == updated_id)
    profile = value["profile"]
    if profile is not None:
        redact(profile["identity"]["contact"])
    for event in value["events"] or []:
        for participant in event["participants"] or []:
            if participant is not None:
                redact(participant["contact"])
    if value["directory"] is not None:
        for _, item in value["directory"]:
            if item is not None:
                redact(item["contact"])
    value["private_directory"] = None
    return value


def selected(index, *, filtered=False):
    return not filtered or index % 2 == 1


def redact(contact):
    if contact is not None:
        if contact["email"] is not None:
            contact["email"] = "[email]"
        for phone in contact["phones"] or []:
            if phone is not None and phone["number"] is not None:
                phone["number"] = "[phone]"


def output_type(value):
    """Independent expected Flight shape: DuckDB emits nullable projection fields."""
    if pa.types.is_struct(value):
        return pa.struct(pa.field(field.name, output_type(field.type)) for field in value)
    if pa.types.is_list(value):
        return pa.list_(pa.field("element", output_type(value.value_type)))
    if pa.types.is_map(value):
        return pa.map_(value.key_type, output_type(value.item_type))
    return value


OUTPUT_SCHEMA = pa.schema(pa.field(field.name, output_type(field.type)) for field in SCHEMA)
