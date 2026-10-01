from __future__ import annotations

import pytest

from examples.demo.keycloak.scripts import provision_demo


def test_provision_demo_preserves_nested_live_schema_identities():
    fields = provision_demo._flatten_schema_fields(
        [
            {
                "field_id": 10,
                "name": "customer",
                "path": {
                    "version": 1,
                    "segments": [{"kind": "field", "name": "customer", "field_id": 10}],
                },
                "type": "struct<email: string>",
                "nullable": False,
                "kind": "struct",
                "children": [
                    {
                        "field_id": 11,
                        "name": "email",
                        "path": {
                            "version": 1,
                            "segments": [
                                {"kind": "field", "name": "customer", "field_id": 10},
                                {"kind": "field", "name": "email", "field_id": 11},
                            ],
                        },
                        "type": "string",
                        "nullable": True,
                        "kind": "scalar",
                    }
                ],
            },
            {
                "field_id": 12,
                "name": "tags",
                "path": {
                    "version": 1,
                    "segments": [{"kind": "field", "name": "tags", "field_id": 12}],
                },
                "type": "list<string>",
                "nullable": True,
                "kind": "list",
                "children": [
                    {
                        "field_id": 13,
                        "name": "element",
                        "path": {
                            "version": 1,
                            "segments": [
                                {"kind": "field", "name": "tags", "field_id": 12},
                                {"kind": "list_element"},
                            ],
                        },
                        "type": "string",
                        "nullable": False,
                        "kind": "scalar",
                    }
                ],
            },
        ]
    )

    assert [(field["field_id"], field["path"]) for field in fields] == [
        ("10", ["customer"]),
        ("11", ["customer", "email"]),
        ("12", ["tags"]),
        ("13", ["tags", "$element"]),
    ]
    assert fields[0]["nullable"] is False
    assert fields[1]["nullable"] is True


def test_provision_demo_rejects_schema_without_provider_field_ids():
    with pytest.raises(RuntimeError, match="provider identity"):
        provision_demo._flatten_schema_fields(
            [
                {
                    "name": "customer_id",
                    "path": {"version": 1, "segments": [{"kind": "field", "name": "customer_id"}]},
                }
            ]
        )


def test_read_check_never_treats_service_failure_as_denial(monkeypatch):
    from dal_obscura.connectors import DalObscuraClient
    from examples.demo.keycloak.scripts.check_demo import verify_denied

    def unavailable(self, **kwargs):
        raise RuntimeError("backend unavailable")

    monkeypatch.setattr(DalObscuraClient, "read_table", unavailable)
    with (
        DalObscuraClient("grpc+tcp://127.0.0.1:1", auth_token="test") as client,
        pytest.raises(RuntimeError, match="backend unavailable"),
    ):
        verify_denied(client, "retail_demo", "retail.customer_revenue")


def test_no_grant_check_rejects_disclosed_values(monkeypatch):
    import pyarrow as pa

    from dal_obscura.connectors import DalObscuraClient
    from examples.demo.keycloak.scripts.check_demo import verify_denied

    monkeypatch.setattr(
        DalObscuraClient,
        "read_table",
        lambda self, **kwargs: pa.table(
            {
                "customer_id": [1, None, None, None],
                "email": [None] * 4,
            }
        ),
    )
    with (
        DalObscuraClient("grpc+tcp://127.0.0.1:1", auth_token="test") as client,
        pytest.raises(AssertionError, match="unexpectedly obtained data"),
    ):
        verify_denied(client, "retail_demo", "retail.customer_revenue")
