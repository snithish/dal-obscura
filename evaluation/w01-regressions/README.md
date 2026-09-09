# W01 regression evidence

Recorded 2026-09-09. This packet preserves observed behavior without adding a
permanent xfail or weakening the supported suite.

## Fixed evidence

The following focused tests were written against the previous behavior and
failed semantically before their respective commits:

- `test_nested_child_projection_cannot_bypass_null_mask_on_parent`: returned a
  raw SSN for `profile.ssn` despite a `profile: null` mask.
- `test_compiler_rejects_invalid_mask_instead_of_dropping_it`: empty, unknown,
  and negative `keep_last` mask definitions were silently accepted.
- `test_fetch_stream_rejects_ticket_when_granting_group_is_removed`: a ticket
  remained exchangeable after removing the granting group.
- `test_published_catalog_registry_binds_the_published_asset_table`: a logical
  alias resolved catalog defaults instead of its published table binding.

Each now passes through the applicable unit/integration path. Their commits
are listed in `docs/gateway-v1/STATUS.md`.

## Open evidence: SDK materialization

`DalObscuraClient.read_batches` currently executes
`reader.read_all().to_batches()` in
`src/dal_obscura/connectors/python_sdk.py`. This violates W01 item 5 and W08:
the first authorized batch cannot be yielded while later data is unavailable.
The reproduction is a fake Flight reader whose `read_all()` raises and whose
first `read_chunk()` returns a batch. Calling `read_batches()` raises from
`read_all()` before yielding the batch.

This is intentionally not committed as an xfail: the master plan forbids
xfails used to make the suite green. It becomes a normal passing regression in
W08 after W07 produces the versioned pinned scan contract.

## Order and next step

W02 is next. It replaces permissive/coercing config and policy paths with
strict immutable models, typed field paths, validated asset bindings and a
canonical identity context. No SDK streaming change is made before W07/W08.
