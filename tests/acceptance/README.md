# Ticket contract fixtures

`test_ticket_encoding.py` verifies canonical encoding and round trips for the
current stored ticket payload in `fixtures/ticket_payload_v2.json`. The fixture
describes the internal stored payload, not the opaque signed transport ticket.
Update the fixture with the model when the supported contract changes.

```bash
uv run pytest tests/acceptance/test_ticket_encoding.py -q
```

Policy, transport, plugin, browser, and consumer behaviors belong to their owning
suites. See [development](../../docs/development.md) and
[read execution invariants](../../docs/read-execution-invariants.md).
