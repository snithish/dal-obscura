# Schema capture query cost

Schema discovery and synthetic evaluation need source identity and revision fields,
not the complete asset editor response. The storage query now selects those fields
with one catalog join. Schema discovery does not read admitted fields or policy
rules. Evaluation reads its policy rules once. Authorization and final revision
checks retain their existing boundaries.

## Reproduce the fixture

```bash
uv run --no-sync python scripts/schema_capture_performance.py --fields 4000 --iterations 5 --output /tmp/schema-capture.json
```

The script owns and deletes an isolated SQLite database. It admits 4,000 metadata
fields through the public API, uses an unchanged one-field provider schema, and
checks the exact returned type and scalar value. Provider IO is stubbed to isolate
HTTP, database and Python work. Each operation has two warmups and five samples.
No user demo, cloud provider or secret value is involved.

The preceding installed wheel at `3af7bba2` and the new implementation ran on the
same host with the same pinned Python 3.12.10, SQLAlchemy 2.0.48, PyArrow 23.0.1
and DuckDB 1.5.4 dependencies. Latency and Python allocation were measured together
with tracemalloc enabled. Timings include instrumentation and TestClient overhead;
they are not production latency or throughput claims. Tracemalloc excludes native
Arrow/DuckDB memory, so the figures are not process RSS measurements.

## Measured results

- Schema GET used 15 queries before and 5 after. Reads of admitted fields fell
  from 2 to 0, and policy queries fell from 4 to 0. Median instrumented time was
  192.625 to 3.788 ms. Median Python peak allocation was 8,209,392 to 88,304 bytes.
- Evaluation used 22 queries before and 7 after. Reads of admitted fields fell
  from 3 to 0, and policy queries fell from 7 to 1. Median instrumented time was
  315.048 to 8.852 ms. Median Python peak allocation was 8,234,221 to 93,758 bytes.

The result removes unnecessary authoring collection reads from these operations.
It does not establish performance for a 4,000-field source schema, native reader
planning, cloud storage, multiple concurrent users or a deployment capacity SLA.
The complete asset editor response still loads its owning authoring collections.
Incremental schema APIs and larger schema/resource qualification remain separate
work.
