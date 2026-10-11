# Native schema boundaries

Execution schemas must cross the pinned Arrow C Data interchange without losing
field identity, types, or metadata. Read schema/plan, admitted plugin planning,
and signed scan-envelope encoding/decoding enforce this before provider planning
or ticket issuance. Generic metadata bounds remain separate: a deep unselected
source field does not prevent browsing metadata or planning an independent scalar
projection. Planning still retains complete nested types for selected top-level
fields under the SDK contract.

The guard roundtrips the schema through PyArrow's owned C schema capsule and
compares metadata explicitly. It does not convert batches or copy their buffers.
PyArrow currently provides no public validation-only native interchange API;
`Schema._import_from_c_capsule` is the narrow in-process bridge used here. Capsule
ownership follows the [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
The [C Data specification](https://arrow.apache.org/docs/format/CDataInterface.html)
uses NUL-terminated field names. An embedded NUL survives IPC but truncates during
C interchange; such selected schemas now fail before execution rather than
silently changing their identity. Names and source metadata are never rewritten.

## Reproduce the depth probe

```bash
uv run --no-sync python scripts/probe_native_schema_depth.py --output /tmp/native-depth.json
```

Each depth runs in a separate child process with a 30-second deadline. The probe
checks exact schema/batch metadata through IPC and Parquet and exact typed values
through DuckDB, including SQL NULL. DuckDB drops metadata; the production projection
compiler owns its restoration. The probe does not qualify catalogs, table-format
deletes, Flight consumers, cloud credentials, or process RSS.

On this host with PyArrow 23.0.1 and DuckDB 1.5.4:

- 32 and 62 struct layers pass schema IPC, batch IPC, Parquet and both leaf/struct
  DuckDB queries. Separate boundary probes at 60 and 61 also pass.
- 63 layers preserve IPC and Parquet values, but DuckDB and direct C Data import
  fail with `Recursion level in ArrowSchema struct exceeded`.
- 64, 65 and 96 layers preserve schema IPC and Parquet. Batch IPC fails with
  `Max recursion depth reached`; DuckDB fails at interchange.
- 128 and 256 layers also fail schema IPC and Parquet with
  `Invalid flatbuffers message.` These are diagnostic dependency results, not
  service-admitted schemas.

Struct-layer counts differ from the service's field-depth count because the root
field counts toward its 64-level metadata budget. No global depth constant was
lowered or lifted. Lists/maps and extension types need their own qualification;
this fixture does not establish a universal 62-layer capability.

A same-host 4,000-string-field schema comparison, after three warmups and 30 samples,
measured median generic bound validation at 3.534 ms and native validation at
4.972 ms. This is the cost of rejecting unusable or lossy schemas before native
execution, not a throughput or RSS claim. Native validation includes the generic
bounds; distinct admission, planning and signed-ticket boundaries retain their
own checks.

## Qualification and next gate

The rebuilt service wheel passed 324 installed, source-disabled schema, read,
plugin, control API and real consumer cases without failures or skips against a
disposable PostgreSQL database. Positive tests preserve bounded deep source
metadata and usable scalar projections. Negative tests reject unusable selected
outputs before provider planning or ticket signing and reject invalid scan
envelopes on both encode and decode. Six unchanged installed plugin artifacts
were locked and admitted separately.

Arbitrary native nesting remains unresolved. Arrow C++ exposes IPC recursion
options, but the pinned Python read/write APIs do not expose those options, and
changing IPC alone would leave C Data and consumers failing. Lift this gate only
with maintained dependency support or a separately reviewed native bridge,
coordinated worker/client wheels, exact nested-value and metadata checks, and
real distributed CoW/MoR qualification. A constants-only change is insufficient.
