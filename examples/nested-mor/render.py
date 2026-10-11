"""Embed completed Compose evidence and exact source excerpts in the offline HTML."""

import argparse
import base64
import gzip
import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "docs/scenarios/nested-mor.html"
FLOW = [
    (
        "Flight request",
        "src/dal_obscura/interfaces/flight/server.py",
        "get_flight_info",
        "Transport decodes the table descriptor, columns and caller filter; delegates to "
        "ReadService.plan.",
        "Flight RPC",
    ),
    (
        "Verify principal",
        "src/dal_obscura/identity/oidc.py",
        "authenticate",
        "Production JWKS authentication verifies JWT signature, issuer, audience and expiry, then "
        "normalizes the principal. The issuer is synthetic in this example.",
        "JWKS HTTP, bounded cache",
    ),
    (
        "Capture configuration",
        "src/dal_obscura/sources/published.py",
        "open",
        "Capture one immutable asset/catalog/policy snapshot; release the database transaction "
        "before provider IO. Retain one opened format during schema admission and planning.",
        "PostgreSQL → provider IO",
    ),
    (
        "Resolve policy snapshot",
        "src/dal_obscura/storage/snapshots.py",
        "get_asset_and_catalog",
        "Feature-owned queries capture published configuration and exact schema identities. Delta "
        "admission uses scoped synthetic IDs; Iceberg uses provider field IDs.",
        "PostgreSQL transaction",
    ),
    (
        "Resolve Delta target",
        "packages/delta-plugin/src/dal_obscura_delta/catalog.py",
        "resolve_table",
        "A bounded 101-entry registry resolves retail.customers to its S3 path and table "
        "UUID/version. Construction validates names without opening each table; "
        "only the requested table is resolved.",
        "S3 registry and target log metadata",
        "delta",
    ),
    (
        "Resolve Iceberg target",
        "src/dal_obscura/sources/iceberg_catalog.py",
        "resolve_table",
        "The SQL catalog looks up the named table and captures its metadata location and snapshot. "
        "Administrative discovery lists 101 tables during setup; a target read resolves one table.",
        "PostgreSQL catalog + target S3 metadata",
        "iceberg",
    ),
    (
        "Admit schema",
        "src/dal_obscura/sources/published.py",
        "_validate_schema_admission",
        "Compare current schema identities and types with the published admission. Metadata-aware "
        "checks bind authorization to the same schema used for task planning.",
        "Captured schema only",
    ),
    (
        "Authorize masks and caller filters",
        "src/dal_obscura/read/service.py",
        "plan",
        "Resolve the analysts allow rule and seven masks. Combine policy/caller "
        "filters with AND and "
        "retain predicate dependencies even when hidden from the output.",
        "Captured policy; no database transaction across provider IO",
    ),
    (
        "Block masked-filter side channels",
        "src/dal_obscura/policy/admission.py",
        "_authorize_requested_row_filter",
        "Caller predicates overlapping masked paths are denied. US and partition "
        "probes use unmasked "
        "fields. Full residual evaluation remains in core even when a backend declares pushdown.",
        "No provider IO",
    ),
    (
        "Compile one output contract",
        "src/dal_obscura/policy/projection.py",
        "compile_projection",
        "One compiler produces SQL and emitted Arrow types together. Struct NULL "
        "guards, nested list "
        "transforms and whole-map hiding for null key masks preserve policy semantics.",
        "No provider IO",
    ),
    (
        "Translate the SDK scan request",
        "src/dal_obscura/sources/plugin_runtime.py",
        "_plan",
        "Send the exact execution schema and three-task budget to the bound format. SQL hints are "
        "sent only for declared filter_pushdown; neither Delta nor Iceberg declares it today.",
        "Format planning",
    ),
    (
        "Delta: split row groups and decode DVs",
        "packages/delta-plugin/src/dal_obscura_delta/format.py",
        "plan",
        "Delta Kernel supplies snapshot selection vectors. Arrow footer metadata identifies row "
        "groups; fully deleted groups are skipped. Group byte costs are greedily balanced across "
        "three passive tasks.",
        "Pinned Delta log, DV and Parquet footer reads",
        "delta",
    ),
    (
        "Delta: official object-store transport",
        "packages/delta-plugin/src/dal_obscura_delta/storage.py",
        "DeltaStorage",
        "delta-rs supplies the filesystem used for logs, data and deletion vectors. "
        "Worker options remain local; root containment is checked before native IO.",
        "Official delta-rs storage; assigned data paths",
        "delta",
    ),
    (
        "Delta Kernel selection vectors",
        "packages/delta-plugin/src/dal_obscura_delta/format.py",
        "_deletion_masks",
        "The maintained deletion_vectors API supplies boolean selection masks. The "
        "plugin validates "
        "counts and snapshot-wide physical-row bounds; it does not decode Roaring.",
        "Pinned DV files and Arrow selection buffers",
        "delta",
    ),
    (
        "Iceberg: split native snapshot work",
        "src/dal_obscura/sources/iceberg_plugin.py",
        "plan",
        "Validate admitted snapshot membership against Apache Iceberg's native plan, then assign "
        "disjoint file/range units under the task budget. Hidden equality keys remain available to "
        "native deletes.",
        "Target metadata/manifests, native delete/file planning",
        "iceberg",
    ),
    (
        "Encode passive tasks",
        "src/dal_obscura/sources/task_codec.py",
        "encode",
        "Serialize bounded JSON envelopes: immutable handle, admitted plugin identity/artifact, "
        "exact Arrow schema, path roots and passive task. No pickle, credentials, connections or "
        "callbacks.",
        "No provider IO",
    ),
    (
        "Issue all tickets atomically",
        "src/dal_obscura/storage/tickets.py",
        "store_many",
        "Only after every task/schema passes validation, store all captured payloads in one "
        "transaction. HMAC-signed references bind the records. Partial planning cannot leave a "
        "usable subset.",
        "PostgreSQL atomic insert",
    ),
    (
        "Fetch on independent workers",
        "src/dal_obscura/read/service.py",
        "fetch",
        "Verify signed reference, stored payload, principal/issuer/context, expiry and revocation. "
        "Restore the captured plugin/task and reserve a shared exchange before execution. Do not "
        "re-resolve latest catalog/policy.",
        "Shared PostgreSQL + worker-local credentials",
    ),
    (
        "Delta: read assigned work",
        "packages/delta-plugin/src/dal_obscura_delta/format.py",
        "_batches",
        "Arrow reads the assigned row group with requested columns and partition constants, no "
        "scanner readahead and one scanner thread. Apply the captured DV slice at the physical row "
        "offset before yielding bounded batches.",
        "Only assigned data files/row groups",
        "delta",
    ),
    (
        "Iceberg: native deletes then field-ID evolution",
        "src/dal_obscura/sources/iceberg_plugin.py",
        "_batches",
        "Apache Iceberg Rust reads only assigned file/range work, applying "
        "position/equality deletes "
        "and data sequence rules. PyIceberg evolves historical field IDs after "
        "deletes; then output "
        "fields are selected.",
        "Assigned data; shared metadata/delete reads",
        "iceberg",
    ),
    (
        "Native Iceberg snapshot planner",
        "packages/iceberg-reader/src/lib.rs",
        "new",
        "Apache Iceberg opens the pinned metadata/snapshot, plans files and retains historical "
        "equality-delete keys. Binding checks resource and file budgets.",
        "S3 target manifests and delete planning",
        "iceberg",
    ),
    (
        "Iceberg: official FileIO transport",
        "src/dal_obscura/sources/iceberg_plugin.py",
        "_FileIOBridge",
        "One worker-configured PyIceberg FileIO opens metadata, manifests, data and deletes. "
        "Every callback checks context/path admission and closes its provider stream.",
        "Official PyArrow/Fsspec backend; worker credentials",
        "iceberg",
    ),
    (
        "Native FileIO range adapter",
        "packages/iceberg-reader/src/storage.rs",
        "read",
        "The read-only Rust adapter invokes worker FileIO callbacks outside the async IO thread. "
        "Range bytes cross Python/Rust; Apache owns all snapshot and delete semantics.",
        "Assigned byte ranges; provider streams close after each range",
        "iceberg",
    ),
    (
        "Native assigned-range scan",
        "packages/iceberg-reader/src/lib.rs",
        "start",
        "Clone only admitted file/range tasks and pass them to the upstream Arrow reader. Apache "
        "Iceberg owns position/equality deletes and row-group range selection.",
        "Only assigned data ranges; shared delete files",
        "iceberg",
    ),
    (
        "Native lazy batch ownership",
        "packages/iceberg-reader/src/lib.rs",
        "next",
        "Release the Python GIL while awaiting a bounded native batch under the "
        "remaining execution "
        "deadline. Export Arrow buffers without Python row conversion.",
        "Native stream to Arrow C interface",
        "iceberg",
    ),
    (
        "Preserve collection validity",
        "src/dal_obscura/sources/iceberg_plugin.py",
        "_NullableProjectionVisitor",
        "Delegate schema evolution to the maintained visitor, restoring original null "
        "bitmaps after "
        "it reconstructs lists/maps of structs. This narrow Arrow bridge does not "
        "interpret Iceberg "
        "deletes or log metadata.",
        "Arrow buffers only",
        "iceberg",
    ),
    (
        "Validate plugin batches",
        "src/dal_obscura/sources/plugin_runtime.py",
        "_checked_plugin_batches",
        "Reject schema/metadata drift and logical or retained batches above the fixed "
        "16 MiB plugin "
        "cap. Activity checks bracket lazy provider work and cleanup preserves the primary error.",
        "Lazy Arrow iterator",
    ),
    (
        "Filter original values, then mask",
        "src/dal_obscura/read/transform.py",
        "apply_filters_and_masks_stream",
        "Each input batch feeds a transient DuckDB query. The complete WHERE predicate evaluates "
        "original values before SELECT masks. One connection per stream, one DuckDB thread; no "
        "full-result accumulation.",
        "In-memory DuckDB over Arrow; no external SQL IO",
    ),
    (
        "Normalize sliced nested Arrow views",
        "src/dal_obscura/read/transform.py",
        "_duckdb_input",
        "Rebase only nested columns containing nonzero offsets before DuckDB "
        "conversion. This avoids "
        "reproduced child-validity/offset misreads while sharing scalar/unsliced buffers.",
        "Bounded Arrow concatenation; no Python scalar conversion",
    ),
    (
        "Guard and close the stream",
        "src/dal_obscura/read/stream.py",
        "__next__",
        "Before emitted batches, verify identity/ticket expiry, deadline and revocation. Close "
        "resources on exhaustion, error, cancellation and early stop while retaining the primary "
        "error.",
        "Revocation checks + Flight output",
    ),
    (
        "Independent scenario verification",
        "examples/nested-mor/scripts/verify.py",
        "read_round",
        "Three concurrent Flight consumers verify exact schema, ID coverage and independent nested "
        "values. Instrumented S3 enforces file ownership and records ranges/bytes; "
        "full results are "
        "emitted only after every assertion.",
        "Flight + S3 evidence controller",
    ),
]


def source_entry(item):
    title, file, symbol, detail, io, *format_name = item
    path = ROOT / file
    lines = path.read_text().splitlines()
    pattern = re.compile(r"(?:def|class|fn) " + re.escape(symbol) + r"\b")
    match = next((index for index, line in enumerate(lines) if pattern.search(line)), None)
    if match is None:
        raise ValueError(f"Missing documented entry point: {file}:{symbol}")
    return {
        "title": title,
        "file": file,
        "symbol": symbol,
        "line": match + 1,
        "detail": detail,
        "io": io,
        "format": format_name[0] if format_name else None,
        "excerpt": "\n".join(
            f"{index + 1:4d}  {lines[index]}" for index in range(match, min(match + 28, len(lines)))
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=ROOT / "examples/nested-mor/results")
    parser.add_argument(
        "--allow-smoke", action="store_true", help="Explicitly label unqualified small probes"
    )
    args = parser.parse_args()
    bundle = {
        mode: json.loads((args.results / f"{mode}.json").read_text())
        for mode in ("delta", "iceberg")
    }
    for mode, evidence in bundle.items():
        if not args.allow_smoke:
            assert evidence["fixture"]["scale_requirement_met"], f"{mode}: scale evidence required"
            assert all(evidence["checks"].values()), f"{mode}: incomplete checks"
            assert min(evidence["pinned"]["partition_counts"]) >= 1_000_000
            assert "partition_read" in evidence and "projected" in evidence, (
                "Optimization probes required"
            )
    bundle["codeflow"] = [source_entry(item) for item in FLOW]
    files = sorted(
        {item[1] for item in FLOW}
        | {
            "examples/nested-mor/compose.yaml",
            "examples/nested-mor/Dockerfile",
            "examples/nested-mor/scripts/seed.py",
            "examples/nested-mor/scripts/services.py",
            "examples/nested-mor/scripts/fixture.py",
            "examples/nested-mor/scripts/provision.py",
            "tests/support/iceberg.py",
            "tests/support/delta.py",
            "packages/iceberg-reader/Cargo.toml",
            "packages/iceberg-reader/Cargo.lock",
            "pyproject.toml",
            "uv.lock",
        }
    )
    provenance = {
        "base_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "source_sha256": {
            file: hashlib.sha256((ROOT / file).read_bytes()).hexdigest() for file in files
        },
        "result_sha256": {
            mode: hashlib.sha256((args.results / f"{mode}.json").read_bytes()).hexdigest()
            for mode in ("delta", "iceberg")
        },
        "commands": ["examples/nested-mor/run delta", "examples/nested-mor/run iceberg"],
    }
    metadata = args.results / "build.json"
    if metadata.exists():
        provenance.update(json.loads(metadata.read_text()))
    bundle["provenance"] = provenance
    payload = json.dumps(bundle, separators=(",", ":")).encode()
    encoded = json.dumps(
        {
            "encoding": "gzip-base64",
            "data": base64.b64encode(gzip.compress(payload, mtime=0)).decode(),
        },
        separators=(",", ":"),
    )
    html = REPORT.read_text()
    html, count = re.subn(
        r'(<script type="application/json" id="evidence">).*?(</script>)',
        lambda match: match[1] + encoded + match[2],
        html,
        count=1,
        flags=re.S,
    )
    assert count == 1, "Missing HTML evidence marker"
    REPORT.write_text(html)
    REPORT.with_name("nested-mor-provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    print(f"Rendered {REPORT} ({REPORT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
