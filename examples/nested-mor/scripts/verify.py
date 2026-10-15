"""Full Flight verification with bounded client memory and separate timing samples."""

import hashlib
import json
import os
import platform
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from importlib.metadata import version
from itertools import pairwise
from pathlib import Path
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.flight as flight
from fixture import (
    BUCKET,
    COLUMNS,
    LATER_ID,
    OUTPUT_SCHEMA,
    PARTITIONS,
    ROWS,
    ROWS_PER_PARTITION,
    SMOKE,
    TARGET,
    expected_row,
    selected,
)
from seed import append_later

from dal_obscura.adapters.sql.database.db import create_engine_from_url, session_factory
from dal_obscura.adapters.sql.tickets import SqlAlchemyTicketStore
from dal_obscura.adapters.tickets.signing import HmacTicketCodecAdapter
from dal_obscura.connectors import DalObscuraClient


def http(url, value=None):
    request = Request(
        url,
        data=None if value is None else json.dumps(value).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def envelopes(info):
    engine = create_engine_from_url(os.environ["DAL_OBSCURA_DATABASE_URL"])
    try:
        store = SqlAlchemyTicketStore(session_factory(engine))
        codec = HmacTicketCodecAdapter(os.environ["DAL_OBSCURA_TICKET_SECRET"])
        return [
            json.loads(
                store.load(codec.verify(endpoint.ticket.ticket.decode()).ticket_id).payload.scan[
                    "read_payload"
                ]
            )
            for endpoint in info.endpoints
        ]
    finally:
        engine.dispose()


def assigned_paths(envelope):
    task = envelope["task"]
    files = (
        task["files"]
        if "files" in task
        else [f"s3://{BUCKET}/delta/customers/" + group["path"] for group in task["row_groups"]]
    )
    return sorted({"/" + urlsplit(path).netloc + unquote(urlsplit(path).path) for path in files})


def verify_work(tasks):
    units = []
    for envelope in tasks:
        task = envelope["task"]
        assert "scenario-only-secret" not in json.dumps(envelope)
        if "row_groups" in task:
            units += [(group["path"], group["row_group"]) for group in task["row_groups"]]
        else:
            units += [
                (path, tuple(span))
                for path, span in zip(task["files"], task["ranges"], strict=True)
            ]
    assert len(units) == len(set(units)), "Duplicated work units"
    if "files" in tasks[0]["task"]:
        spans = {}
        for path, (start, length) in units:
            spans.setdefault(path, []).append((start, start + length))
        for ranges in spans.values():
            ranges.sort()
            assert ranges[0][0] == 0
            assert all(left[1] == right[0] for left, right in pairwise(ranges))
    return units


def fetch(index, endpoint, schema, token, started, seen, lock, updated_id, check_values):
    begin = time.perf_counter()
    first = None
    details, samples = [], []
    counts = [0] * PARTITIONS
    scalar_ms = 0.0
    total = 0
    with flight.FlightClient(f"grpc+tcp://worker-{index + 1}:8815") as client:
        reader = client.do_get(
            endpoint.ticket,
            options=flight.FlightCallOptions(
                headers=[(b"authorization", ("Bearer " + token).encode())], timeout=3600
            ),
        )
        try:
            for chunk in reader:
                if chunk.data is None:
                    continue
                if first is None:
                    first = 1000 * (time.perf_counter() - started)
                batch = chunk.data
                assert batch.schema.equals(schema, check_metadata=True)
                ids = batch.column(0).to_pylist()
                with lock:
                    for identifier in ids:
                        assert 0 <= identifier <= LATER_ID and seen[identifier] == 0, (
                            "Duplicate/out-of-range id"
                        )
                        seen[identifier] = 1
                        counts[(identifier // ROWS_PER_PARTITION) % PARTITIONS] += 1
                checked = time.perf_counter()
                if check_values:
                    for value in batch.to_pylist():
                        assert value == expected_row(value["id"], updated_id=updated_id), (
                            f"Nested oracle mismatch for id={value['id']}"
                        )
                elif LATER_ID in ids and schema.names == COLUMNS:
                    position = ids.index(LATER_ID)
                    assert batch.slice(position, 1).to_pylist()[0] == expected_row(
                        LATER_ID, updated_id=updated_id
                    )
                scalar_ms += 1000 * (time.perf_counter() - checked)
                if not samples:
                    samples = batch.slice(0, 2).to_pylist()
                details.append(
                    {
                        "rows": batch.num_rows,
                        "logical_bytes": batch.nbytes,
                        "retained_bytes": batch.get_total_buffer_size(),
                    }
                )
                total += batch.num_rows
                if len(details) % 100 == 0:
                    print(f"worker-{index + 1}: {total:,} rows consumed", flush=True)
        finally:
            reader.cancel()
    return {
        "worker": f"worker-{index + 1}",
        "fetch_ms": 1000 * (time.perf_counter() - begin),
        "first_batch_ms": first,
        "scalar_validation_ms": scalar_ms,
        "rows": total,
        "partition_counts": counts,
        "batches": details,
        "sample": samples,
    }


def read_round(
    token,
    fixture,
    *,
    mutate=False,
    later=False,
    filtered=False,
    check_values=True,
    partition=None,
    columns=None,
):
    http("http://s3:5000/__evidence", {"phase": "read"})
    started = time.perf_counter()
    with DalObscuraClient("grpc+tcp://planner:8815", auth_token=token) as planner:
        info = planner.plan(
            catalog="scenario",
            target=TARGET,
            columns=COLUMNS if columns is None else columns,
            row_filter=f"partition = '{partition}'"
            if partition is not None
            else "region = 'US'"
            if filtered
            else None,
        )
    planned = time.perf_counter()
    assert len(info.endpoints) == 3
    expected_schema = (
        OUTPUT_SCHEMA
        if columns is None
        else pa.schema(
            [OUTPUT_SCHEMA.field(name) for name in columns], metadata=OUTPUT_SCHEMA.metadata
        )
    )
    assert info.schema.equals(expected_schema, check_metadata=True), "Unexpected output shape"
    tasks = envelopes(info)
    units = verify_work(tasks)
    plan_evidence = http("http://s3:5000/__evidence")
    if mutate:
        append_later(os.environ["SCENARIO"])
    http(
        "http://s3:5000/__evidence",
        {
            "phase": "fetch",
            "allowed": {
                f"worker-{index + 1}": assigned_paths(task) for index, task in enumerate(tasks)
            },
        },
    )
    seen, lock = bytearray(ROWS + 1), threading.Lock()
    fetch_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=3) as pool:
        workers = list(
            pool.map(
                lambda pair: fetch(
                    pair[0],
                    pair[1],
                    info.schema,
                    token,
                    fetch_started,
                    seen,
                    lock,
                    fixture["mor"]["replacement_id"],
                    check_values,
                ),
                enumerate(info.endpoints),
            )
        )
    finished = time.perf_counter()
    deleted_id = fixture["mor"]["deleted_id"]
    for identifier in range(ROWS + 1):
        wanted = (
            identifier != deleted_id
            and (identifier < ROWS or later)
            and selected(identifier, filtered=filtered)
            and (partition is None or (identifier // ROWS_PER_PARTITION) % PARTITIONS == partition)
        )
        assert seen[identifier] == int(wanted), f"Coverage mismatch for id={identifier}"
    counts = [
        sum(worker["partition_counts"][partition] for worker in workers)
        for partition in range(PARTITIONS)
    ]
    if not filtered and partition is None and not SMOKE:
        assert min(counts) >= 1000000, counts
    evidence = http("http://s3:5000/__evidence")
    events = plan_evidence["events"] + evidence["events"]
    assert not any(
        (
            "/other_" in event["path"]
            and event["method"] == "GET"
            and event["path"].endswith(".parquet")
        )
        or event["status"] == 403
        or event["status"] >= 500
        for event in events
    ), "Unexpected/unrelated S3 IO"
    actual_reads = {f"worker-{index + 1}": set() for index in range(3)}
    allowed = {f"worker-{index + 1}": set(assigned_paths(task)) for index, task in enumerate(tasks)}
    for event in evidence["events"]:
        if (
            event["method"] == "GET"
            and event["worker"] in allowed
            and event["path"] in allowed[event["worker"]]
        ):
            actual_reads[event["worker"]].add(event["path"])
    assert actual_reads == allowed
    plan_ms, fetch_ms = 1000 * (planned - started), 1000 * (finished - fetch_started)
    firsts = [
        worker["first_batch_ms"] for worker in workers if worker["first_batch_ms"] is not None
    ]
    result = {
        "requested_columns": COLUMNS if columns is None else columns,
        "partition_filter": partition,
        "plan_ms": plan_ms,
        "parallel_fetch_ms": fetch_ms,
        "read_total_ms": plan_ms + fetch_ms,
        "first_batch_ms": plan_ms + min(firsts),
        "exact_scalar_validation": check_values,
        "rows": seen.count(1),
        "partition_counts": counts,
        "coverage_sha256": hashlib.sha256(seen).hexdigest(),
        "task_count": len(tasks),
        "unit_count": len(units),
        "tasks": tasks,
        "workers": workers,
        "s3_plan_requests": len(plan_evidence["events"]),
        "s3_fetch_requests": len(evidence["events"]),
        "s3_events": events,
    }
    print(
        f"Read complete: {result['rows']:,} rows, {result['read_total_ms'] / 1000:.3f}s, "
        f"scalar oracle={check_values}",
        flush=True,
    )
    return result


def main():
    fixture = json.loads(Path("/state/fixture.json").read_text())
    token = http("http://issuer:8080/token")["access_token"]
    rounds = int(os.environ.get("SCENARIO_ROUNDS", "1"))
    if not 1 <= rounds <= 20:
        raise ValueError("SCENARIO_ROUNDS must be between 1 and 20")
    pinned = read_round(token, fixture, mutate=True)
    checkpoint = Path("/results") / (os.environ["SCENARIO"] + ".partial.json")
    checkpoint.write_text(json.dumps({"status": "incomplete", "pinned": pinned}, indent=2))
    measured = [
        read_round(
            http("http://issuer:8080/token")["access_token"],
            fixture,
            later=True,
            check_values=False,
        )
        for _ in range(rounds)
    ]
    filtered = read_round(
        http("http://issuer:8080/token")["access_token"], fixture, later=True, filtered=True
    )
    partition_read = read_round(
        http("http://issuer:8080/token")["access_token"],
        fixture,
        later=True,
        partition=3,
        check_values=False,
    )
    projected = read_round(
        http("http://issuer:8080/token")["access_token"],
        fixture,
        later=True,
        columns=["id"],
        check_values=False,
    )
    with DalObscuraClient(
        "grpc+tcp://planner:8815", auth_token=http("http://issuer:8080/token")["access_token"]
    ) as planner:
        try:
            planner.plan(
                catalog="scenario",
                target=TARGET,
                columns=COLUMNS,
                row_filter="profile.identity.contact.email = 'original-1@pii.test'",
            )
        except flight.FlightUnauthorizedError:
            pass
        else:
            raise AssertionError("Caller filter on masked PII must be denied")
    result = {
        "scenario": os.environ["SCENARIO"],
        "verified_at": datetime.now(UTC).isoformat(),
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "versions": {
                name: version(name)
                for name in [
                    "dal-obscura",
                    "dal-obscura-delta",
                    "dal-obscura-iceberg-reader",
                    "duckdb",
                    "pyarrow",
                    "pyiceberg",
                    "deltalake",
                    "moto",
                ]
            },
        },
        "fixture": fixture,
        "governance": json.loads(Path("/state/governance.json").read_text()),
        "config": {
            "partitions": PARTITIONS,
            "rows_per_partition": ROWS_PER_PARTITION,
            "rows": ROWS,
            "rounds": rounds,
            "tickets": 3,
            "workers": 3,
            "row_group_target_rows": 16384,
            "native_batch_rows": 8192,
            "transform_batch_rows": 8192,
            "duckdb_threads": 1,
            "duckdb_memory_per_stream": "256MB",
            "active_streams_per_worker": 2,
            "input_output_batch_byte_limit": 67108864,
            "plugin_batch_byte_limit": 16777216,
            "ticket_ttl_seconds": 3600,
            "ticket_exchanges": 1,
            "stream_deadline_seconds": 3600,
        },
        "checks": {
            "scale_requirement_met": not SMOKE,
            "exact_nested_values": True,
            "declared_and_emitted_schema_metadata": True,
            "original_deleted_id_absent": True,
            "updated_id_once": True,
            "pinned_snapshot_excludes_later_append": True,
            "new_snapshot_includes_later_append": True,
            "null_empty_containers_preserved": True,
            "pii_map_keys_hide_entire_map": True,
            "unrelated_table_data_reads_absent": True,
            "worker_io_restricted": True,
            "unique_disjoint_work_units": True,
            "authorized_caller_filter_exact_coverage": True,
            "masked_caller_filter_denied": True,
        },
        "pinned": pinned,
        "rounds": measured,
        "filtered": filtered,
        "partition_read": partition_read,
        "projected": projected,
    }
    output = Path("/results") / (os.environ["SCENARIO"] + ".json")
    output.write_text(json.dumps(result, indent=2))
    checkpoint.unlink()
    print(
        json.dumps(
            {
                "scenario": result["scenario"],
                "checks": result["checks"],
                "read_total_ms": [item["read_total_ms"] for item in measured],
                "output": str(output),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
