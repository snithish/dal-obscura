"""Apache Iceberg writers own delete encoding and snapshot construction."""

import json
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow.parquet as pq
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.update import (
    AddSnapshotUpdate,
    AssertRefSnapshotId,
    AssertTableUUID,
    SetSnapshotRefUpdate,
)


def write_deletes(
    table,
    *,
    kind,
    ids=(),
    delete_path=None,
    equality_columns=("id",),
    equality_records=None,
    partition=(),
):
    """Generate native MoR work, then publish through the public catalog API.

    PyIceberg lacks a public MoR writer. Apache Iceberg creates the delete
    file, manifests and snapshot; PyIceberg performs the catalog commit with
    UUID and parent-snapshot requirements.
    """
    if kind not in {"equality", "position"}:
        raise ValueError("Delete kind must be equality or position")
    if kind == "equality":
        records = equality_records if equality_records is not None else [{"id": i} for i in ids]
    else:
        records = []
        for task in table.scan().plan_files():
            with table.io.new_input(task.file.file_path).open() as stream:
                parquet = pq.ParquetFile(stream)
                offset = 0
                for batch in parquet.iter_batches(batch_size=8192, columns=["id"]):
                    records.extend(
                        {"file_path": task.file.file_path, "pos": offset + position}
                        for position, identifier in enumerate(batch.column(0).to_pylist())
                        if identifier in ids
                    )
                    offset += batch.num_rows
        records.sort(key=lambda row: (row["file_path"], row["pos"]))
    parent = table.current_snapshot()
    request = {
        "metadata_location": table.metadata_location,
        "kind": kind,
        "records": records,
        "equality_columns": equality_columns,
        "partition": partition,
        "delete_path": delete_path
        or f"{table.metadata.location}/metadata/delete-{uuid4().hex}.parquet",
    }
    writers = Path(__file__).with_name("writers")
    with TemporaryDirectory(prefix="dal-iceberg-writer-") as directory:
        temporary = Path(directory)
        classpath_file = temporary / "classpath.txt"
        subprocess.run(
            [
                "mvn",
                "-q",
                "-f",
                str(writers / "pom.xml"),
                "dependency:build-classpath",
                f"-Dmdep.outputFile={classpath_file}",
            ],
            check=True,
            capture_output=True,
            timeout=180,
        )
        classpath = classpath_file.read_text().strip()
        subprocess.run(
            ["javac", "-cp", classpath, "-d", directory, str(writers / "IcebergDeletes.java")],
            check=True,
            capture_output=True,
            timeout=60,
        )
        input_file, output_file = temporary / "request.json", temporary / "metadata.json"
        input_file.write_text(json.dumps(request))
        subprocess.run(
            [
                "java",
                "-cp",
                directory + ":" + classpath,
                "IcebergDeletes",
                str(input_file),
                str(output_file),
            ],
            check=True,
            capture_output=True,
            timeout=90,
        )
        metadata = json.loads(output_file.read_text())
    snapshot = Snapshot.model_validate(metadata["snapshots"][-1])
    table.catalog.commit_table(
        table,
        requirements=(
            AssertTableUUID(uuid=table.metadata.table_uuid),
            AssertRefSnapshotId(ref="main", snapshot_id=parent.snapshot_id if parent else None),
        ),
        updates=(
            AddSnapshotUpdate(snapshot=snapshot),
            SetSnapshotRefUpdate.model_validate(
                {"ref-name": "main", "type": "branch", "snapshot-id": snapshot.snapshot_id}
            ),
        ),
    )
    table.refresh()
    return table
