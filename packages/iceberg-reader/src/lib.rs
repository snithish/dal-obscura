//! Apache Iceberg owns protocol, partition/sequence matching, and all delete semantics.
//! Workers reconstruct native plans from pinned metadata; only assigned data files are read.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow_pyarrow::ToPyArrow;
use futures::{StreamExt, TryStreamExt};
use iceberg::{
    TableIdent,
    io::{FileIOBuilder, StorageFactory},
    scan::{ArrowRecordBatchStream, FileScanTask},
    spec::{NestedFieldRef, Schema, StructType, Type, prune_columns},
    table::StaticTable,
};
use iceberg_storage_opendal::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
use pyo3::{
    exceptions::{PyRuntimeError, PyTimeoutError, PyValueError},
    prelude::*,
};

const MAX_FILES: usize = 10_000;
fn error(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}
fn timeout(seconds: f64) -> PyResult<Duration> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(PyTimeoutError::new_err(
            "Iceberg operation deadline expired",
        ));
    }
    Duration::try_from_secs_f64(seconds).map_err(error)
}

// Upstream plans use snapshot schemas, which can omit a dropped equality key.
// Restore only those fields from metadata history; upstream still owns matching,
// type/default evolution, and delete execution. Synthetic names stay internal.
fn retain_delete_keys(table: &StaticTable, task: &mut FileScanTask) -> PyResult<()> {
    let mut missing: HashSet<_> = task
        .deletes
        .iter()
        .flat_map(|delete| delete.equality_ids.iter().flatten().copied())
        .filter(|id| task.schema.field_by_id(*id).is_none())
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let mut fields = task.schema.as_struct().fields().to_vec();
    let metadata = table.metadata();
    let mut schemas: Vec<_> = metadata.schemas_iter().collect();
    schemas.sort_by_key(|schema| std::cmp::Reverse(schema.schema_id()));
    for schema in schemas {
        let found: Vec<_> = missing
            .iter()
            .copied()
            .filter(|id| schema.field_by_id(*id).is_some())
            .collect();
        if found.is_empty() {
            continue;
        }
        if let Type::Struct(extra) =
            prune_columns(schema, found.iter().copied(), false).map_err(error)?
        {
            fields = merge_fields(&fields, extra.fields());
        }
        for id in found {
            missing.remove(&id);
        }
    }
    if !missing.is_empty() {
        return Err(PyValueError::new_err(
            "Equality-delete key missing from schema history",
        ));
    }
    task.schema = Arc::new(
        Schema::builder()
            .with_schema_id(task.schema.schema_id())
            .with_fields(fields)
            .build()
            .map_err(error)?,
    );
    task.project_field_ids = task
        .schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect();
    Ok(())
}

fn merge_fields(fields: &[NestedFieldRef], extra: &[NestedFieldRef]) -> Vec<NestedFieldRef> {
    let mut result = fields.to_vec();
    for field in extra {
        if let Some(index) = result.iter().position(|existing| existing.id == field.id) {
            if let (Type::Struct(current), Type::Struct(additional)) =
                (result[index].field_type.as_ref(), field.field_type.as_ref())
            {
                let mut parent = result[index].as_ref().clone();
                parent.field_type = Box::new(Type::Struct(StructType::new(merge_fields(
                    current.fields(),
                    additional.fields(),
                ))));
                result[index] = Arc::new(parent);
            }
        } else {
            let mut hidden = field.as_ref().clone();
            while result.iter().any(|existing| existing.name == hidden.name) {
                hidden.name = format!("__dal_delete_{}_{}", hidden.id, hidden.name);
            }
            result.push(Arc::new(hidden));
        }
    }
    result
}

#[pyclass]
struct Reader {
    runtime: Arc<tokio::runtime::Runtime>,
    table: StaticTable,
    tasks: Vec<FileScanTask>,
}

#[pymethods]
impl Reader {
    #[new]
    fn new(
        py: Python<'_>,
        location: String,
        snapshot: i64,
        properties: HashMap<String, String>,
        seconds: f64,
    ) -> PyResult<Self> {
        let duration = timeout(seconds)?;
        py.detach(|| {
            let runtime = Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .map_err(error)?,
            );
            let (table, tasks) = runtime.block_on(async {
                tokio::time::timeout(duration, async {
                    let factory: Arc<dyn StorageFactory> =
                        if location.starts_with('/') || location.starts_with("file:") {
                            Arc::new(OpenDalStorageFactory::Fs)
                        } else {
                            Arc::new(OpenDalResolvingStorageFactory::new())
                        };
                    let io = FileIOBuilder::new(factory).with_props(properties).build();
                    let table = StaticTable::from_metadata_file(
                        &location,
                        TableIdent::from_strs(["native", "scan"]).map_err(error)?,
                        io,
                    )
                    .await
                    .map_err(error)?;
                    let scan = table
                        .scan()
                        .snapshot_id(snapshot)
                        .select_all()
                        .with_concurrency_limit(1)
                        .build()
                        .map_err(error)?;
                    let mut stream = scan.plan_files().await.map_err(error)?;
                    let mut tasks = Vec::new();
                    while let Some(mut task) = stream.try_next().await.map_err(error)? {
                        if tasks.len() == MAX_FILES {
                            return Err(PyValueError::new_err(
                                "Iceberg snapshot exceeds the file budget",
                            ));
                        }
                        retain_delete_keys(&table, &mut task)?;
                        tasks.push(task);
                    }
                    Ok::<_, PyErr>((table, tasks))
                })
                .await
                .map_err(|_| PyTimeoutError::new_err("Iceberg planning deadline expired"))?
            })?;
            Ok(Self {
                runtime,
                table,
                tasks,
            })
        })
    }

    fn files(&self) -> Vec<(String, u64)> {
        self.tasks
            .iter()
            .map(|task| (task.data_file_path.clone(), task.file_size_in_bytes))
            .collect()
    }

    fn start(&self, fragments: Vec<(String, u64, u64)>) -> PyResult<Batches> {
        let assigned: HashSet<_> = fragments.iter().collect();
        if assigned.is_empty() || assigned.len() != fragments.len() {
            return Err(PyValueError::new_err("Invalid Iceberg task ranges"));
        }
        let parents: HashMap<_, _> = self
            .tasks
            .iter()
            .map(|task| (task.data_file_path.as_str(), task))
            .collect();
        let mut tasks = Vec::with_capacity(fragments.len());
        let mut intervals: HashMap<&str, Vec<(u64, u64)>> = HashMap::new();
        for (path, start, length) in &fragments {
            let parent = parents.get(path.as_str()).ok_or_else(|| {
                PyValueError::new_err("Iceberg task is outside its pinned snapshot")
            })?;
            let end = start
                .checked_add(*length)
                .filter(|end| *length > 0 && *end <= parent.file_size_in_bytes)
                .ok_or_else(|| PyValueError::new_err("Invalid Iceberg task range"))?;
            intervals.entry(path).or_default().push((*start, end));
            let mut task = (*parent).clone();
            task.start = *start;
            task.length = *length;
            tasks.push(task);
        }
        for ranges in intervals.values_mut() {
            ranges.sort_unstable();
            if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
                return Err(PyValueError::new_err("Overlapping Iceberg task ranges"));
            }
        }
        let reader = self
            .table
            .reader_builder()
            .with_batch_size(8192)
            .with_data_file_concurrency_limit(1)
            .build();
        let stream = reader
            .read(Box::pin(futures::stream::iter(tasks.into_iter().map(Ok))))
            .map_err(error)?
            .stream();
        Ok(Batches {
            runtime: self.runtime.clone(),
            stream: Mutex::new(Some(stream)),
        })
    }
}

#[pyclass]
struct Batches {
    runtime: Arc<tokio::runtime::Runtime>,
    stream: Mutex<Option<ArrowRecordBatchStream>>,
}

#[pymethods]
impl Batches {
    fn next<'py>(&self, py: Python<'py>, seconds: f64) -> PyResult<Option<Bound<'py, PyAny>>> {
        let duration = timeout(seconds)?;
        let batch = py.detach(|| {
            let mut guard = self.stream.lock().map_err(error)?;
            let Some(stream) = guard.as_mut() else {
                return Ok(None);
            };
            let result = self
                .runtime
                .block_on(async { tokio::time::timeout(duration, stream.next()).await });
            match result {
                Ok(Some(Ok(batch))) => Ok(Some(batch)),
                Ok(None) => {
                    *guard = None;
                    Ok(None)
                }
                Ok(Some(Err(e))) => {
                    *guard = None;
                    Err(error(e))
                }
                Err(_) => {
                    *guard = None;
                    Err(PyTimeoutError::new_err("Iceberg read deadline expired"))
                }
            }
        })?;
        batch.map(|batch| batch.to_pyarrow(py)).transpose()
    }

    fn close(&self) -> PyResult<()> {
        *self.stream.lock().map_err(error)? = None;
        Ok(())
    }
}

#[pymodule]
fn dal_obscura_iceberg_reader(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Reader>()?;
    module.add_class::<Batches>()
}
