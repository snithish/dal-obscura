//! Read-only transport bridge to the worker's official PyIceberg FileIO.
//! Native Iceberg still owns manifests, snapshots, ranges and delete semantics.
use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::{
    Error, ErrorKind, Result,
    io::{
        FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
        StorageFactory,
    },
};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

// Iceberg's traits require serde. These live, worker-local providers cannot be
// reconstructed from serialized data; a deserialized instance fails closed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonStorage {
    #[serde(skip)]
    pub provider: Option<Arc<Py<PyAny>>>,
}

fn failure(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Unexpected, error.to_string())
}

fn read_only<T>() -> Result<T> {
    Err(failure("Governed Iceberg storage is read-only"))
}

impl PythonStorage {
    fn provider(&self) -> Result<Arc<Py<PyAny>>> {
        self.provider
            .clone()
            .ok_or_else(|| failure("Worker FileIO is unavailable"))
    }
}

#[typetag::serde]
impl StorageFactory for PythonStorage {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        self.provider()?;
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for PythonStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let provider = self.provider()?;
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                provider
                    .bind(py)
                    .call_method1("exists", (path,))?
                    .extract::<bool>()
            })
        })
        .await
        .map_err(failure)?
        .map_err(failure)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let provider = self.provider()?;
        let path = path.to_owned();
        let size = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                provider
                    .bind(py)
                    .call_method1("size", (path,))?
                    .extract::<u64>()
            })
        })
        .await
        .map_err(failure)?
        .map_err(failure)?;
        Ok(FileMetadata { size })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let provider = self.provider()?;
        let path = path.to_owned();
        let bytes = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                provider
                    .bind(py)
                    .call_method1("read", (path,))?
                    .extract::<Vec<u8>>()
            })
        })
        .await
        .map_err(failure)?
        .map_err(failure)?;
        Ok(Bytes::from(bytes))
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        Ok(Box::new(PythonRangeReader {
            provider: self.provider()?,
            path: path.to_owned(),
        }))
    }

    async fn write(&self, _path: &str, _bytes: Bytes) -> Result<()> {
        read_only()
    }
    async fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
        read_only()
    }
    async fn delete(&self, _path: &str) -> Result<()> {
        read_only()
    }
    async fn delete_prefix(&self, _path: &str) -> Result<()> {
        read_only()
    }
    async fn delete_stream(&self, _paths: BoxStream<'static, String>) -> Result<()> {
        read_only()
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.provider()?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }

    fn new_output(&self, _path: &str) -> Result<OutputFile> {
        read_only()
    }
}

struct PythonRangeReader {
    provider: Arc<Py<PyAny>>,
    path: String,
}

#[async_trait]
impl FileRead for PythonRangeReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let provider = self.provider.clone();
        let path = self.path.clone();
        let expected = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| failure("Invalid range"))?;
        let bytes = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                provider
                    .bind(py)
                    .call_method1("read_range", (path, range.start, expected))?
                    .extract::<Vec<u8>>()
            })
        })
        .await
        .map_err(failure)?
        .map_err(failure)?;
        if bytes.len() as u64 != expected {
            return Err(failure("FileIO returned a truncated range"));
        }
        Ok(Bytes::from(bytes))
    }
}
