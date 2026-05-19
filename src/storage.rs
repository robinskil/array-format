//! Storage backend trait and implementations.
//!
//! The [`Storage`] trait abstracts file I/O so the format can work with
//! local files, object stores, or in-memory buffers. An
//! [`ObjectStoreBackend`] adapter is provided for the `object_store` crate,
//! and [`InMemoryStorage`] is provided for testing.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::sync::RwLock;

use crate::error::{Error, Result};

/// Streaming writer used to build up a file in chunks.
///
/// Obtained from [`Storage::write_multipart`]. Callers push successive
/// chunks via [`write_chunk`] and finalize with [`complete`]; dropping
/// the writer without calling `complete` aborts the upload.
pub trait MultipartWriter: Send {
    /// Appends `data` to the in-flight upload.
    fn write_chunk(&mut self, data: Bytes) -> BoxFuture<'_, Result<()>>;

    /// Finalizes the upload, committing all previously written chunks.
    fn complete(self: Box<Self>) -> BoxFuture<'static, Result<()>>;
}

/// Async storage backend for reading and writing file data.
///
/// All methods return [`BoxFuture`] so the trait is object-safe
/// (`dyn Storage` is valid).
///
/// # Extensibility
///
/// Implement this trait to plug in custom storage backends (e.g. a
/// distributed file system, an HTTP range-request backend, etc.).
pub trait Storage: Send + Sync {
    /// Reads the byte range `range` from the file.
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>>;

    /// Replaces the entire file content with `data`.
    fn write(&self, data: Bytes) -> BoxFuture<'_, Result<()>>;

    /// Returns the total size of the file in bytes.
    fn size(&self) -> BoxFuture<'_, Result<u64>>;

    /// Begins a streaming write that replaces the file's contents.
    ///
    /// The returned writer accepts successive byte chunks; the upload is
    /// committed once [`MultipartWriter::complete`] is awaited. This lets
    /// callers ship large payloads without materializing the whole file in
    /// memory.
    fn write_multipart(&self) -> BoxFuture<'_, Result<Box<dyn MultipartWriter>>>;
}

/// An in-memory storage backend for testing.
///
/// Wraps a `Vec<u8>` behind an `Arc<RwLock<..>>` so it can be shared
/// across async tasks.
#[derive(Debug, Clone)]
pub struct InMemoryStorage {
    data: Arc<RwLock<Vec<u8>>>,
}

impl InMemoryStorage {
    /// Creates a new empty in-memory store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Creates an in-memory store pre-loaded with `data`.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Self {
            data: Arc::new(RwLock::new(data)),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for InMemoryStorage {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            let data = self.data.read().await;
            let start = range.start as usize;
            let end = range.end as usize;
            if end > data.len() {
                return Err(Error::Storage(format!(
                    "read range {}..{} exceeds file size {}",
                    start,
                    end,
                    data.len()
                )));
            }
            Ok(Bytes::copy_from_slice(&data[start..end]))
        })
    }

    fn write(&self, bytes: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut data = self.data.write().await;
            *data = bytes.to_vec();
            Ok(())
        })
    }

    fn size(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move {
            let data = self.data.read().await;
            Ok(data.len() as u64)
        })
    }

    fn write_multipart(&self) -> BoxFuture<'_, Result<Box<dyn MultipartWriter>>> {
        Box::pin(async move {
            Ok(Box::new(InMemoryMultipart {
                data: Arc::clone(&self.data),
                buf: Vec::new(),
            }) as Box<dyn MultipartWriter>)
        })
    }
}

struct InMemoryMultipart {
    data: Arc<RwLock<Vec<u8>>>,
    buf: Vec<u8>,
}

impl MultipartWriter for InMemoryMultipart {
    fn write_chunk(&mut self, data: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.buf.extend_from_slice(&data);
            Ok(())
        })
    }

    fn complete(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let mut guard = self.data.write().await;
            *guard = self.buf;
            Ok(())
        })
    }
}

/// A storage backend backed by an [`object_store::ObjectStore`] implementation.
///
/// Wraps any `ObjectStore` (local filesystem, S3, GCS, Azure, in-memory)
/// and a [`Path`](object_store::path::Path) pointing to the target file.
#[derive(Clone)]
pub struct ObjectStoreBackend {
    store: Arc<dyn object_store::ObjectStore>,
    path: object_store::path::Path,
}

impl ObjectStoreBackend {
    /// Creates a new backend targeting `path` within the given `store`.
    pub fn new(store: Arc<dyn object_store::ObjectStore>, path: object_store::path::Path) -> Self {
        Self { store, path }
    }
}

impl Storage for ObjectStoreBackend {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            use object_store::ObjectStoreExt;
            let bytes = self
                .store
                .get_range(&self.path, range)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            Ok(bytes)
        })
    }

    fn write(&self, data: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            use object_store::ObjectStoreExt;
            self.store
                .put(&self.path, data.into())
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            Ok(())
        })
    }

    fn size(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move {
            use object_store::ObjectStoreExt;
            let meta = self
                .store
                .head(&self.path)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            Ok(meta.size as u64)
        })
    }

    fn write_multipart(&self) -> BoxFuture<'_, Result<Box<dyn MultipartWriter>>> {
        Box::pin(async move {
            use object_store::buffered::BufWriter;
            let writer = BufWriter::with_capacity(
                Arc::clone(&self.store),
                self.path.clone(),
                8 * 1024 * 1024,
            );
            Ok(Box::new(ObjectStoreMultipart { writer }) as Box<dyn MultipartWriter>)
        })
    }
}

struct ObjectStoreMultipart {
    writer: object_store::buffered::BufWriter,
}

impl MultipartWriter for ObjectStoreMultipart {
    fn write_chunk(&mut self, data: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            use tokio::io::AsyncWriteExt;
            self.writer
                .write_all(&data)
                .await
                .map_err(|e| Error::Storage(e.to_string()))
        })
    }

    fn complete(mut self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            use tokio::io::AsyncWriteExt;
            self.writer
                .shutdown()
                .await
                .map_err(|e| Error::Storage(e.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_write_read() {
        let storage = InMemoryStorage::new();
        let payload = Bytes::from_static(b"hello world");
        storage.write(payload.clone()).await.unwrap();

        let size = storage.size().await.unwrap();
        assert_eq!(size, 11);

        let read = storage.read_range(0..5).await.unwrap();
        assert_eq!(&read[..], b"hello");
    }

    #[tokio::test]
    async fn in_memory_out_of_range() {
        let storage = InMemoryStorage::from_bytes(vec![1, 2, 3]);
        let result = storage.read_range(0..10).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn in_memory_overwrite() {
        let storage = InMemoryStorage::new();
        storage.write(Bytes::from_static(b"first")).await.unwrap();
        storage.write(Bytes::from_static(b"second")).await.unwrap();
        let size = storage.size().await.unwrap();
        assert_eq!(size, 6);
        let data = storage.read_range(0..6).await.unwrap();
        assert_eq!(&data[..], b"second");
    }

    #[tokio::test]
    async fn in_memory_multipart_streams_chunks() {
        let storage = InMemoryStorage::from_bytes(b"stale".to_vec());
        let mut writer = storage.write_multipart().await.unwrap();
        writer.write_chunk(Bytes::from_static(b"hello "))
            .await
            .unwrap();
        writer.write_chunk(Bytes::from_static(b"streaming "))
            .await
            .unwrap();
        writer.write_chunk(Bytes::from_static(b"world"))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        assert_eq!(storage.size().await.unwrap(), 21);
        let data = storage.read_range(0..21).await.unwrap();
        assert_eq!(&data[..], b"hello streaming world");
    }
}
