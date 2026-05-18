use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use moka::future::Cache;

use crate::{
    block::{BlockMeta, CodecId},
    codec::decompress_by_id,
    error::{Error, Result},
    storage::Storage,
};

const IO_SLAB_SIZE: u64 = 1024 * 1024; // 1 MiB

/// Raw I/O cache: caches compressed file bytes in 1 MiB aligned slabs.
///
/// Key: `(delta_path, slab_index)`. Concurrent requests for the same slab are
/// coalesced — only one task performs the actual storage read.
#[derive(Clone)]
struct IoCache {
    inner: Cache<(Arc<str>, u64), Bytes>,
    /// File sizes cached per path so `storage.size()` is only called once per delta.
    file_sizes: Cache<Arc<str>, u64>,
}

impl IoCache {
    fn new(max_capacity_bytes: u64) -> Self {
        let inner = Cache::builder()
            .weigher(|_: &(Arc<str>, u64), v: &Bytes| v.len().try_into().unwrap_or(u32::MAX))
            .max_capacity(max_capacity_bytes)
            .build();
        let file_sizes = Cache::builder().max_capacity(1024).build();
        Self { inner, file_sizes }
    }

    /// Reads `range` bytes, routing through aligned 1 MiB slab loads.
    async fn read_range(
        &self,
        path: &Arc<str>,
        range: Range<u64>,
        storage: &(dyn Storage + Sync),
    ) -> Result<Bytes> {
        let start_slab = range.start / IO_SLAB_SIZE;
        let end_slab = (range.end.saturating_sub(1)) / IO_SLAB_SIZE;

        if start_slab == end_slab {
            let slab = self.load_slab(path, start_slab, storage).await?;
            let offset = (range.start - start_slab * IO_SLAB_SIZE) as usize;
            let len = (range.end - range.start) as usize;
            Ok(slab.slice(offset..offset + len))
        } else {
            let mut out = Vec::with_capacity((range.end - range.start) as usize);
            for slab_idx in start_slab..=end_slab {
                let slab = self.load_slab(path, slab_idx, storage).await?;
                let slab_start = slab_idx * IO_SLAB_SIZE;
                let from = (range.start.max(slab_start) - slab_start) as usize;
                let to = (range.end.min(slab_start + IO_SLAB_SIZE) - slab_start) as usize;
                out.extend_from_slice(&slab[from..to.min(slab.len())]);
            }
            Ok(Bytes::from(out))
        }
    }

    async fn load_slab(
        &self,
        path: &Arc<str>,
        slab_idx: u64,
        storage: &(dyn Storage + Sync),
    ) -> Result<Bytes> {
        let file_size = self
            .file_sizes
            .try_get_with(Arc::clone(path), async move { storage.size().await })
            .await
            .map_err(|e| Error::Storage(format!("size fetch failed: {e}")))?;

        let slab_start = slab_idx * IO_SLAB_SIZE;
        let slab_end = (slab_start + IO_SLAB_SIZE).min(file_size);
        let key = (Arc::clone(path), slab_idx);
        self.inner
            .try_get_with(key, async move {
                storage.read_range(slab_start..slab_end).await
            })
            .await
            .map_err(|e| Error::Storage(format!("io slab load failed: {e}")))
    }
}

/// Two-level cache shared across all delta layers in an [`ArrayFile`](crate::file::ArrayFile).
///
/// Level 1 — block cache: decompressed block bytes, keyed by `(path, block_id)`.
/// Level 2 — I/O slab cache: raw compressed bytes in 1 MiB slabs, keyed by `(path, slab_index)`.
///
/// On a block cache miss, raw bytes are fetched via the I/O slab cache (if enabled),
/// then decompressed and stored in the block cache.
#[derive(Clone)]
pub struct DeltaCache {
    block_cache: Cache<(Arc<str>, u32), Bytes>,
    io_cache: Option<IoCache>,
}

impl DeltaCache {
    /// Creates a new cache.
    ///
    /// `block_capacity` is the byte budget for decompressed blocks.
    /// `io_capacity` is the byte budget for raw I/O slabs (0 disables the I/O tier).
    pub fn new(block_capacity: u64, io_capacity: u64) -> Self {
        let block_cache = Cache::builder()
            .weigher(|_: &(Arc<str>, u32), v: &Bytes| v.len().try_into().unwrap_or(u32::MAX))
            .max_capacity(block_capacity)
            .build();
        let io_cache = if io_capacity > 0 {
            Some(IoCache::new(io_capacity))
        } else {
            None
        };
        Self { block_cache, io_cache }
    }

    /// Returns the decompressed block, loading from `storage` on a cache miss.
    ///
    /// Concurrent requests for the same `(path, block_id)` are coalesced.
    pub async fn get_or_load(
        &self,
        path: &Arc<str>,
        block_meta: &BlockMeta,
        storage: &(dyn Storage + Sync),
    ) -> Result<Bytes> {
        let key = (Arc::clone(path), block_meta.id.0);
        let range = block_meta.file_range();
        let uncompressed_size = block_meta.uncompressed_size as usize;
        let codec_id = block_meta.codec.clone();
        let io_cache = self.io_cache.clone();
        let path_for_io = Arc::clone(path);

        self.block_cache
            .try_get_with(key, async move {
                // Only use the I/O slab cache when the block fits in a single slab.
                // Multi-slab blocks (compressed_size > IO_SLAB_SIZE) are read directly:
                // splitting them across N slab loads adds overhead with no coalescing benefit.
                let same_slab = range.start / IO_SLAB_SIZE
                    == (range.end.saturating_sub(1)) / IO_SLAB_SIZE;
                let raw = match &io_cache {
                    Some(io) if same_slab => {
                        io.read_range(&path_for_io, range, storage).await?
                    }
                    _ => storage.read_range(range).await?,
                };
                let decompressed = if codec_id == CodecId::None {
                    raw
                } else {
                    Bytes::from(decompress_by_id(&codec_id, &raw, uncompressed_size)?)
                };
                Ok::<Bytes, Error>(decompressed)
            })
            .await
            .map_err(|e| Error::Storage(format!("cache load failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::BlockId;
    use crate::block::{BlockMeta, CodecId};
    use crate::storage::InMemoryStorage;

    fn make_block_meta(id: u32, offset: u64, size: u64) -> BlockMeta {
        BlockMeta {
            id: BlockId(id),
            file_offset: offset,
            compressed_size: size,
            uncompressed_size: size,
            codec: CodecId::None,
        }
    }

    // ── Block cache tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn block_cache_miss_loads_block() {
        let data = vec![0xAA; 100];
        let storage = InMemoryStorage::from_bytes(data.clone());
        let cache = DeltaCache::new(1024, 0);
        let meta = make_block_meta(0, 0, 100);

        let result = cache.get_or_load(&Arc::from("p"), &meta, &storage).await.unwrap();
        assert_eq!(&result[..], &data[..]);
    }

    #[tokio::test]
    async fn block_cache_hit_returns_same_bytes() {
        let data = vec![0xBB; 200];
        let storage = InMemoryStorage::from_bytes(data.clone());
        let cache = DeltaCache::new(4096, 0);
        let meta = make_block_meta(0, 0, 200);

        let first = cache.get_or_load(&Arc::from("p"), &meta, &storage).await.unwrap();
        let second = cache.get_or_load(&Arc::from("p"), &meta, &storage).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(&first[..], &data[..]);
    }

    // ── IoCache tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn io_cache_read_within_single_slab() {
        let mut data = vec![0u8; 2 * 1024 * 1024]; // 2 MiB
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let storage = InMemoryStorage::from_bytes(data.clone());
        let io = IoCache::new(4 * 1024 * 1024);

        let range = 100u64..500u64;
        let result = io.read_range(&Arc::from("p"), range.clone(), &storage).await.unwrap();
        assert_eq!(&result[..], &data[100..500]);
    }

    #[tokio::test]
    async fn io_cache_read_spanning_two_slabs() {
        let size = 2 * 1024 * 1024usize;
        let mut data = vec![0u8; size];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let storage = InMemoryStorage::from_bytes(data.clone());
        let io = IoCache::new(4 * 1024 * 1024);

        // Range that straddles the 1 MiB boundary
        let start = 1024 * 1024 - 100;
        let end = 1024 * 1024 + 100;
        let result = io
            .read_range(&Arc::from("p"), start as u64..end as u64, &storage)
            .await
            .unwrap();
        assert_eq!(&result[..], &data[start..end]);
    }

    #[tokio::test]
    async fn io_cache_handles_partial_last_slab() {
        // File smaller than 1 MiB
        let data = vec![0xCC; 500_000];
        let storage = InMemoryStorage::from_bytes(data.clone());
        let io = IoCache::new(2 * 1024 * 1024);

        let result = io
            .read_range(&Arc::from("p"), 0..500_000, &storage)
            .await
            .unwrap();
        assert_eq!(&result[..], &data[..]);
    }

    #[tokio::test]
    async fn block_cache_with_io_tier_returns_correct_bytes() {
        let data = vec![0xDE; 300];
        let storage = InMemoryStorage::from_bytes(data.clone());
        let cache = DeltaCache::new(4096, 2 * 1024 * 1024);
        let meta = make_block_meta(0, 0, 300);

        let result = cache.get_or_load(&Arc::from("p"), &meta, &storage).await.unwrap();
        assert_eq!(&result[..], &data[..]);
    }
}
