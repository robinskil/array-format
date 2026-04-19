//! Block cache backed by [`moka`].
//!
//! The [`BlockCache`] wraps a `moka::future::Cache` keyed by block id.
//! It provides coalesced loading: if multiple async tasks request the
//! same block concurrently, only one will perform the actual I/O and
//! decompression. All others wait and receive the cached result.

use bytes::Bytes;
use moka::future::Cache;

use crate::block::{BlockMeta, CodecId};
use crate::codec::decompress_by_id;
use crate::error::{Error, Result};
use crate::storage::Storage;

/// A byte-weighted block cache shared across readers.
///
/// Each [`Reader`](crate::reader::Reader) owns one `BlockCache` instance.
/// Many threads/tasks may read through the same cache concurrently.
///
/// On a cache miss the full compressed block is read from storage,
/// decompressed, and inserted. Subsequent hits return the decompressed
/// bytes directly from memory.
#[derive(Clone)]
pub struct BlockCache {
    inner: Cache<u32, Bytes>,
}

impl BlockCache {
    /// Creates a new cache with the given maximum capacity in bytes.
    ///
    /// The cache evicts least-recently-used blocks when the total size
    /// of cached decompressed blocks exceeds `max_capacity_bytes`.
    pub fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .weigher(|_key: &u32, value: &Bytes| -> u32 {
                value.len().try_into().unwrap_or(u32::MAX)
            })
            .max_capacity(max_capacity_bytes)
            .build();
        Self { inner: cache }
    }

    /// Gets the decompressed block bytes, loading from storage on a cache miss.
    ///
    /// If the block is already cached, returns it immediately.
    /// If not, reads the full compressed block via `storage`, decompresses
    /// it using the codec recorded in the block metadata, inserts the
    /// result into the cache, and returns it.
    ///
    /// Concurrent calls for the same `block_id` are coalesced: only one
    /// task performs the I/O and decompression.
    pub async fn get_or_load(
        &self,
        block_meta: &BlockMeta,
        storage: &(dyn Storage + Sync),
    ) -> Result<Bytes> {
        let block_id = block_meta.id.0;
        let range = block_meta.file_range();
        let uncompressed_size = block_meta.uncompressed_size as usize;
        let codec_id = block_meta.codec.clone();

        self.inner
            .try_get_with(block_id, async move {
                let raw = storage.read_range(range).await?;
                let decompressed = if codec_id == CodecId::None {
                    raw // zero-copy: no decompression needed
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

    #[tokio::test]
    async fn cache_miss_loads_block() {
        let data = vec![0xAA; 100];
        let storage = InMemoryStorage::from_bytes(data.clone());
        let cache = BlockCache::new(1024);
        let meta = make_block_meta(0, 0, 100);

        let result = cache.get_or_load(&meta, &storage).await.unwrap();
        assert_eq!(&result[..], &data[..]);
    }

    #[tokio::test]
    async fn cache_hit_returns_same_bytes() {
        let data = vec![0xBB; 200];
        let storage = InMemoryStorage::from_bytes(data.clone());
        let cache = BlockCache::new(4096);
        let meta = make_block_meta(0, 0, 200);

        let first = cache.get_or_load(&meta, &storage).await.unwrap();
        let second = cache.get_or_load(&meta, &storage).await.unwrap();

        // Both should return identical bytes.
        assert_eq!(first, second);
        assert_eq!(&first[..], &data[..]);
    }
}
