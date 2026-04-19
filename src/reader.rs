//! Reader with a shared [`BlockCache`] for parallel-safe array reads.
//!
//! A single [`Reader`] instance owns one [`BlockCache`]. Many threads
//! and async tasks can call read methods concurrently. The cache coalesces
//! concurrent loads for the same block so that only one I/O + decompress
//! cycle occurs per cache miss.
//!
//! The compression codec is inferred from each block's metadata at read
//! time — callers do not need to specify a codec when opening a file.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::address::BlockId;
use crate::array::{self, ArrayData, NativeType, PrimitiveArray};
use crate::block::BlockMeta;
use crate::cache::BlockCache;
use crate::error::{Error, Result};
use crate::footer::Footer;
use crate::layout::{ArrayLayout, ArrayMeta};
use crate::storage::Storage;

/// Default cache capacity: 64 MiB.
pub const DEFAULT_CACHE_CAPACITY: u64 = 64 * 1024 * 1024;

/// Reads arrays from an array-format file.
///
/// The reader loads the footer once at construction time and caches
/// decompressed blocks in a shared [`BlockCache`]. It is `Clone`-able
/// and safe to share across threads/tasks.
///
/// Array lookups by name are O(1) via an internal `HashMap` index.
///
/// The compression codec for each block is inferred from the block
/// metadata stored in the footer — no codec parameter is needed.
pub struct Reader {
    storage: Arc<dyn Storage>,
    footer: Footer,
    cache: BlockCache,
    /// name → index into `footer.arrays` for non-deleted arrays.
    array_index: HashMap<String, usize>,
    /// block_id → index into `footer.blocks` for O(1) block lookup.
    block_index: HashMap<BlockId, usize>,
}

/// Builds a name→index map for non-deleted arrays.
fn build_array_index(arrays: &[ArrayMeta]) -> HashMap<String, usize> {
    arrays
        .iter()
        .enumerate()
        .filter(|(_, a)| !a.deleted)
        .map(|(i, a)| (a.name.clone(), i))
        .collect()
}

/// Builds a block_id→index map for O(1) block lookup.
fn build_block_index(blocks: &[BlockMeta]) -> HashMap<BlockId, usize> {
    blocks.iter().enumerate().map(|(i, b)| (b.id, i)).collect()
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            footer: self.footer.clone(),
            cache: self.cache.clone(),
            array_index: self.array_index.clone(),
            block_index: self.block_index.clone(),
        }
    }
}

impl Reader {
    /// Opens a file and reads its footer.
    ///
    /// The codec is inferred from each block's metadata at read time.
    /// `cache_capacity_bytes` sets the maximum total size of decompressed
    /// blocks held in the cache.
    pub async fn open(storage: impl Storage + 'static, cache_capacity_bytes: u64) -> Result<Self> {
        let storage: Arc<dyn Storage> = Arc::new(storage);
        Self::open_dyn(storage, cache_capacity_bytes).await
    }

    /// Opens a file from an already-boxed storage reference.
    pub async fn open_dyn(storage: Arc<dyn Storage>, cache_capacity_bytes: u64) -> Result<Self> {
        let footer = crate::footer::read_footer(storage.as_ref()).await?;
        let array_index = build_array_index(&footer.arrays);
        let block_index = build_block_index(&footer.blocks);

        Ok(Self {
            storage,
            footer,
            cache: BlockCache::new(cache_capacity_bytes),
            array_index,
            block_index,
        })
    }

    /// Returns metadata for all non-deleted arrays.
    pub fn list_arrays(&self) -> Vec<&ArrayMeta> {
        self.array_index
            .values()
            .map(|&i| &self.footer.arrays[i])
            .collect()
    }

    /// Reads a flat array as a typed [`PrimitiveArray<T>`].
    ///
    /// Returns [`Error::DTypeMismatch`] if `T::DTYPE` does not match the
    /// array's dtype in the footer.
    pub async fn read_array<T: NativeType>(&self, name: &str) -> Result<PrimitiveArray<T>> {
        let array = self.get_array(name)?;
        if array.dtype != T::DTYPE {
            return Err(Error::DTypeMismatch {
                expected: array.dtype.clone(),
                actual: T::DTYPE,
            });
        }
        let bytes = self.read_raw_bytes(name).await?;
        PrimitiveArray::<T>::from_bytes(bytes)
    }

    /// Reads a flat array as a `Box<dyn ArrayData>`, dynamically
    /// dispatching based on the dtype stored in the footer.
    pub async fn read_array_dynamic(&self, name: &str) -> Result<Box<dyn ArrayData>> {
        let array = self.get_array(name)?;
        let dtype = array.dtype.clone();
        let bytes = self.read_raw_bytes(name).await?;
        array::from_bytes_dynamic(&dtype, bytes)
    }

    /// Reads a single chunk from a chunked array as a typed [`PrimitiveArray<T>`].
    pub async fn read_chunk<T: NativeType>(
        &self,
        name: &str,
        coord: &[u32],
    ) -> Result<PrimitiveArray<T>> {
        let array = self.get_array(name)?;
        if array.dtype != T::DTYPE {
            return Err(Error::DTypeMismatch {
                expected: array.dtype.clone(),
                actual: T::DTYPE,
            });
        }
        let bytes = self.read_chunk_raw(name, coord).await?;
        PrimitiveArray::<T>::from_bytes(bytes)
    }

    /// Reads a single chunk as a `Box<dyn ArrayData>`.
    pub async fn read_chunk_dynamic(
        &self,
        name: &str,
        coord: &[u32],
    ) -> Result<Box<dyn ArrayData>> {
        let array = self.get_array(name)?;
        let dtype = array.dtype.clone();
        let bytes = self.read_chunk_raw(name, coord).await?;
        array::from_bytes_dynamic(&dtype, bytes)
    }

    /// Returns a reference to the loaded footer.
    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    // ── internal helpers ────────────────────────────────────────────

    /// Reads the full raw bytes for an array.
    ///
    /// For flat arrays (single address) this returns a zero-copy
    /// `Bytes::slice()` from the cached block with no allocation.
    pub async fn read_raw_bytes(&self, name: &str) -> Result<Bytes> {
        let array = self.get_array(name)?;

        match &array.layout {
            ArrayLayout::Flat { address } => {
                let block_bytes = self.load_block(address.block_id).await?;
                let start = address.offset as usize;
                let end = start + address.size as usize;
                if end > block_bytes.len() {
                    return Err(Error::BlockOutOfRange {
                        block_id: address.block_id.0,
                    });
                }
                Ok(block_bytes.slice(start..end))
            }
            ArrayLayout::Chunked { chunks, .. } => {
                let total: usize = chunks.iter().map(|(_, a)| a.size as usize).sum();
                let mut out = Vec::with_capacity(total);
                for (_, addr) in chunks {
                    let block_bytes = self.load_block(addr.block_id).await?;
                    let start = addr.offset as usize;
                    let end = start + addr.size as usize;
                    if end > block_bytes.len() {
                        return Err(Error::BlockOutOfRange {
                            block_id: addr.block_id.0,
                        });
                    }
                    out.extend_from_slice(&block_bytes[start..end]);
                }
                Ok(Bytes::from(out))
            }
        }
    }

    /// Reads a single chunk's raw bytes.
    pub async fn read_chunk_raw(&self, name: &str, coord: &[u32]) -> Result<Bytes> {
        let array = self.get_array(name)?;

        let addr = match &array.layout {
            ArrayLayout::Chunked { chunks, .. } => chunks
                .iter()
                .find(|(c, _)| c == coord)
                .map(|(_, a)| a)
                .ok_or_else(|| Error::ArrayNotFound {
                    name: format!("{name}[{coord:?}]"),
                })?,
            ArrayLayout::Flat { .. } => {
                return Err(Error::InvalidFooter(
                    "read_chunk called on a flat array".into(),
                ));
            }
        };

        let block_bytes = self.load_block(addr.block_id).await?;
        let start = addr.offset as usize;
        let end = start + addr.size as usize;
        if end > block_bytes.len() {
            return Err(Error::BlockOutOfRange {
                block_id: addr.block_id.0,
            });
        }
        Ok(block_bytes.slice(start..end))
    }

    /// Loads a decompressed block by its id, using the block index for O(1) lookup.
    async fn load_block(&self, block_id: BlockId) -> Result<Bytes> {
        let &idx = self
            .block_index
            .get(&block_id)
            .ok_or(Error::BlockOutOfRange {
                block_id: block_id.0,
            })?;
        let block_meta = &self.footer.blocks[idx];
        self.cache
            .get_or_load(block_meta, self.storage.as_ref())
            .await
    }

    /// O(1) lookup of a non-deleted array by name.
    fn get_array(&self, name: &str) -> Result<&ArrayMeta> {
        self.array_index
            .get(name)
            .map(|&i| &self.footer.arrays[i])
            .ok_or_else(|| Error::ArrayNotFound { name: name.into() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::NoCompression;
    use crate::storage::InMemoryStorage;
    use crate::writer::{Writer, WriterConfig};

    fn small_config() -> WriterConfig<NoCompression> {
        WriterConfig {
            block_target_size: 64,
            codec: NoCompression,
        }
    }

    async fn write_test_file(storage: &InMemoryStorage) {
        let mut writer = Writer::new(storage.clone(), small_config());
        writer
            .write_flat(
                "ones",
                crate::dtype::DType::UInt8,
                vec!["x".into()],
                &[1u8; 50],
            )
            .unwrap();
        writer
            .write_flat(
                "twos",
                crate::dtype::DType::UInt8,
                vec!["x".into()],
                &[2u8; 30],
            )
            .unwrap();
        writer.flush().await.unwrap();
    }

    #[tokio::test]
    async fn read_flat_array() {
        let storage = InMemoryStorage::new();
        write_test_file(&storage).await;

        let reader = Reader::open(storage, 1024).await.unwrap();
        let arr = reader.read_array::<u8>("ones").await.unwrap();
        assert_eq!(arr.values(), &[1u8; 50]);

        let arr = reader.read_array::<u8>("twos").await.unwrap();
        assert_eq!(arr.values(), &[2u8; 30]);
    }

    #[tokio::test]
    async fn read_array_dynamic_dispatch() {
        let storage = InMemoryStorage::new();
        write_test_file(&storage).await;

        let reader = Reader::open(storage, 1024).await.unwrap();
        let arr = reader.read_array_dynamic("ones").await.unwrap();
        assert_eq!(*arr.dtype(), crate::dtype::DType::UInt8);
        assert_eq!(arr.as_bytes(), &[1u8; 50]);
    }

    #[tokio::test]
    async fn read_array_dtype_mismatch() {
        let storage = InMemoryStorage::new();
        write_test_file(&storage).await;

        let reader = Reader::open(storage, 1024).await.unwrap();
        // "ones" is UInt8, reading as i32 should fail
        let result = reader.read_array::<i32>("ones").await;
        assert!(matches!(result, Err(Error::DTypeMismatch { .. })));
    }

    #[tokio::test]
    async fn list_arrays_excludes_deleted() {
        let storage = InMemoryStorage::new();
        {
            let mut writer = Writer::new(storage.clone(), small_config());
            writer
                .write_flat("keep", crate::dtype::DType::UInt8, vec![], &[1])
                .unwrap();
            writer
                .write_flat("remove", crate::dtype::DType::UInt8, vec![], &[2])
                .unwrap();
            writer.delete("remove").unwrap();
            writer.flush().await.unwrap();
        }

        let reader = Reader::open(storage, 1024).await.unwrap();
        let names: Vec<_> = reader.list_arrays().iter().map(|a| &a.name).collect();
        assert_eq!(names, vec!["keep"]);
    }

    #[tokio::test]
    async fn read_array_not_found() {
        let storage = InMemoryStorage::new();
        write_test_file(&storage).await;
        let reader = Reader::open(storage, 1024).await.unwrap();
        let result = reader.read_array::<u8>("nonexistent").await;
        assert!(matches!(result, Err(Error::ArrayNotFound { .. })));
    }

    #[tokio::test]
    async fn read_array_exceeding_block_target() {
        let storage = InMemoryStorage::new();
        let data: Vec<u8> = (0..200).map(|i| (i % 256) as u8).collect();
        {
            let mut writer = Writer::new(storage.clone(), small_config());
            writer
                .write_flat("big", crate::dtype::DType::UInt8, vec!["x".into()], &data)
                .unwrap();
            writer.flush().await.unwrap();
        }

        let reader = Reader::open(storage, 4096).await.unwrap();
        let arr = reader.read_array::<u8>("big").await.unwrap();
        assert_eq!(arr.values(), &data[..]);
    }
}
