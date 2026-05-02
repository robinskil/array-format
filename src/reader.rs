//! Reader with a shared [`BlockCache`] for parallel-safe array reads.
//!
//! A single [`Reader`] instance owns one [`BlockCache`]. Many threads
//! and async tasks can call read methods concurrently. The cache coalesces
//! concurrent loads for the same block so that only one I/O + decompress
//! cycle occurs per cache miss.
//!
//! The compression codec is inferred from each block's metadata at read
//! time — callers do not need to specify a codec when opening a file.

use std::sync::Arc;

use bytes::Bytes;

use crate::address::BlockId;
use crate::array::{self, ArrayData, NativeType, PrimitiveArray};
use crate::cache::BlockCache;
use crate::error::{Error, Result};
use crate::footer::Footer;
use crate::layout::{ArrayMeta, StorageLayout};
use crate::storage::Storage;

/// Default cache capacity: 64 MiB.
pub const DEFAULT_CACHE_CAPACITY: u64 = 64 * 1024 * 1024;

/// Reads arrays from an array-format file.
///
/// The reader loads the footer once at construction time and caches
/// decompressed blocks in a shared [`BlockCache`]. It is `Clone`-able
/// and safe to share across threads/tasks.
///
/// Array lookups by name are O(log n) via a sorted index with no string
/// duplication. Block lookups are O(1) via direct indexing (block IDs are
/// always assigned sequentially from 0).
///
/// The compression codec for each block is inferred from the block
/// metadata stored in the footer — no codec parameter is needed.
pub struct Reader {
    storage: Arc<dyn Storage>,
    footer: Footer,
    cache: BlockCache,
    /// Indices into `footer.arrays` for non-deleted arrays, sorted by name.
    /// Binary search gives O(log n) lookup with no string duplication.
    name_index: Vec<usize>,
}

/// Builds a sorted index of non-deleted array positions.
///
/// Each entry is an index into `arrays`. The vec is sorted by the name
/// at that position so binary search can be used for lookup.
fn build_name_index(arrays: &[ArrayMeta]) -> Vec<usize> {
    let mut indices: Vec<usize> = arrays
        .iter()
        .enumerate()
        .filter(|(_, a)| !a.deleted)
        .map(|(i, _)| i)
        .collect();
    indices.sort_by(|&a, &b| arrays[a].name.cmp(&arrays[b].name));
    indices
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            footer: self.footer.clone(),
            cache: self.cache.clone(),
            name_index: self.name_index.clone(),
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
        let name_index = build_name_index(&footer.arrays);

        Ok(Self {
            storage,
            footer,
            cache: BlockCache::new(cache_capacity_bytes),
            name_index,
        })
    }

    /// Returns metadata for all non-deleted arrays.
    pub fn list_arrays(&self) -> Vec<&ArrayMeta> {
        self.name_index
            .iter()
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

        match &array.layout.storage {
            StorageLayout::Flat { address } => {
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
            StorageLayout::Chunked { chunks, .. } => {
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

        let addr = match &array.layout.storage {
            StorageLayout::Chunked { chunks, .. } => chunks
                .iter()
                .find(|(c, _)| c == coord)
                .map(|(_, a)| a)
                .ok_or_else(|| Error::ArrayNotFound {
                    name: format!("{name}[{coord:?}]"),
                })?,
            StorageLayout::Flat { .. } => {
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

    /// Loads a decompressed block by its id.
    ///
    /// Block IDs are always assigned sequentially from 0, so the id is used
    /// as a direct index into `footer.blocks` with no indirection.
    async fn load_block(&self, block_id: BlockId) -> Result<Bytes> {
        let block_meta = self
            .footer
            .blocks
            .get(block_id.0 as usize)
            .ok_or(Error::BlockOutOfRange {
                block_id: block_id.0,
            })?;
        self.cache
            .get_or_load(block_meta, self.storage.as_ref())
            .await
    }

    /// O(log n) lookup of a non-deleted array by name.
    pub fn get_array(&self, name: &str) -> Result<&ArrayMeta> {
        self.name_index
            .binary_search_by(|&i| self.footer.arrays[i].name.as_str().cmp(name))
            .map(|pos| &self.footer.arrays[self.name_index[pos]])
            .map_err(|_| Error::ArrayNotFound { name: name.into() })
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
                vec![50],
                None,
                &[1u8; 50],
            )
            .unwrap();
        writer
            .write_flat(
                "twos",
                crate::dtype::DType::UInt8,
                vec!["x".into()],
                vec![30],
                None,
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
                .write_flat("keep", crate::dtype::DType::UInt8, vec![], vec![], None, &[1])
                .unwrap();
            writer
                .write_flat("remove", crate::dtype::DType::UInt8, vec![], vec![], None, &[2])
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
                .write_flat(
                    "big",
                    crate::dtype::DType::UInt8,
                    vec!["x".into()],
                    vec![200],
                    None,
                    &data,
                )
                .unwrap();
            writer.flush().await.unwrap();
        }

        let reader = Reader::open(storage, 4096).await.unwrap();
        let arr = reader.read_array::<u8>("big").await.unwrap();
        assert_eq!(arr.values(), &data[..]);
    }

    #[tokio::test]
    async fn lookup_order_independent_of_write_order() {
        // Arrays written in reverse alphabetical order must still be findable
        // by name, confirming the sorted name_index is built correctly.
        let storage = InMemoryStorage::new();
        {
            let mut writer = Writer::new(storage.clone(), small_config());
            for name in ["z_arr", "m_arr", "a_arr"] {
                writer
                    .write_flat(name, crate::dtype::DType::UInt8, vec![], vec![], None, &[0])
                    .unwrap();
            }
            writer.flush().await.unwrap();
        }

        let reader = Reader::open(storage, 1024).await.unwrap();
        assert!(reader.get_array("a_arr").is_ok());
        assert!(reader.get_array("m_arr").is_ok());
        assert!(reader.get_array("z_arr").is_ok());
        assert!(reader.get_array("missing").is_err());

        // list_arrays returns in sorted order
        let names: Vec<_> = reader.list_arrays().iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, vec!["a_arr", "m_arr", "z_arr"]);
    }
}
