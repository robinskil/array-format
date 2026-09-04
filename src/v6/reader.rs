//! Read-only view of a finished file.
//!
//! [`ArrayFile::open`] reads the footer once and turns it into the in-memory
//! form: one [`ArrayInfo`] per array in file order, a name index, and one
//! attribute map per array. Data blocks are read on demand through an
//! optional cache.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use ecow::EcoString;

use crate::{
    array::ArrayElement,
    block::BlockMeta,
    codec::decompress_by_id,
    // The block cache of the old delta layer. It moves here under this name
    // once the delta modules are removed.
    delta::DeltaCache as BlockCache,
    dtype::DType,
    error::{Error, Result},
    file::{DEFAULT_CACHE_CAPACITY, DEFAULT_IO_CACHE_CAPACITY},
    layout::{ChunkEntry, FillValue},
    stats::ArrayStats,
    storage::{ObjectStoreBackend, Storage},
};

use super::{AttributeValue, Footer, footer::find_chunk, nd, read_footer};

/// Options for opening an [`ArrayFile`].
///
/// A file needs no codec to open: every block records its own.
pub struct ReadConfig {
    /// Byte budget for this file's decompressed-block cache.
    ///
    /// Ignored when [`cache`](Self::cache) is `Some`.
    pub cache_capacity: usize,
    /// Byte budget for this file's raw I/O slab cache. `0` disables it.
    ///
    /// Ignored when [`cache`](Self::cache) is `Some`.
    pub io_cache_capacity: usize,
    /// A cache to share across files. Entries are keyed by `(path, block)`,
    /// so files do not collide. When `Some`, the two budgets above are ignored.
    pub cache: Option<Arc<BlockCache>>,
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            io_cache_capacity: DEFAULT_IO_CACHE_CAPACITY,
            cache: None,
        }
    }
}

impl ReadConfig {
    pub(crate) fn resolve_cache(&self) -> Option<Arc<BlockCache>> {
        if let Some(c) = &self.cache {
            return Some(Arc::clone(c));
        }
        if self.cache_capacity == 0 {
            return None;
        }
        Some(Arc::new(BlockCache::new(
            self.cache_capacity as u64,
            self.io_cache_capacity as u64,
        )))
    }
}

/// Metadata of one array, as stored in the file.
#[derive(Debug, Clone)]
pub struct ArrayInfo {
    /// Unique name within the file.
    pub name: EcoString,
    /// Element type.
    pub dtype: DType,
    /// Length of each dimension.
    pub shape: Vec<u32>,
    /// Name of each dimension.
    pub dimension_names: Vec<String>,
    /// Extent of one chunk along each dimension.
    pub chunk_shape: Vec<u32>,
    /// Fill value for unwritten elements. `None` means zero or empty.
    pub fill_value: Option<FillValue>,
    /// Aggregate statistics over every chunk, if the writer computed them.
    pub stats: Option<ArrayStats>,
    /// Written chunks, sorted by coordinate.
    chunks: Vec<ChunkEntry>,
}

impl ArrayInfo {
    /// Coordinates of every written chunk, in sorted order.
    pub fn written_chunks(&self) -> impl Iterator<Item = &[u32]> {
        self.chunks.iter().map(|e| e.coord.as_slice())
    }

    /// Number of elements in the chunk at `coord`. Edge chunks are clipped to
    /// the array shape.
    fn chunk_len(&self, coord: &[u32]) -> usize {
        self.chunk_shape
            .iter()
            .enumerate()
            .map(|(i, &cs)| {
                let axis_len = self.shape[i] as usize;
                let start = coord[i] as usize * cs as usize;
                (cs as usize).min(axis_len.saturating_sub(start))
            })
            .product()
    }
}

/// A finished, read-only array file. See the [module docs](self).
pub struct ArrayFile {
    storage: Arc<dyn Storage>,
    /// Cache key for this file's blocks.
    path: Arc<str>,
    cache: Option<Arc<BlockCache>>,
    /// Block table; block ids are dense, so `blocks[id]` is the block.
    blocks: Vec<BlockMeta>,
    /// Every array, in file order.
    arrays: Vec<ArrayInfo>,
    /// Attributes of `arrays[i]` are `attrs[i]`.
    attrs: Vec<HashMap<EcoString, AttributeValue>>,
    /// Array name to position in `arrays`.
    by_name: HashMap<EcoString, usize>,
}

impl std::fmt::Debug for ArrayFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayFile")
            .field("path", &self.path)
            .field("arrays", &self.arrays.len())
            .field("blocks", &self.blocks.len())
            .finish_non_exhaustive()
    }
}

// ── Opening ──────────────────────────────────────────────────────────

impl ArrayFile {
    /// Opens the file at `path` in `store`.
    pub async fn open(
        store: Arc<dyn object_store::ObjectStore>,
        path: object_store::path::Path,
        config: ReadConfig,
    ) -> Result<Self> {
        let key: Arc<str> = Arc::from(path.as_ref());
        let storage: Arc<dyn Storage> = Arc::new(ObjectStoreBackend::new(store, path));
        Self::open_storage(storage, key, config.resolve_cache()).await
    }

    /// Opens a file from any [`Storage`]. `path` is only the cache key.
    pub(crate) async fn open_storage(
        storage: Arc<dyn Storage>,
        path: Arc<str>,
        cache: Option<Arc<BlockCache>>,
    ) -> Result<Self> {
        let footer = read_footer(&*storage).await?;
        Self::from_footer(footer, storage, path, cache)
    }

    /// Builds the in-memory form from a footer. Moves every field out of the
    /// footer; only the string pool is converted, once.
    pub(crate) fn from_footer(
        footer: Footer,
        storage: Arc<dyn Storage>,
        path: Arc<str>,
        cache: Option<Arc<BlockCache>>,
    ) -> Result<Self> {
        let strings: Vec<EcoString> = footer.strings.into_iter().map(EcoString::from).collect();
        let n = footer.arrays.len();
        let mut arrays = Vec::with_capacity(n);
        let mut attrs = Vec::with_capacity(n);
        let mut by_name = HashMap::with_capacity(n);

        for (i, meta) in footer.arrays.into_iter().enumerate() {
            let mut map = HashMap::with_capacity(meta.attributes.len());
            for (key, value) in meta.attributes {
                let key = strings.get(key as usize).cloned().ok_or_else(|| {
                    Error::InvalidFooter(format!(
                        "attribute key index {key} out of range, pool has {} strings",
                        strings.len()
                    ))
                })?;
                map.insert(key, value.decode(&strings)?);
            }

            let name = EcoString::from(meta.name);
            if by_name.insert(name.clone(), i).is_some() {
                return Err(Error::InvalidFooter(format!(
                    "array '{name}' is defined twice"
                )));
            }
            arrays.push(ArrayInfo {
                name,
                dtype: meta.dtype,
                shape: meta.shape,
                dimension_names: meta.dimension_names,
                chunk_shape: meta.chunk_shape,
                fill_value: meta.fill_value,
                stats: meta.stats,
                chunks: meta.chunks,
            });
            attrs.push(map);
        }

        Ok(Self {
            storage,
            path,
            cache,
            blocks: footer.blocks,
            arrays,
            attrs,
            by_name,
        })
    }
}

// ── Schema, attributes, stats ────────────────────────────────────────

impl ArrayFile {
    /// Every array, in file order.
    pub fn arrays(&self) -> &[ArrayInfo] {
        &self.arrays
    }

    /// The array called `name`, if present.
    pub fn array(&self, name: &str) -> Option<&ArrayInfo> {
        self.by_name.get(name).map(|&i| &self.arrays[i])
    }

    /// All attributes of array `name`, or `None` if the array does not exist.
    pub fn attributes(&self, name: &str) -> Option<&HashMap<EcoString, AttributeValue>> {
        self.by_name.get(name).map(|&i| &self.attrs[i])
    }

    /// Attribute `key` of array `name`. `None` if either does not exist.
    pub fn get_attribute(&self, name: &str, key: &str) -> Option<&AttributeValue> {
        self.attributes(name)?.get(key)
    }

    /// Attribute `key` for every array, in file order: `Some(value)` where
    /// the array carries it, `None` where it does not.
    ///
    /// Use this to select arrays by attribute without a call per array.
    /// Names and values borrow from the open file.
    pub fn attribute_index(&self, key: &str) -> Vec<(&str, Option<&AttributeValue>)> {
        self.arrays
            .iter()
            .zip(&self.attrs)
            .map(|(info, map)| (info.name.as_str(), map.get(key)))
            .collect()
    }

    /// Statistics of array `name`, if the array exists and has them.
    pub fn array_stats(&self, name: &str) -> Option<&ArrayStats> {
        self.array(name)?.stats.as_ref()
    }

    /// Statistics of every array that has them, in file order.
    pub fn stats(&self) -> impl Iterator<Item = &ArrayStats> {
        self.arrays.iter().filter_map(|a| a.stats.as_ref())
    }

    fn info(&self, name: &str) -> Result<&ArrayInfo> {
        self.array(name).ok_or_else(|| Error::ArrayNotFound {
            name: name.to_string(),
        })
    }
}

// ── Reading ──────────────────────────────────────────────────────────

impl ArrayFile {
    /// Reads the sub-region of array `name` starting at `start` with the given
    /// `shape`.
    ///
    /// Pass `vec![], vec![]` to read the whole array. Chunks that were never
    /// written come back as the fill value. `T` must match the array's
    /// declared dtype, otherwise [`Error::DTypeMismatch`] is returned.
    pub async fn read_array<T: ArrayElement>(
        &self,
        name: &str,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> Result<ndarray::ArcArray<T, ndarray::IxDyn>> {
        let info = self.info(name)?;
        let slice: Option<Vec<Range<usize>>> = if start.is_empty() && shape.is_empty() {
            None
        } else {
            let ndim = info.shape.len();
            let start = if start.len() == ndim {
                start
            } else {
                vec![0; ndim]
            };
            let shape: Vec<usize> = if shape.len() == ndim {
                shape
            } else {
                info.shape.iter().map(|&s| s as usize).collect()
            };
            Some(start.iter().zip(&shape).map(|(&s, &n)| s..s + n).collect())
        };
        nd::assemble_nd(self, info, slice.as_deref()).await
    }

    /// Decodes the chunk at `coord`. An unwritten chunk is materialized from
    /// the fill value.
    pub(crate) async fn read_chunk<T: ArrayElement>(
        &self,
        info: &ArrayInfo,
        coord: &[u32],
    ) -> Result<Vec<T>> {
        if let Some(bytes) = self.read_raw_chunk(info, coord).await? {
            return Ok(T::decode_chunk(&bytes));
        }
        Ok(vec![
            T::fill_element(info.fill_value.as_ref());
            info.chunk_len(coord)
        ])
    }

    /// Raw bytes of the chunk at `coord`, or `None` if it was never written.
    pub(crate) async fn read_raw_chunk(
        &self,
        info: &ArrayInfo,
        coord: &[u32],
    ) -> Result<Option<Bytes>> {
        let Some(addr) = find_chunk(&info.chunks, coord) else {
            return Ok(None);
        };
        let block = self
            .blocks
            .get(addr.block_id.0 as usize)
            .ok_or(Error::BlockOutOfRange {
                block_id: addr.block_id.0,
            })?;

        let block_bytes = match &self.cache {
            Some(cache) => cache.get_or_load(&self.path, block, &*self.storage).await?,
            None => {
                let compressed = self.storage.read_range(block.file_range()).await?;
                Bytes::from(decompress_by_id(
                    &block.codec,
                    &compressed,
                    block.uncompressed_size as usize,
                )?)
            }
        };

        let start = addr.offset as usize;
        let end = start + addr.size as usize;
        if end > block_bytes.len() {
            return Err(Error::InvalidFooter(format!(
                "chunk {coord:?} of '{}' ends at {end}, past block {} of {} bytes",
                info.name,
                addr.block_id.0,
                block_bytes.len()
            )));
        }
        Ok(Some(block_bytes.slice(start..end)))
    }
}

#[cfg(test)]
mod tests {
    use ndarray::Array;

    use super::*;
    use crate::address::{BlockId, ChunkAddress};
    use crate::stats::StatValue;
    use crate::storage::InMemoryStorage;
    use crate::v6::{ArrayMeta, ArrayWriter, FOOTER_VERSION, WriterConfig};
    use crate::{Lz4Codec, NoCompression};

    fn writer() -> ArrayWriter {
        ArrayWriter::new(WriterConfig {
            block_target_size: 64,
            ..WriterConfig::new(NoCompression)
        })
    }

    /// Finishes into memory; returns the reader from `finish` and the
    /// storage, so a test can also reopen the same bytes.
    async fn finish(w: ArrayWriter) -> (ArrayFile, Arc<InMemoryStorage>) {
        let storage = Arc::new(InMemoryStorage::new());
        let file = w
            .finish_to(storage.clone(), Arc::from("mem"), None)
            .await
            .unwrap();
        (file, storage)
    }

    async fn reopen(storage: Arc<InMemoryStorage>, cache: Option<Arc<BlockCache>>) -> ArrayFile {
        ArrayFile::open_storage(storage, Arc::from("mem"), cache)
            .await
            .unwrap()
    }

    fn sample() -> ArrayWriter {
        let mut w = writer();
        w.define_array::<f32>("temp", vec!["t".into()], vec![6], Some(vec![4]), None)
            .unwrap();
        w.write_array(
            "temp",
            vec![0],
            Array::from_vec(vec![1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0])
                .into_dyn()
                .view(),
        )
        .unwrap();
        w.set_attribute("temp", "units", AttributeValue::String("K".into()))
            .unwrap();
        w.set_attribute("temp", "scale", AttributeValue::Float64(0.5))
            .unwrap();

        w.define_array::<i32>(
            "count",
            vec!["t".into()],
            vec![3],
            None,
            Some(FillValue::Int(-1)),
        )
        .unwrap();
        w.set_attribute("count", "units", AttributeValue::String("n".into()))
            .unwrap();
        w
    }

    #[tokio::test]
    async fn finish_returns_a_reader_that_matches_a_fresh_open() {
        let (from_finish, storage) = finish(sample()).await;
        let from_open = reopen(storage, None).await;

        for file in [&from_finish, &from_open] {
            let names: Vec<&str> = file.arrays().iter().map(|a| a.name.as_str()).collect();
            assert_eq!(names, vec!["temp", "count"], "file order");
            let temp = file
                .read_array::<f32>("temp", vec![], vec![])
                .await
                .unwrap();
            assert_eq!(
                temp.iter().copied().collect::<Vec<_>>(),
                vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
            );
            assert_eq!(
                file.get_attribute("temp", "units"),
                Some(&AttributeValue::String("K".into()))
            );
        }
    }

    #[tokio::test]
    async fn array_lookup_borrows_metadata() {
        let (file, _) = finish(sample()).await;
        let temp = file.array("temp").unwrap();
        assert_eq!(temp.dtype, DType::Float32);
        assert_eq!(temp.shape, vec![6]);
        assert_eq!(temp.chunk_shape, vec![4]);
        assert_eq!(temp.dimension_names, vec!["t"]);
        assert_eq!(
            temp.written_chunks().collect::<Vec<_>>(),
            vec![&[0u32][..], &[1]]
        );
        let count = file.array("count").unwrap();
        assert_eq!(count.fill_value, Some(FillValue::Int(-1)));
        assert!(count.written_chunks().next().is_none());
        assert!(file.array("missing").is_none());
    }

    #[tokio::test]
    async fn unwritten_chunks_read_as_fill_and_sub_regions_span_chunks() {
        let (file, _) = finish(sample()).await;
        let count = file
            .read_array::<i32>("count", vec![], vec![])
            .await
            .unwrap();
        assert_eq!(count.iter().copied().collect::<Vec<_>>(), vec![-1, -1, -1]);

        // [3, 5) crosses the chunk boundary at 4.
        let part = file
            .read_array::<f32>("temp", vec![3], vec![2])
            .await
            .unwrap();
        assert_eq!(part.shape(), &[2]);
        assert_eq!(part.iter().copied().collect::<Vec<_>>(), vec![4.0, 5.0]);
    }

    #[tokio::test]
    async fn partially_written_chunked_array_mixes_data_and_fill() {
        let mut w = writer();
        w.define_array::<u8>(
            "x",
            vec![],
            vec![8],
            Some(vec![4]),
            Some(FillValue::UInt(9)),
        )
        .unwrap();
        w.write_array(
            "x",
            vec![4],
            Array::from_vec(vec![1u8, 2, 3, 4]).into_dyn().view(),
        )
        .unwrap();
        let (file, _) = finish(w).await;
        let x = file.read_array::<u8>("x", vec![], vec![]).await.unwrap();
        assert_eq!(
            x.iter().copied().collect::<Vec<_>>(),
            vec![9, 9, 9, 9, 1, 2, 3, 4]
        );
    }

    #[tokio::test]
    async fn attributes_decode_into_per_array_maps() {
        let (file, _) = finish(sample()).await;
        let temp = file.attributes("temp").unwrap();
        assert_eq!(temp.len(), 2);
        assert_eq!(temp.get("scale"), Some(&AttributeValue::Float64(0.5)));
        assert_eq!(file.get_attribute("count", "scale"), None);
        assert_eq!(file.get_attribute("missing", "units"), None);
        assert!(file.attributes("missing").is_none());

        let column = file.attribute_index("units");
        assert_eq!(
            column,
            vec![
                ("temp", Some(&AttributeValue::String("K".into()))),
                ("count", Some(&AttributeValue::String("n".into()))),
            ]
        );
        let scale = file.attribute_index("scale");
        assert_eq!(scale[0].1, Some(&AttributeValue::Float64(0.5)));
        assert_eq!(scale[1], ("count", None));
        assert!(
            file.attribute_index("nope")
                .iter()
                .all(|(_, v)| v.is_none())
        );
    }

    #[tokio::test]
    async fn a_long_string_value_is_shared_between_arrays() {
        let long = "a value longer than fifteen bytes";
        let mut w = writer();
        for name in ["a", "b"] {
            w.define_array::<u8>(name, vec![], vec![1], None, None)
                .unwrap();
            w.set_attribute(name, "k", AttributeValue::String(long.into()))
                .unwrap();
        }
        let (_, storage) = finish(w).await;
        // The open path is what builds the maps from the pool.
        let file = reopen(storage, None).await;
        let get = |name: &str| match file.get_attribute(name, "k") {
            Some(AttributeValue::String(s)) => s,
            other => panic!("unexpected {other:?}"),
        };
        let (a, b) = (get("a"), get("b"));
        assert_eq!(a, long);
        assert_eq!(
            a.as_ptr(),
            b.as_ptr(),
            "one heap allocation, two references"
        );
    }

    #[tokio::test]
    async fn stats_per_array_and_iterated() {
        let (file, _) = finish(sample()).await;
        let temp = file.array_stats("temp").unwrap();
        assert_eq!(temp.min, Some(StatValue::Float(1.0)));
        assert_eq!(temp.max, Some(StatValue::Float(6.0)));
        assert_eq!(temp.row_count, 6);
        assert_eq!(temp.null_count, 0);
        let count = file.array_stats("count").unwrap();
        assert_eq!(count.null_count, 3, "never written");
        assert!(file.array_stats("missing").is_none());
        let names: Vec<&str> = file.stats().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["temp", "count"]);
    }

    #[tokio::test]
    async fn read_errors() {
        let (file, _) = finish(sample()).await;
        assert!(matches!(
            file.read_array::<i32>("temp", vec![], vec![]).await,
            Err(Error::DTypeMismatch { .. })
        ));
        assert!(matches!(
            file.read_array::<f32>("missing", vec![], vec![]).await,
            Err(Error::ArrayNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn cached_and_uncached_reads_agree_on_compressed_blocks() {
        let mut w = ArrayWriter::new(WriterConfig {
            block_target_size: 64,
            ..WriterConfig::new(Lz4Codec)
        });
        w.define_array::<u16>("x", vec![], vec![200], Some(vec![50]), None)
            .unwrap();
        let values: Vec<u16> = (0..200).collect();
        w.write_array(
            "x",
            vec![0],
            Array::from_vec(values.clone()).into_dyn().view(),
        )
        .unwrap();
        let (_, storage) = finish(w).await;

        let cached = reopen(storage.clone(), Some(Arc::new(BlockCache::new(1 << 20, 0)))).await;
        let plain = reopen(storage, None).await;
        for file in [&cached, &plain] {
            let x = file.read_array::<u16>("x", vec![], vec![]).await.unwrap();
            assert_eq!(x.iter().copied().collect::<Vec<_>>(), values);
            // Second read hits the cache on the cached file.
            let again = file
                .read_array::<u16>("x", vec![100], vec![10])
                .await
                .unwrap();
            assert_eq!(again.iter().copied().collect::<Vec<_>>(), &values[100..110]);
        }
    }

    #[tokio::test]
    async fn zero_length_axis_reads_back_empty() {
        let mut w = writer();
        w.define_array::<f32>("h", vec!["n".into(), "x".into()], vec![0, 3], None, None)
            .unwrap();
        let (file, _) = finish(w).await;
        let h = file.read_array::<f32>("h", vec![], vec![]).await.unwrap();
        assert_eq!(h.shape(), &[0, 3]);
        assert_eq!(file.array("h").unwrap().shape, vec![0, 3]);
    }

    #[tokio::test]
    async fn duplicate_array_names_are_rejected_on_open() {
        let meta = ArrayMeta {
            name: "twice".into(),
            dtype: DType::UInt8,
            shape: vec![1],
            dimension_names: vec!["x".into()],
            chunk_shape: vec![1],
            chunks: vec![],
            fill_value: None,
            attributes: vec![],
            stats: None,
        };
        let footer = Footer {
            version: FOOTER_VERSION,
            blocks: vec![],
            strings: vec![],
            arrays: vec![meta.clone(), meta],
        };
        let storage = Arc::new(InMemoryStorage::from_bytes(footer.serialize().unwrap()));
        let err = ArrayFile::open_storage(storage, Arc::from("mem"), None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidFooter(ref m) if m.contains("twice")));
    }

    #[tokio::test]
    async fn a_chunk_pointing_past_its_block_is_an_error_not_a_panic() {
        let mut footer = Footer::new();
        footer.blocks.push(BlockMeta {
            id: BlockId(0),
            file_offset: 0,
            compressed_size: 4,
            uncompressed_size: 4,
            codec: crate::block::CodecId::None,
        });
        footer.arrays.push(ArrayMeta {
            name: "x".into(),
            dtype: DType::UInt8,
            shape: vec![8],
            dimension_names: vec!["x".into()],
            chunk_shape: vec![8],
            chunks: vec![ChunkEntry {
                coord: vec![0],
                address: ChunkAddress {
                    block_id: BlockId(0),
                    offset: 0,
                    size: 8,
                },
            }],
            fill_value: None,
            attributes: vec![],
            stats: None,
        });
        let mut bytes = vec![1u8, 2, 3, 4];
        bytes.extend(footer.serialize().unwrap());
        let storage = Arc::new(InMemoryStorage::from_bytes(bytes));
        let file = ArrayFile::open_storage(storage, Arc::from("mem"), None)
            .await
            .unwrap();
        assert!(matches!(
            file.read_array::<u8>("x", vec![], vec![]).await,
            Err(Error::InvalidFooter(_))
        ));
    }
}
