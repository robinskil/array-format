//! Builds an immutable file.
//!
//! Define arrays, write chunks, set attributes, then call
//! [`finish`](ArrayWriter::finish). That writes the data region and the
//! footer to storage in one pass and consumes the writer. Chunk bytes are
//! packed into compressed blocks in a temp file as they arrive, so memory use
//! does not grow with the data.

use std::collections::BTreeMap;
use std::sync::Arc;

use ecow::EcoString;
use indexmap::IndexMap;

use crate::{
    address::{BlockAllocAddress, ChunkAddress},
    array::ArrayElement,
    attr::{AttributeValue, DiskValue, StringPool},
    block_cache::BlockCache,
    block_writer::{BlockWriter, BlockWriterOutput},
    codec::CompressionCodec,
    dtype::DType,
    error::{Error, Result},
    footer::{ArrayMeta, FOOTER_VERSION, Footer},
    layout::{ChunkEntry, FillValue},
    nd,
    reader::{ArrayFile, ReadConfig},
    stats::{ArrayStats, StatValue, compute_chunk_partial, merge_partial},
    storage::{ObjectStoreBackend, Storage, write_file_then_bytes},
};

/// Default target size of a data block before it is compressed (8 MiB).
pub const DEFAULT_BLOCK_TARGET_SIZE: usize = 8 * 1024 * 1024;

/// Options for an [`ArrayWriter`].
pub struct WriterConfig<C: CompressionCodec> {
    /// Codec applied to every data block.
    pub codec: C,
    /// Bytes of raw chunk data packed into one block before it is compressed.
    pub block_target_size: usize,
}

impl<C: CompressionCodec> WriterConfig<C> {
    /// Creates a config with `codec` and the default block size.
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            block_target_size: DEFAULT_BLOCK_TARGET_SIZE,
        }
    }
}

/// One chunk written in this session: where its bytes went, and the partial
/// statistics [`finish`](ArrayWriter::finish) merges into the array total.
struct WrittenChunk {
    address: BlockAllocAddress,
    min: Option<StatValue>,
    max: Option<StatValue>,
    null_count: u64,
    row_count: u64,
}

/// An array under construction.
struct PendingArray {
    dtype: DType,
    shape: Vec<u32>,
    dimension_names: Vec<String>,
    chunk_shape: Vec<u32>,
    fill_value: Option<FillValue>,
    /// Keyed by coordinate, so `finish` emits the chunk table already sorted.
    /// A coordinate written twice keeps only the last write.
    chunks: BTreeMap<Vec<u32>, WrittenChunk>,
    /// Insertion order, so the footer is deterministic.
    attributes: IndexMap<EcoString, AttributeValue>,
}

/// Shape information the n-dimensional write path needs.
pub(crate) struct Schema {
    pub shape: Vec<u32>,
    pub chunk_shape: Vec<u32>,
    pub dtype: DType,
}

/// Builds an immutable array file. See the [module docs](self).
pub struct ArrayWriter {
    blocks: BlockWriter,
    /// Definition order is file order.
    arrays: IndexMap<String, PendingArray>,
}

impl ArrayWriter {
    /// Creates an empty writer. Blocks spill to a temp file as they fill.
    pub fn new<C: CompressionCodec + 'static>(config: WriterConfig<C>) -> Self {
        Self {
            blocks: BlockWriter::new(Arc::new(config.codec), config.block_target_size),
            arrays: IndexMap::new(),
        }
    }

    /// Defines a new array.
    ///
    /// `shape` is the full array shape; `chunk_shape` tiles it into a grid of
    /// independently stored chunks, or `None` to store the whole array as a
    /// single chunk. If `dimension_names` does not have one entry per
    /// dimension it is replaced with `dim0`, `dim1`, … . `fill_value` is
    /// returned for elements that are never written.
    ///
    /// An axis of length 0 is allowed. The array then holds no elements. The
    /// chunk extent of that axis is stored as 1, because an empty axis yields
    /// no chunks either way.
    ///
    /// Errors with [`Error::ArrayAlreadyExists`] if the name is taken, or with
    /// [`Error::InvalidChunkShape`] if `chunk_shape` has the wrong number of
    /// axes or a zero extent on a non-empty axis.
    pub fn define_array<T: ArrayElement>(
        &mut self,
        name: impl Into<String>,
        dimension_names: Vec<String>,
        shape: Vec<usize>,
        chunk_shape: Option<Vec<usize>>,
        fill_value: Option<FillValue>,
    ) -> Result<()> {
        self.define(
            name.into(),
            T::DTYPE,
            dimension_names,
            shape,
            chunk_shape,
            fill_value,
        )
    }

    /// Copies array `name` from `source` into this writer: its definition,
    /// every written chunk, and its attributes.
    ///
    /// Chunks are read decompressed and packed into this writer's blocks, so
    /// the copy uses this writer's codec and block size. Statistics are
    /// recomputed from the copied bytes.
    ///
    /// Errors with [`Error::ArrayNotFound`] if `source` has no such array, or
    /// [`Error::ArrayAlreadyExists`] if this writer already has one.
    pub async fn copy_array(&mut self, source: &ArrayFile, name: &str) -> Result<()> {
        let info = source.array(name).ok_or_else(|| Error::ArrayNotFound {
            name: name.to_string(),
        })?;
        self.define(
            name.to_string(),
            info.dtype.clone(),
            info.dimension_names.clone(),
            info.shape.iter().map(|&s| s as usize).collect(),
            Some(info.chunk_shape.iter().map(|&c| c as usize).collect()),
            info.fill_value.clone(),
        )?;
        for coord in info.written_chunks() {
            if let Some(bytes) = source.read_raw_chunk(info, coord).await? {
                self.write_chunk_raw(name, coord.to_vec(), &bytes)?;
            }
        }
        if let Some(attrs) = source.attributes(name) {
            // Sorted, so the footer does not depend on hash order.
            let mut entries: Vec<(&EcoString, &AttributeValue)> = attrs.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let pending = self.array_mut(name)?;
            for (key, value) in entries {
                pending.attributes.insert(key.clone(), value.clone());
            }
        }
        Ok(())
    }

    /// Defines an array from a runtime dtype. See
    /// [`define_array`](Self::define_array) for the rules.
    fn define(
        &mut self,
        name: String,
        dtype: DType,
        dimension_names: Vec<String>,
        shape: Vec<usize>,
        chunk_shape: Option<Vec<usize>>,
        fill_value: Option<FillValue>,
    ) -> Result<()> {
        if self.arrays.contains_key(&name) {
            return Err(Error::ArrayAlreadyExists { name });
        }
        let shape_u32: Vec<u32> = shape.iter().map(|&s| s as u32).collect();
        let ndim = shape_u32.len();
        let chunk_shape_u32: Vec<u32> = match chunk_shape {
            None => shape_u32.iter().map(|&s| s.max(1)).collect(),
            Some(cs) => {
                if cs.len() != ndim {
                    return Err(Error::InvalidChunkShape {
                        name,
                        reason: format!("{} axes, but the shape has {ndim}", cs.len()),
                    });
                }
                // A zero extent on an empty axis mirrors the shape, which is
                // what a NetCDF converter passes. Store 1 instead, so the grid
                // arithmetic stays defined. On a non-empty axis a zero extent
                // has no meaning, so reject it.
                let mut out = Vec::with_capacity(ndim);
                for (axis, (&c, &s)) in cs.iter().zip(&shape_u32).enumerate() {
                    let c = c as u32;
                    if c == 0 && s != 0 {
                        return Err(Error::InvalidChunkShape {
                            name,
                            reason: format!("axis {axis} has extent 0, but its length is {s}"),
                        });
                    }
                    out.push(c.max(1));
                }
                out
            }
        };
        let dimension_names = if dimension_names.len() == ndim {
            dimension_names
        } else {
            (0..ndim).map(|i| format!("dim{i}")).collect()
        };
        self.arrays.insert(
            name,
            PendingArray {
                dtype,
                shape: shape_u32,
                dimension_names,
                chunk_shape: chunk_shape_u32,
                fill_value,
                chunks: BTreeMap::new(),
                attributes: IndexMap::new(),
            },
        );
        Ok(())
    }

    /// Writes `data` into array `name` with its origin at coordinate `start`.
    ///
    /// The region may span multiple chunks and need not be chunk-aligned;
    /// partly covered chunks are read back, patched and written again. `T`
    /// must match the array's declared dtype, otherwise
    /// [`Error::DTypeMismatch`] is returned.
    pub fn write_array<T: ArrayElement>(
        &mut self,
        name: &str,
        start: Vec<usize>,
        data: ndarray::ArrayView<'_, T, ndarray::IxDyn>,
    ) -> Result<()> {
        nd::write_nd(self, name, data, &start)
    }

    /// Sets attribute `key` on array `name`, replacing any existing value.
    /// Errors if the array does not exist.
    pub fn set_attribute(&mut self, name: &str, key: &str, value: AttributeValue) -> Result<()> {
        self.array_mut(name)?
            .attributes
            .insert(EcoString::from(key), value);
        Ok(())
    }

    /// Writes the data region and the footer to `path` in `store`, consumes
    /// the writer, and returns the finished file open for reading.
    ///
    /// The reader is built from the footer in memory, so nothing is read
    /// back. It uses the default [`ReadConfig`]; open the path again for a
    /// shared cache.
    pub async fn finish(
        self,
        store: Arc<dyn object_store::ObjectStore>,
        path: object_store::path::Path,
    ) -> Result<ArrayFile> {
        let key: Arc<str> = Arc::from(path.as_ref());
        let storage: Arc<dyn Storage> = Arc::new(ObjectStoreBackend::new(store, path));
        self.finish_to(storage, key, ReadConfig::default().resolve_cache())
            .await
    }

    /// Writes the file to any [`Storage`] and opens it. `path` is only the
    /// reader's cache key.
    pub(crate) async fn finish_to(
        self,
        storage: Arc<dyn Storage>,
        path: Arc<str>,
        cache: Option<Arc<BlockCache>>,
    ) -> Result<ArrayFile> {
        let ArrayWriter { blocks, arrays } = self;
        let BlockWriterOutput {
            mut file,
            output_size,
            blocks,
        } = blocks.commit().await;

        let mut pool = StringPool::default();
        let mut metas = Vec::with_capacity(arrays.len());
        for (name, array) in arrays {
            // Every unwritten element counts as a null, so the total is the
            // shape product minus what the written chunks hold.
            let shape_product: u64 = array.shape.iter().map(|&s| s as u64).product();
            let mut stats = ArrayStats::new(name.clone());
            let mut written_non_null = 0u64;
            let mut chunks = Vec::with_capacity(array.chunks.len());
            for (coord, written) in array.chunks {
                written_non_null += written.row_count - written.null_count;
                merge_partial(
                    &mut stats,
                    written.min,
                    written.max,
                    written.null_count,
                    written.row_count,
                );
                chunks.push(ChunkEntry {
                    coord,
                    address: ChunkAddress::from(written.address),
                });
            }
            stats.row_count = shape_product;
            stats.null_count = shape_product - written_non_null;

            let attributes = array
                .attributes
                .iter()
                .map(|(key, value)| (pool.intern(key), DiskValue::encode(value, &mut pool)))
                .collect();

            metas.push(ArrayMeta {
                name,
                dtype: array.dtype,
                shape: array.shape,
                dimension_names: array.dimension_names,
                chunk_shape: array.chunk_shape,
                chunks,
                fill_value: array.fill_value,
                attributes,
                stats: Some(stats),
            });
        }

        let footer = Footer {
            version: FOOTER_VERSION,
            blocks,
            strings: pool.into_strings(),
            arrays: metas,
        };
        let footer_bytes = footer.serialize()?;
        write_file_then_bytes(&mut file, output_size, &footer_bytes, &*storage).await?;
        ArrayFile::from_footer(footer, storage, path, cache)
    }

    fn array(&self, name: &str) -> Result<&PendingArray> {
        self.arrays.get(name).ok_or_else(|| Error::ArrayNotFound {
            name: name.to_string(),
        })
    }

    fn array_mut(&mut self, name: &str) -> Result<&mut PendingArray> {
        self.arrays
            .get_mut(name)
            .ok_or_else(|| Error::ArrayNotFound {
                name: name.to_string(),
            })
    }

    pub(crate) fn schema(&self, name: &str) -> Result<Schema> {
        let array = self.array(name)?;
        Ok(Schema {
            shape: array.shape.clone(),
            chunk_shape: array.chunk_shape.clone(),
            dtype: array.dtype.clone(),
        })
    }

    /// Decodes the chunk at `coord`. An unwritten chunk is materialized from
    /// the fill value.
    pub(crate) fn read_chunk<T: ArrayElement>(&self, name: &str, coord: &[u32]) -> Result<Vec<T>> {
        let array = self.array(name)?;
        if let Some(written) = array.chunks.get(coord) {
            let bytes = self
                .blocks
                .fetch(&written.address)
                .ok_or(Error::BlockOutOfRange {
                    block_id: written.address.id().0,
                })?;
            return Ok(T::decode_chunk(&bytes));
        }
        // Edge chunks are clipped to the array shape.
        let len: usize = array
            .chunk_shape
            .iter()
            .enumerate()
            .map(|(i, &cs)| {
                let axis_len = array.shape[i] as usize;
                let start = coord[i] as usize * cs as usize;
                (cs as usize).min(axis_len.saturating_sub(start))
            })
            .product();
        Ok(vec![T::fill_element(array.fill_value.as_ref()); len])
    }

    /// Stores encoded chunk bytes and records their partial statistics.
    pub(crate) fn write_chunk_raw(
        &mut self,
        name: &str,
        coord: Vec<u32>,
        bytes: &[u8],
    ) -> Result<()> {
        let (dtype, fill_value) = {
            let array = self.array(name)?;
            (array.dtype.clone(), array.fill_value.clone())
        };
        let (min, max, null_count, row_count) =
            compute_chunk_partial(bytes, &dtype, fill_value.as_ref());
        let address = self.blocks.allocate(bytes);
        self.array_mut(name)?.chunks.insert(
            coord,
            WrittenChunk {
                address,
                min,
                max,
                null_count,
                row_count,
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ndarray::{Array, IxDyn};

    use super::*;
    use crate::codec::decompress_by_id;
    use crate::footer::{find_chunk, read_footer};
    use crate::stats::StatValue;
    use crate::storage::{InMemoryStorage, Storage};
    use crate::{NoCompression, ZstdCodec};

    fn writer() -> ArrayWriter {
        ArrayWriter::new(WriterConfig {
            block_target_size: 64,
            ..WriterConfig::new(NoCompression)
        })
    }

    /// Finishes into memory and returns the raw file plus its parsed footer,
    /// read back from the bytes, so these tests check what is on disk.
    async fn finish(w: ArrayWriter) -> (Vec<u8>, Footer) {
        let storage = Arc::new(InMemoryStorage::new());
        w.finish_to(storage.clone(), Arc::from("mem"), None)
            .await
            .unwrap();
        let footer = read_footer(&*storage).await.unwrap();
        let size = storage.size().await.unwrap();
        let raw = storage.read_range(0..size).await.unwrap().to_vec();
        (raw, footer)
    }

    /// Decodes one chunk straight from the raw file bytes.
    fn chunk_bytes(raw: &[u8], footer: &Footer, addr: &ChunkAddress) -> Vec<u8> {
        let block = &footer.blocks[addr.block_id.0 as usize];
        let range = block.file_range();
        let decompressed = decompress_by_id(
            &block.codec,
            &raw[range.start as usize..range.end as usize],
            block.uncompressed_size as usize,
        )
        .unwrap();
        let start = addr.offset as usize;
        decompressed[start..start + addr.size as usize].to_vec()
    }

    fn strings(footer: &Footer) -> Vec<EcoString> {
        footer.strings.iter().map(EcoString::from).collect()
    }

    #[tokio::test]
    async fn flat_array_roundtrips_through_finish() {
        let mut w = writer();
        w.define_array::<f32>("signal", vec!["t".into()], vec![4], None, None)
            .unwrap();
        let data = Array::from_vec(vec![1.0f32, 2.0, 3.0, 4.0]).into_dyn();
        w.write_array("signal", vec![0], data.view()).unwrap();

        let (raw, footer) = finish(w).await;
        assert_eq!(footer.version, FOOTER_VERSION);
        assert_eq!(footer.arrays.len(), 1);
        let meta = &footer.arrays[0];
        assert_eq!(meta.name, "signal");
        assert_eq!(meta.dtype, DType::Float32);
        assert_eq!(meta.shape, vec![4]);
        assert_eq!(meta.chunk_shape, vec![4]);
        assert_eq!(meta.dimension_names, vec!["t"]);
        assert_eq!(meta.chunks.len(), 1);
        assert_eq!(meta.chunks[0].coord, vec![0]);
        assert_eq!(
            chunk_bytes(&raw, &footer, &meta.chunks[0].address),
            f32::encode_chunk(&[1.0, 2.0, 3.0, 4.0])
        );
    }

    #[tokio::test]
    async fn compressed_blocks_roundtrip() {
        let mut w = ArrayWriter::new(WriterConfig {
            block_target_size: 64,
            ..WriterConfig::new(ZstdCodec { level: 3 })
        });
        w.define_array::<u8>("bytes", vec![], vec![256], Some(vec![64]), None)
            .unwrap();
        let values: Vec<u8> = (0..=255).collect();
        let data = Array::from_vec(values.clone()).into_dyn();
        w.write_array("bytes", vec![0], data.view()).unwrap();

        let (raw, footer) = finish(w).await;
        let meta = &footer.arrays[0];
        assert_eq!(meta.chunks.len(), 4);
        for (i, entry) in meta.chunks.iter().enumerate() {
            assert_eq!(entry.coord, vec![i as u32]);
            assert_eq!(
                chunk_bytes(&raw, &footer, &entry.address),
                &values[i * 64..(i + 1) * 64]
            );
        }
    }

    #[tokio::test]
    async fn partial_write_patches_the_existing_chunk() {
        let mut w = writer();
        w.define_array::<u8>("x", vec![], vec![8], Some(vec![4]), None)
            .unwrap();
        w.write_array(
            "x",
            vec![0],
            Array::from_vec(vec![1u8; 8]).into_dyn().view(),
        )
        .unwrap();
        w.write_array(
            "x",
            vec![2],
            Array::from_vec(vec![9u8; 4]).into_dyn().view(),
        )
        .unwrap();

        assert_eq!(w.read_chunk::<u8>("x", &[0]).unwrap(), vec![1, 1, 9, 9]);
        assert_eq!(w.read_chunk::<u8>("x", &[1]).unwrap(), vec![9, 9, 1, 1]);

        let (raw, footer) = finish(w).await;
        let meta = &footer.arrays[0];
        assert_eq!(meta.chunks.len(), 2, "a rewritten chunk keeps one entry");
        assert_eq!(
            chunk_bytes(&raw, &footer, &meta.chunks[0].address),
            [1, 1, 9, 9]
        );
        assert_eq!(
            chunk_bytes(&raw, &footer, &meta.chunks[1].address),
            [9, 9, 1, 1]
        );
    }

    #[tokio::test]
    async fn unwritten_chunk_reads_as_fill() {
        let mut w = writer();
        w.define_array::<i32>(
            "x",
            vec![],
            vec![5],
            Some(vec![2]),
            Some(FillValue::Int(-7)),
        )
        .unwrap();
        // The last chunk is clipped to one element.
        assert_eq!(w.read_chunk::<i32>("x", &[2]).unwrap(), vec![-7]);
        assert_eq!(w.read_chunk::<i32>("x", &[0]).unwrap(), vec![-7, -7]);
    }

    #[tokio::test]
    async fn chunks_are_sorted_by_coordinate() {
        let mut w = writer();
        w.define_array::<u8>("x", vec![], vec![12], Some(vec![4]), None)
            .unwrap();
        for start in [8usize, 0, 4] {
            let data = Array::from_vec(vec![start as u8; 4]).into_dyn();
            w.write_array("x", vec![start], data.view()).unwrap();
        }
        let (_, footer) = finish(w).await;
        let coords: Vec<_> = footer.arrays[0]
            .chunks
            .iter()
            .map(|e| e.coord.clone())
            .collect();
        assert_eq!(coords, vec![vec![0], vec![1], vec![2]]);
        assert!(find_chunk(&footer.arrays[0].chunks, &[2]).is_some());
    }

    #[tokio::test]
    async fn attributes_share_the_string_pool() {
        let mut w = writer();
        for name in ["a", "b"] {
            w.define_array::<f64>(name, vec![], vec![1], None, None)
                .unwrap();
            w.set_attribute(name, "units", AttributeValue::String("m/s".into()))
                .unwrap();
        }
        w.set_attribute("a", "scale", AttributeValue::Float64(0.5))
            .unwrap();
        w.set_attribute(
            "b",
            "tags",
            AttributeValue::StringList(Box::new(["units".into(), "x".into()])),
        )
        .unwrap();

        let (_, footer) = finish(w).await;
        // "units" appears as a key twice and inside a list; "m/s" twice.
        let mut pooled = footer.strings.clone();
        pooled.sort();
        assert_eq!(pooled, vec!["m/s", "scale", "tags", "units", "x"]);

        let strings = strings(&footer);
        let get = |array: usize, key: &str| {
            footer.arrays[array]
                .attributes
                .iter()
                .find(|(k, _)| strings[*k as usize] == key)
                .map(|(_, v)| v.decode(&strings).unwrap())
        };
        assert_eq!(get(0, "units"), Some(AttributeValue::String("m/s".into())));
        assert_eq!(get(1, "units"), Some(AttributeValue::String("m/s".into())));
        assert_eq!(get(0, "scale"), Some(AttributeValue::Float64(0.5)));
        assert_eq!(
            get(1, "tags"),
            Some(AttributeValue::StringList(Box::new([
                "units".into(),
                "x".into()
            ])))
        );
        assert_eq!(get(1, "scale"), None);
    }

    #[tokio::test]
    async fn overwritten_attribute_keeps_the_last_value() {
        let mut w = writer();
        w.define_array::<f64>("a", vec![], vec![1], None, None)
            .unwrap();
        w.set_attribute("a", "k", AttributeValue::Int64(1)).unwrap();
        w.set_attribute("a", "k", AttributeValue::Int64(2)).unwrap();
        let (_, footer) = finish(w).await;
        assert_eq!(footer.arrays[0].attributes.len(), 1);
        assert_eq!(footer.arrays[0].attributes[0].1, DiskValue::Int64(2));
    }

    #[tokio::test]
    async fn stats_cover_written_and_unwritten_chunks() {
        let mut w = writer();
        w.define_array::<i32>(
            "x",
            vec![],
            vec![6],
            Some(vec![3]),
            Some(FillValue::Int(-1)),
        )
        .unwrap();
        // Chunk 0 written, chunk 1 never written.
        let data = Array::from_vec(vec![5i32, 2, 7]).into_dyn();
        w.write_array("x", vec![0], data.view()).unwrap();

        let (_, footer) = finish(w).await;
        let stats = footer.arrays[0].stats.as_ref().unwrap();
        assert_eq!(stats.min, Some(StatValue::Int(2)));
        assert_eq!(stats.max, Some(StatValue::Int(7)));
        assert_eq!(stats.row_count, 6);
        assert_eq!(stats.null_count, 3, "the unwritten chunk is all nulls");
    }

    #[tokio::test]
    async fn stats_follow_a_chunk_overwrite() {
        let mut w = writer();
        w.define_array::<i32>("x", vec![], vec![2], None, None)
            .unwrap();
        w.write_array(
            "x",
            vec![0],
            Array::from_vec(vec![100i32, 200]).into_dyn().view(),
        )
        .unwrap();
        w.write_array(
            "x",
            vec![0],
            Array::from_vec(vec![1i32, 2]).into_dyn().view(),
        )
        .unwrap();
        let (_, footer) = finish(w).await;
        let stats = footer.arrays[0].stats.as_ref().unwrap();
        assert_eq!(
            stats.max,
            Some(StatValue::Int(2)),
            "old partial is replaced"
        );
        assert_eq!(stats.null_count, 0);
    }

    #[tokio::test]
    async fn empty_writer_finishes_to_an_empty_footer() {
        let (_, footer) = finish(writer()).await;
        assert!(footer.arrays.is_empty());
        assert!(footer.blocks.is_empty());
        assert!(footer.strings.is_empty());
    }

    #[tokio::test]
    async fn zero_length_axis_is_allowed() {
        let mut w = writer();
        w.define_array::<f32>(
            "h",
            vec!["n".into(), "x".into()],
            vec![0, 3],
            Some(vec![0, 3]),
            None,
        )
        .unwrap();
        let data = Array::from_shape_vec(IxDyn(&[0, 3]), Vec::<f32>::new()).unwrap();
        w.write_array("h", vec![0, 0], data.view()).unwrap();
        let (_, footer) = finish(w).await;
        let meta = &footer.arrays[0];
        assert_eq!(meta.shape, vec![0, 3]);
        assert_eq!(meta.chunk_shape, vec![1, 3], "zero extent is stored as 1");
        assert!(meta.chunks.is_empty());
        assert_eq!(meta.stats.as_ref().unwrap().row_count, 0);
    }

    // ── copy_array ───────────────────────────────────────────────────

    /// A source file with a partially written chunked array, attributes, a
    /// fill value, and a second plain array.
    async fn source_file() -> (ArrayFile, Arc<InMemoryStorage>) {
        let mut w = writer();
        w.define_array::<i32>(
            "grid",
            vec!["x".into()],
            vec![10],
            Some(vec![4]),
            Some(FillValue::Int(-1)),
        )
        .unwrap();
        // Chunk 0 full, chunk 1 untouched, chunk 2 clipped to two elements.
        w.write_array(
            "grid",
            vec![0],
            Array::from_vec(vec![1i32, 2, 3, 4]).into_dyn().view(),
        )
        .unwrap();
        w.write_array(
            "grid",
            vec![8],
            Array::from_vec(vec![9i32, 10]).into_dyn().view(),
        )
        .unwrap();
        w.set_attribute("grid", "units", AttributeValue::String("m".into()))
            .unwrap();
        w.set_attribute(
            "grid",
            "levels",
            AttributeValue::Int32List(Box::new([1, 2, 3])),
        )
        .unwrap();
        w.define_array::<u8>("flags", vec![], vec![3], None, None)
            .unwrap();
        w.write_array(
            "flags",
            vec![0],
            Array::from_vec(vec![7u8, 8, 9]).into_dyn().view(),
        )
        .unwrap();
        w.define_array::<f64>("unused", vec![], vec![2], None, None)
            .unwrap();

        let storage = Arc::new(InMemoryStorage::new());
        let file = w
            .finish_to(storage.clone(), Arc::from("src"), None)
            .await
            .unwrap();
        (file, storage)
    }

    #[tokio::test]
    async fn copy_array_carries_definition_chunks_attributes_and_stats() {
        let (source, _) = source_file().await;
        let mut w = writer();
        w.copy_array(&source, "grid").await.unwrap();
        let dest_storage = Arc::new(InMemoryStorage::new());
        let dest = w
            .finish_to(dest_storage, Arc::from("dst"), None)
            .await
            .unwrap();

        let (a, b) = (source.array("grid").unwrap(), dest.array("grid").unwrap());
        assert_eq!(a.dtype, b.dtype);
        assert_eq!(a.shape, b.shape);
        assert_eq!(a.chunk_shape, b.chunk_shape);
        assert_eq!(a.dimension_names, b.dimension_names);
        assert_eq!(a.fill_value, b.fill_value);
        assert_eq!(
            a.written_chunks().collect::<Vec<_>>(),
            b.written_chunks().collect::<Vec<_>>(),
            "only written chunks are copied; the fill is not materialized"
        );
        assert_eq!(a.stats, b.stats, "recomputed stats match the source");
        assert_eq!(source.attributes("grid"), dest.attributes("grid"));

        let from_dest = dest
            .read_array::<i32>("grid", vec![], vec![])
            .await
            .unwrap();
        assert_eq!(
            from_dest.iter().copied().collect::<Vec<_>>(),
            vec![1, 2, 3, 4, -1, -1, -1, -1, 9, 10]
        );
    }

    #[tokio::test]
    async fn edit_workflow_keeps_some_arrays_and_adds_one() {
        let (source, _) = source_file().await;
        let mut w = writer();
        for info in source.arrays() {
            if info.name != "unused" {
                w.copy_array(&source, &info.name).await.unwrap();
            }
        }
        w.define_array::<u8>("added", vec![], vec![2], None, None)
            .unwrap();
        w.write_array(
            "added",
            vec![0],
            Array::from_vec(vec![5u8, 6]).into_dyn().view(),
        )
        .unwrap();
        let dest = w
            .finish_to(Arc::new(InMemoryStorage::new()), Arc::from("dst"), None)
            .await
            .unwrap();

        let names: Vec<&str> = dest.arrays().iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, vec!["grid", "flags", "added"]);
        assert!(dest.array("unused").is_none());
        let flags = dest
            .read_array::<u8>("flags", vec![], vec![])
            .await
            .unwrap();
        assert_eq!(flags.iter().copied().collect::<Vec<_>>(), vec![7, 8, 9]);
    }

    #[tokio::test]
    async fn copied_chunks_are_repacked_with_the_writers_codec() {
        let (source, _) = source_file().await;
        let mut w = ArrayWriter::new(WriterConfig {
            block_target_size: 64,
            ..WriterConfig::new(ZstdCodec { level: 3 })
        });
        w.copy_array(&source, "grid").await.unwrap();
        let storage = Arc::new(InMemoryStorage::new());
        let dest = w
            .finish_to(storage.clone(), Arc::from("dst"), None)
            .await
            .unwrap();
        let footer = read_footer(&*storage).await.unwrap();
        assert!(!footer.blocks.is_empty());
        assert!(
            footer
                .blocks
                .iter()
                .all(|b| b.codec == crate::block::CodecId::Named("zstd".into())),
            "blocks carry the destination codec"
        );
        let grid = dest
            .read_array::<i32>("grid", vec![], vec![])
            .await
            .unwrap();
        assert_eq!(
            grid.iter().copied().collect::<Vec<_>>(),
            vec![1, 2, 3, 4, -1, -1, -1, -1, 9, 10]
        );
    }

    #[tokio::test]
    async fn copy_array_errors() {
        let (source, _) = source_file().await;
        let mut w = writer();
        assert!(matches!(
            w.copy_array(&source, "missing").await,
            Err(Error::ArrayNotFound { .. })
        ));
        w.copy_array(&source, "grid").await.unwrap();
        assert!(matches!(
            w.copy_array(&source, "grid").await,
            Err(Error::ArrayAlreadyExists { .. })
        ));
    }

    #[test]
    fn definition_errors() {
        let mut w = writer();
        w.define_array::<f32>("a", vec![], vec![4], None, None)
            .unwrap();
        assert!(matches!(
            w.define_array::<f32>("a", vec![], vec![4], None, None),
            Err(Error::ArrayAlreadyExists { .. })
        ));
        assert!(matches!(
            w.define_array::<f32>("b", vec![], vec![4, 4], Some(vec![2]), None),
            Err(Error::InvalidChunkShape { .. })
        ));
        assert!(matches!(
            w.define_array::<f32>("c", vec![], vec![4], Some(vec![0]), None),
            Err(Error::InvalidChunkShape { .. })
        ));
    }

    #[test]
    fn write_errors() {
        let mut w = writer();
        w.define_array::<f32>("a", vec![], vec![4], None, None)
            .unwrap();
        let f32s = Array::from_vec(vec![0.0f32; 4]).into_dyn();
        let i32s = Array::from_vec(vec![0i32; 4]).into_dyn();
        assert!(matches!(
            w.write_array("missing", vec![0], f32s.view()),
            Err(Error::ArrayNotFound { .. })
        ));
        assert!(matches!(
            w.write_array("a", vec![0], i32s.view()),
            Err(Error::DTypeMismatch { .. })
        ));
        assert!(
            w.write_array("a", vec![2], f32s.view()).is_err(),
            "region past the end"
        );
        assert!(matches!(
            w.set_attribute("missing", "k", AttributeValue::Bool(true)),
            Err(Error::ArrayNotFound { .. })
        ));
    }
}
