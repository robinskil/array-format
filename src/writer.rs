//! Writer for creating and appending to array-format files.
//!
//! The [`Writer`] packs array data into blocks, compresses them, and
//! writes the data region followed by the footer to storage.
//!
//! For chunked arrays, [`Writer::begin_chunked_array`] returns a
//! [`ChunkedArrayWriter`] handle that borrows the writer and provides
//! a convenient builder-style interface for writing chunks.

use std::collections::HashMap;

use bytes::Bytes;

use crate::address::{BlockId, ChunkAddress};
use crate::array::ArrayData;
use crate::block::BlockMeta;
use crate::codec::CompressionCodec;
use crate::dtype::DType;
use crate::error::{Error, Result};
use crate::footer::Footer;
use crate::layout::{ArrayLayout, ArrayMeta};
use crate::storage::Storage;

/// Default block target size: 8 MiB.
pub const DEFAULT_BLOCK_TARGET_SIZE: usize = 8 * 1024 * 1024;

/// Configuration for the [`Writer`].
pub struct WriterConfig<C: CompressionCodec> {
    /// Target size in bytes for each block before compression.
    pub block_target_size: usize,
    /// Compression codec to use for new blocks.
    pub codec: C,
}

impl Default for WriterConfig<crate::codec::NoCompression> {
    fn default() -> Self {
        Self {
            block_target_size: DEFAULT_BLOCK_TARGET_SIZE,
            codec: crate::codec::NoCompression,
        }
    }
}

/// Writes arrays into an array-format file.
///
/// Data is accumulated into blocks up to the configured target size.
/// Call [`flush`](Writer::flush) to compress all pending blocks,
/// serialize the footer, and write the complete file to storage.
///
/// # Type Parameters
///
/// * `C` – compression codec (see [`CompressionCodec`])
pub struct Writer<C: CompressionCodec> {
    storage: Box<dyn Storage>,
    config: WriterConfig<C>,
    footer: Footer,
    /// In-progress block buffer (uncompressed bytes).
    current_block: Vec<u8>,
    /// Completed blocks: `(meta, compressed_bytes)`.
    finished_blocks: Vec<(BlockMeta, Vec<u8>)>,
    /// name → index into `footer.arrays` for non-deleted arrays.
    array_index: HashMap<String, usize>,
}

impl<C: CompressionCodec> Writer<C> {
    /// Creates a new writer that will produce a fresh file.
    pub fn new(storage: impl Storage + 'static, config: WriterConfig<C>) -> Self {
        Self {
            storage: Box::new(storage),
            config,
            footer: Footer::new(),
            current_block: Vec::new(),
            finished_blocks: Vec::new(),
            array_index: HashMap::new(),
        }
    }

    /// Opens an existing file and loads its footer so new arrays
    /// can be appended.
    pub async fn open(storage: impl Storage + 'static, config: WriterConfig<C>) -> Result<Self> {
        let storage: Box<dyn Storage> = Box::new(storage);
        let file_size = storage.size().await?;
        if file_size == 0 {
            return Ok(Self {
                storage,
                config,
                footer: Footer::new(),
                current_block: Vec::new(),
                finished_blocks: Vec::new(),
                array_index: HashMap::new(),
            });
        }

        let footer = crate::footer::read_footer(storage.as_ref()).await?;
        let array_index = build_writer_index(&footer.arrays);

        Ok(Self {
            storage,
            config,
            footer,
            current_block: Vec::new(),
            finished_blocks: Vec::new(),
            array_index,
        })
    }

    /// Writes a typed array.
    ///
    /// The dtype is taken from the [`ArrayData`] trait object.
    pub fn write_array(
        &mut self,
        name: &str,
        dimensions: Vec<String>,
        data: &dyn ArrayData,
    ) -> Result<()> {
        self.write_flat(name, data.dtype().clone(), dimensions, data.as_bytes())
    }

    /// Writes a flat (non-chunked) array from raw bytes.
    pub(crate) fn write_flat(
        &mut self,
        name: &str,
        dtype: DType,
        dimensions: Vec<String>,
        data: &[u8],
    ) -> Result<()> {
        if self.array_index.contains_key(name) {
            return Err(Error::ArrayAlreadyExists { name: name.into() });
        }

        let address = self.pack_into_block(data);
        let meta = ArrayMeta {
            name: name.to_string(),
            dtype,
            dimensions,
            layout: ArrayLayout::Flat { address },
            deleted: false,
        };
        let idx = self.footer.arrays.len();
        self.footer.arrays.push(meta);
        self.array_index.insert(name.to_string(), idx);
        Ok(())
    }

    /// Registers a new chunked array and returns a [`ChunkedArrayWriter`]
    /// that can be used to write chunks without repeating the array name.
    ///
    /// The returned handle mutably borrows `self`, preventing other writer
    /// operations until it is dropped.
    pub fn begin_chunked_array(
        &mut self,
        name: &str,
        dtype: DType,
        dimensions: Vec<String>,
        chunk_shape: Vec<u32>,
    ) -> Result<ChunkedArrayWriter<'_, C>> {
        if self.array_index.contains_key(name) {
            return Err(Error::ArrayAlreadyExists { name: name.into() });
        }

        let meta = ArrayMeta {
            name: name.to_string(),
            dtype,
            dimensions,
            layout: ArrayLayout::Chunked {
                chunk_shape,
                chunks: Vec::new(),
            },
            deleted: false,
        };
        let idx = self.footer.arrays.len();
        self.footer.arrays.push(meta);
        self.array_index.insert(name.to_string(), idx);
        Ok(ChunkedArrayWriter { writer: self, idx })
    }

    /// Logically deletes an array by setting its `deleted` flag.
    pub fn delete(&mut self, name: &str) -> Result<()> {
        let idx = self
            .array_index
            .remove(name)
            .ok_or_else(|| Error::ArrayNotFound { name: name.into() })?;
        self.footer.arrays[idx].deleted = true;
        Ok(())
    }

    /// Flushes all pending data and writes the complete file to storage.
    ///
    /// This finalizes the current block (if non-empty), compresses all
    /// blocks, serializes the footer with the trailer, and writes the
    /// entire file content as a single `Storage::write` call.
    pub async fn flush(&mut self) -> Result<()> {
        // Finalize the current block if it has data.
        self.finalize_current_block()?;

        // Build the file: data region + footer.
        let mut file_bytes: Vec<u8> = Vec::new();

        // Assign file offsets to all blocks and write compressed data.
        let mut block_metas: Vec<BlockMeta> = Vec::new();
        for (mut meta, compressed) in self.finished_blocks.drain(..) {
            meta.file_offset = file_bytes.len() as u64;
            meta.compressed_size = compressed.len() as u64;
            file_bytes.extend_from_slice(&compressed);
            block_metas.push(meta);
        }

        self.footer.blocks = block_metas;

        // Serialize and append footer.
        let footer_bytes = self.footer.serialize()?;
        file_bytes.extend_from_slice(&footer_bytes);

        self.storage.write(Bytes::from(file_bytes)).await?;
        Ok(())
    }

    // ── internal helpers ────────────────────────────────────────────

    fn write_chunk_at(&mut self, idx: usize, coord: Vec<u32>, data: &[u8]) -> Result<()> {
        let addr = self.pack_into_block(data);

        let array = &mut self.footer.arrays[idx];
        match &mut array.layout {
            ArrayLayout::Chunked { chunks, .. } => {
                chunks.push((coord, addr));
            }
            ArrayLayout::Flat { .. } => {
                return Err(Error::InvalidFooter(
                    "cannot write chunk to a flat array".into(),
                ));
            }
        }

        Ok(())
    }

    fn next_block_id(&self) -> BlockId {
        let existing = self.footer.blocks.len() + self.finished_blocks.len();
        BlockId(existing as u32)
    }

    fn finalize_current_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        let block_id = self.next_block_id();
        let uncompressed = std::mem::take(&mut self.current_block);
        let uncompressed_size = uncompressed.len() as u64;
        let compressed = self.config.codec.compress(&uncompressed)?;

        let meta = BlockMeta {
            id: block_id,
            file_offset: 0,     // assigned during flush
            compressed_size: 0, // assigned during flush
            uncompressed_size,
            codec: self.config.codec.id(),
        };

        self.finished_blocks.push((meta, compressed));
        Ok(())
    }

    /// Alignment for array data within a block.
    ///
    /// All payloads are padded to start at an 8-byte boundary so that
    /// the reader can reinterpret cached block slices as typed arrays
    /// without copying for alignment.
    const BLOCK_ALIGN: usize = 8;

    /// Appends data into the current block as a single contiguous entry.
    ///
    /// If the current block is non-empty and would exceed the target size,
    /// it is finalized first. The entire payload is then appended to the
    /// (possibly new) current block — the target size is a soft limit so
    /// a single array or chunk is never split across blocks.
    fn pack_into_block(&mut self, data: &[u8]) -> ChunkAddress {
        if !self.current_block.is_empty()
            && self.current_block.len() + data.len() > self.config.block_target_size
        {
            self.finalize_current_block()
                .expect("block finalization failed");
        }

        // Pad to 8-byte alignment so readers can zero-copy reinterpret.
        let misalign = self.current_block.len() % Self::BLOCK_ALIGN;
        if misalign != 0 {
            let padding = Self::BLOCK_ALIGN - misalign;
            self.current_block.extend(std::iter::repeat_n(0u8, padding));
        }

        let offset = self.current_block.len() as u32;
        self.current_block.extend_from_slice(data);
        let block_id = self.next_block_id();

        ChunkAddress {
            block_id,
            offset,
            size: data.len() as u32,
        }
    }
}

/// A handle for writing chunks into a single chunked array.
///
/// Obtained via [`Writer::begin_chunked_array`]. Mutably borrows the parent
/// [`Writer`] so no other arrays can be modified while chunks are being
/// written. The number of chunks written is tracked and accessible via
/// [`chunks_written`](ChunkedArrayWriter::chunks_written).
pub struct ChunkedArrayWriter<'w, C: CompressionCodec> {
    writer: &'w mut Writer<C>,
    idx: usize,
}

impl<C: CompressionCodec> ChunkedArrayWriter<'_, C> {
    /// Writes a single chunk at the given coordinates.
    pub fn write(&mut self, coord: Vec<u32>, data: &[u8]) -> Result<()> {
        self.writer.write_chunk_at(self.idx, coord, data)
    }

    /// Writes a single typed chunk at the given coordinates.
    ///
    /// Returns [`Error::DTypeMismatch`] if `data.dtype()` does not
    /// match the array's dtype.
    pub fn write_array(&mut self, coord: Vec<u32>, data: &dyn ArrayData) -> Result<()> {
        let expected = &self.writer.footer.arrays[self.idx].dtype;
        if expected != data.dtype() {
            return Err(Error::DTypeMismatch {
                expected: expected.clone(),
                actual: data.dtype().clone(),
            });
        }
        self.writer.write_chunk_at(self.idx, coord, data.as_bytes())
    }

    /// Returns how many chunks have been written so far.
    pub fn chunks_written(&self) -> usize {
        match &self.writer.footer.arrays[self.idx].layout {
            ArrayLayout::Chunked { chunks, .. } => chunks.len(),
            _ => 0,
        }
    }
}

fn build_writer_index(arrays: &[ArrayMeta]) -> HashMap<String, usize> {
    arrays
        .iter()
        .enumerate()
        .filter(|(_, a)| !a.deleted)
        .map(|(i, a)| (a.name.clone(), i))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::PrimitiveArray;
    use crate::codec::NoCompression;
    use crate::footer::{Footer, TRAILER_SIZE};
    use crate::storage::InMemoryStorage;

    fn small_config() -> WriterConfig<NoCompression> {
        WriterConfig {
            block_target_size: 64,
            codec: NoCompression,
        }
    }

    #[tokio::test]
    async fn write_flat_and_flush() {
        let storage = InMemoryStorage::new();
        let mut writer = Writer::new(storage.clone(), small_config());

        let data = vec![0x42u8; 100];
        writer
            .write_flat("arr", DType::UInt8, vec!["x".into()], &data)
            .unwrap();
        writer.flush().await.unwrap();

        // Verify footer can be read back.
        let file_size = storage.size().await.unwrap();
        assert!(file_size > TRAILER_SIZE as u64);
        let tail = storage.read_range(0..file_size).await.unwrap();
        let footer = Footer::deserialize(&tail).unwrap();
        assert_eq!(footer.arrays.len(), 1);
        assert_eq!(footer.arrays[0].name, "arr");
    }

    #[tokio::test]
    async fn write_typed_array() {
        let storage = InMemoryStorage::new();
        let mut writer = Writer::new(storage.clone(), small_config());

        let arr = PrimitiveArray::<f32>::from_slice(&[1.0, 2.0, 3.0]);
        writer
            .write_array("floats", vec!["x".into()], &arr)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = crate::reader::Reader::open(storage, 4096).await.unwrap();
        let result = reader.read_array::<f32>("floats").await.unwrap();
        assert_eq!(result.values(), &[1.0f32, 2.0, 3.0]);
    }

    #[tokio::test]
    async fn write_spans_multiple_blocks() {
        let storage = InMemoryStorage::new();
        let mut writer = Writer::new(storage.clone(), small_config());

        // Two arrays each larger than block_target_size (64 bytes).
        // The first should trigger a block flush before the second is written.
        let data_a = vec![0xAA; 100];
        let data_b = vec![0xBB; 120];
        writer
            .write_flat("a", DType::UInt8, vec!["x".into()], &data_a)
            .unwrap();
        writer
            .write_flat("b", DType::UInt8, vec!["x".into()], &data_b)
            .unwrap();
        writer.flush().await.unwrap();

        let file_size = storage.size().await.unwrap();
        let tail = storage.read_range(0..file_size).await.unwrap();
        let footer = Footer::deserialize(&tail).unwrap();

        // Should have two blocks (one per array since each exceeds target).
        assert_eq!(footer.blocks.len(), 2);

        // Each array has a single address covering its full size.
        if let ArrayLayout::Flat { address } = &footer.arrays[0].layout {
            assert_eq!(address.size, 100);
        } else {
            panic!("expected flat layout");
        }
        if let ArrayLayout::Flat { address } = &footer.arrays[1].layout {
            assert_eq!(address.size, 120);
        } else {
            panic!("expected flat layout");
        }
    }

    #[tokio::test]
    async fn duplicate_array_name_rejected() {
        let storage = InMemoryStorage::new();
        let mut writer = Writer::new(storage, small_config());

        writer
            .write_flat("arr", DType::UInt8, vec![], &[1, 2, 3])
            .unwrap();
        let err = writer
            .write_flat("arr", DType::UInt8, vec![], &[4, 5, 6])
            .unwrap_err();
        assert!(matches!(err, Error::ArrayAlreadyExists { .. }));
    }

    #[tokio::test]
    async fn delete_array() {
        let storage = InMemoryStorage::new();
        let mut writer = Writer::new(storage.clone(), small_config());

        writer
            .write_flat("a", DType::Int32, vec![], &[0; 16])
            .unwrap();
        writer.delete("a").unwrap();
        writer.flush().await.unwrap();

        let file_size = storage.size().await.unwrap();
        let tail = storage.read_range(0..file_size).await.unwrap();
        let footer = Footer::deserialize(&tail).unwrap();
        assert!(footer.arrays[0].deleted);
    }

    #[tokio::test]
    async fn chunked_array_writer_api() {
        let storage = InMemoryStorage::new();
        let mut writer = Writer::new(storage.clone(), small_config());

        {
            let mut chunked = writer
                .begin_chunked_array(
                    "grid",
                    DType::Float32,
                    vec!["x".into(), "y".into()],
                    vec![4, 4],
                )
                .unwrap();
            chunked.write(vec![0, 0], &[0xAA; 16]).unwrap();
            chunked.write(vec![0, 1], &[0xBB; 16]).unwrap();
            chunked.write(vec![1, 0], &[0xCC; 16]).unwrap();
            assert_eq!(chunked.chunks_written(), 3);
        }

        writer.flush().await.unwrap();

        let reader = crate::reader::Reader::open(storage, 4096).await.unwrap();

        let chunk = reader.read_chunk_raw("grid", &[0, 0]).await.unwrap();
        assert_eq!(&chunk[..], &[0xAA; 16]);
        let chunk = reader.read_chunk_raw("grid", &[1, 0]).await.unwrap();
        assert_eq!(&chunk[..], &[0xCC; 16]);
    }
}
