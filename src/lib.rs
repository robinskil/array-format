//! # array-format
//!
//! A block-backed, footer-indexed container for storing many n-dimensional
//! arrays in a single immutable file.
//!
//! A file is written once by an [`ArrayWriter`] and then only read, through
//! an [`ArrayFile`]. To change a file, open it, copy the arrays to keep into
//! a new writer with [`ArrayWriter::copy_array`], add the rest, and finish
//! to a new path.
//!
//! ## Features
//!
//! - Store many arrays in one object.
//! - Per-block compression (LZ4, Zstd, or none) recorded in the block table,
//!   so readers need no configuration to decode a file.
//! - Chunked or single-chunk layouts with coordinate-addressed reads.
//! - Per-array key-value attributes, in memory from the moment the file
//!   opens. One attribute across every array is a single call.
//! - Per-array statistics (min, max, null count, row count) computed while
//!   the file is written and stored in the footer.
//! - Works with any [`object_store`]-compatible backend (local filesystem,
//!   S3, GCS, Azure).
//!
//! ## Quick start
//!
//! ```
//! use std::sync::Arc;
//!
//! use array_format::{ArrayWriter, Lz4Codec, WriterConfig};
//! use ndarray::Array;
//! use object_store::memory::InMemory;
//!
//! # async fn example() -> array_format::Result<()> {
//! let store = Arc::new(InMemory::new());
//! let path = object_store::path::Path::from("signal.af");
//!
//! // Define and write a 1-D f32 array, then finish the file.
//! let mut writer = ArrayWriter::new(WriterConfig::new(Lz4Codec));
//! writer.define_array::<f32>("signal", vec!["t".into()], vec![4], None, None)?;
//! let data = Array::from_vec(vec![1.0f32, 2.0, 3.0, 4.0]).into_dyn();
//! writer.write_array("signal", vec![0], data.view())?;
//! let file = writer.finish(store, path).await?;
//!
//! // `finish` returns the file open for reading. `vec![], vec![]` reads it all.
//! let out = file.read_array::<f32>("signal", vec![], vec![]).await?;
//! assert_eq!(out.len(), 4);
//! # Ok(())
//! # }
//! ```
//!
//! ## Architecture
//!
//! The crate is organized in layers:
//!
//! | Layer | Purpose | Key types |
//! |-------|---------|-----------|
//! | 0 — Core | Primitives | [`DType`], [`ChunkAddress`], [`BlockId`], [`Error`] |
//! | 1 — Metadata | Array description | [`ArrayInfo`], [`AttributeValue`], [`FillValue`], [`ArrayStats`] |
//! | 2 — Codecs | Compression extension point | [`CompressionCodec`] |
//! | 3 — Runtime | Write / read | [`ArrayWriter`], [`ArrayFile`], [`BlockCache`] |
//!
//! [`CompressionCodec`] is the extension point: implement it to plug in custom
//! compression algorithms. Storage is provided through any
//! [`object_store`]-compatible backend, passed to [`ArrayWriter::finish`] and
//! [`ArrayFile::open`].
//!
//! ## Performance
//!
//! On an Apple M3 Pro, a file with 100K arrays and 2M attributes opens in
//! about 40 ms. Attributes are held key-major, so
//! [`attribute_column`](ArrayFile::attribute_column) across all 100K arrays
//! is a borrow, [`attribute_index`](ArrayFile::attribute_index) takes about
//! 0.15 ms, and one [`get_attribute`](ArrayFile::get_attribute) about 20 ns. Reading 25K whole
//! arrays (1 GB) with 32 concurrent tasks reaches 10–18 GiB/s. The README has
//! the full tables; `cargo bench` reproduces them.
//!
//! [`ChunkAddress`]: address::ChunkAddress
//! [`BlockId`]: address::BlockId
//! [`object_store`]: https://docs.rs/object_store

#![warn(missing_docs)]

// ── Layer 0: Core types ─────────────────────────────────────────────
pub mod address;
pub mod dtype;
pub mod error;

// ── Layer 1: Metadata ───────────────────────────────────────────────
pub mod attr;
pub mod block;
mod footer;
pub mod layout;
pub mod stats;

// ── Layer 2: Codec extension trait ──────────────────────────────────
pub mod codec;
mod storage;

// ── Layer 3: Runtime ────────────────────────────────────────────────
pub mod array;
mod block_cache;
mod block_writer;
mod nd;
pub mod reader;
pub mod timestamp;
pub mod writer;

// ── Public re-exports ───────────────────────────────────────────────
pub use array::ArrayElement;
pub use attr::AttributeValue;
pub use block_cache::BlockCache;
pub use codec::{CompressionCodec, Lz4Codec, NoCompression, ZstdCodec};
pub use dtype::DType;
pub use ecow::EcoString;
pub use error::{Error, Result};
pub use layout::FillValue;
pub use reader::{
    ArrayFile, ArrayInfo, DEFAULT_CACHE_CAPACITY, DEFAULT_IO_CACHE_CAPACITY, ReadConfig,
};
pub use stats::{ArrayStats, StatValue};
pub use timestamp::TimestampNs;
pub use writer::{ArrayWriter, DEFAULT_BLOCK_TARGET_SIZE, WriterConfig};
