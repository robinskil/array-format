//! # array-format
//!
//! A block-backed, footer-indexed container for storing many n-dimensional
//! arrays in a single file.
//!
//! The format uses a **delta/overlay architecture**: each flush produces a
//! self-describing sidecar file that stacks on top of the base, recording only
//! the chunks that changed. Reads fall through to older layers for unchanged
//! chunks, and layers can be merged back into a single file with
//! [`compact`](ArrayFile::compact).
//!
//! ## Features
//!
//! - Store many arrays in one object (or a small set of related sidecar files).
//! - Append arrays and update individual chunks without rewriting the whole file.
//! - Per-block compression (LZ4, Zstd, or none) recorded in the block table, so
//!   readers need no configuration to decode a file.
//! - Chunked or single-chunk layouts with coordinate-addressed reads.
//! - Logical deletes with periodic compaction to reclaim space.
//! - Works with any [`object_store`]-compatible backend (local filesystem,
//!   S3, GCS, Azure).
//!
//! ## Quick start
//!
//! ```
//! use array_format::{ArrayFile, FileConfig, Lz4Codec};
//! use ndarray::Array;
//!
//! # async fn example() -> array_format::Result<()> {
//! // An in-memory file; use `ArrayFile::create(store, path, config)` for on-disk.
//! let mut file = ArrayFile::create_memory(FileConfig::new(Lz4Codec)).await?;
//!
//! // Define and write a 1-D f32 array.
//! file.define_array::<f32>("signal", vec!["t".into()], vec![4], None, None)?;
//! let data = Array::from_vec(vec![1.0f32, 2.0, 3.0, 4.0]).into_dyn();
//! file.write_array("signal", vec![0], data.view()).await?;
//!
//! // Read it back — `vec![], vec![]` means "the whole array".
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
//! | 1 — Metadata | Array description | [`MergedArrayMeta`], [`FillValue`] |
//! | 2 — Codecs | Compression extension point | [`CompressionCodec`] |
//! | 3 — Runtime | Read / write / compact | [`ArrayFile`] |
//!
//! [`CompressionCodec`] is the extension point: implement it to plug in custom
//! compression algorithms. Storage is provided through any
//! [`object_store`]-compatible backend (passed to [`ArrayFile::create`]); for
//! tests and ephemeral use, [`ArrayFile::create_memory`] uses `object_store`'s
//! in-memory backend.
//!
//! [`ChunkAddress`]: address::ChunkAddress
//! [`BlockId`]: address::BlockId
//! [`object_store`]: https://docs.rs/object_store

#![warn(missing_docs)]

// ── Layer 0: Core types ─────────────────────────────────────────────
pub mod address;
mod delta;
pub mod dtype;
pub mod error;

// ── Layer 1: Metadata ───────────────────────────────────────────────
pub mod block;
mod footer;
pub mod layout;

// ── Layer 2: Codec extension trait ──────────────────────────────────
pub mod codec;
mod storage;

// ── Layer 3: Runtime ────────────────────────────────────────────────
pub mod array;
pub mod file;
pub mod stats;

mod ndarray_ext;
pub mod timestamp;

// ── Public re-exports ───────────────────────────────────────────────
pub use array::ArrayElement;
pub use codec::{CompressionCodec, Lz4Codec, NoCompression, ZstdCodec};
pub use delta::DeltaCache;
pub use dtype::DType;
pub use error::{Error, Result};
pub use file::{
    ArrayFile, DEFAULT_BLOCK_TARGET_SIZE, DEFAULT_CACHE_CAPACITY, DEFAULT_IO_CACHE_CAPACITY,
    FileConfig, MergedArrayMeta,
};
pub use layout::{AttributeValue, FillValue};
pub use stats::{ArrayStats, StatValue, StatsFile};
pub use timestamp::TimestampNs;
