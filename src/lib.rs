//! # array-format
//!
//! A block-backed, footer-indexed container for storing multiple
//! n-dimensional arrays in a single file.
//!
//! ## Architecture
//!
//! The crate is organized in four layers:
//!
//! | Layer | Purpose | Key types |
//! |-------|---------|-----------|
//! | 0 — Core | Primitives | [`DType`], [`ChunkAddress`], [`BlockId`], [`Error`] |
//! | 1 — Metadata | Footer model | [`BlockMeta`], [`Footer`] |
//! | 2 — Traits | Extension points | [`CompressionCodec`], [`Storage`] |
//! | 3 — Runtime | Read / write / compact | [`File`] |
//!
//! The [`CompressionCodec`] and [`Storage`] traits allow plugging in
//! custom compression algorithms and storage backends.

// ── Layer 0: Core types ─────────────────────────────────────────────
pub mod address;
pub mod delta;
pub mod dtype;
pub mod error;

// ── Layer 1: Metadata ───────────────────────────────────────────────
pub mod block;
pub mod footer;
pub mod layout;

// ── Layer 2: Extension traits ───────────────────────────────────────
pub mod codec;
pub mod storage;

// ── Layer 3: Runtime ────────────────────────────────────────────────
pub mod array;
pub mod cache;
pub mod file;
pub mod stats;

pub mod ndarray_ext;

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
pub use storage::InMemoryStorage;
