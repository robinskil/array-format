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
//! | 1 — Metadata | Footer model | [`BlockMeta`], [`ArrayMeta`], [`ArrayLayout`], [`Footer`] |
//! | 2 — Traits | Extension points | [`CompressionCodec`], [`Storage`] |
//! | 3 — Runtime | Read / write / compact | [`Reader`], [`Writer`], [`compact()`] |
//!
//! The [`CompressionCodec`] and [`Storage`] traits allow plugging in
//! custom compression algorithms and storage backends.

// ── Layer 0: Core types ─────────────────────────────────────────────
pub mod address;
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
pub mod compact;
pub mod reader;
pub mod writer;

// ── Public re-exports ───────────────────────────────────────────────
pub use address::{BlockId, ChunkAddress};
pub use array::{
    ArrayData, BinaryArray, NativeType, PrimitiveArray, StringArray, from_bytes_dynamic,
};
pub use block::{BlockMeta, CodecId};
pub use cache::BlockCache;
pub use codec::{CompressionCodec, Lz4Codec, NoCompression, ZstdCodec, decompress_by_id};
pub use compact::compact;
pub use dtype::DType;
pub use error::{Error, Result};
pub use footer::{FOOTER_VERSION, Footer, MAGIC};
pub use layout::{ArrayLayout, ArrayMeta};
pub use reader::Reader;
pub use storage::{InMemoryStorage, ObjectStoreBackend, Storage};
pub use writer::{ChunkedArrayWriter, DEFAULT_BLOCK_TARGET_SIZE, Writer, WriterConfig};
