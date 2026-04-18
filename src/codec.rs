//! Compression codec trait and built-in implementations.
//!
//! The [`CompressionCodec`] trait allows plugging in different compression
//! algorithms. The footer records which codec was used per block via
//! [`CodecId`](crate::block::CodecId), so the reader must be configured
//! with a codec that can handle all codec ids present in the file.

use crate::block::CodecId;
use crate::error::{Error, Result};

/// A compression codec that can compress and decompress block data.
///
/// Implementations must be `Send + Sync` so they can be shared across
/// threads and async tasks.
///
/// # Extensibility
///
/// Implement this trait to add support for custom compression algorithms
/// (e.g. zstd, lz4, snappy). Register the codec by its
/// [`CodecId::Named`](crate::block::CodecId::Named) identifier.
pub trait CompressionCodec: Send + Sync {
    /// Returns the [`CodecId`] that identifies this codec in the footer.
    fn id(&self) -> CodecId;

    /// Compresses `data` and returns the compressed bytes.
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Decompresses `data` and returns the original bytes.
    fn decompress(&self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>>;
}

/// A no-op codec that stores blocks uncompressed.
///
/// This is the default codec used when no compression is configured.
#[derive(Debug, Clone, Copy)]
pub struct NoCompression;

impl CompressionCodec for NoCompression {
    fn id(&self) -> CodecId {
        CodecId::None
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8], _uncompressed_size: usize) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

/// Zstandard compression codec.
///
/// Uses a configurable compression level (default: 3).
#[derive(Debug, Clone)]
pub struct ZstdCodec {
    /// Zstd compression level (typically 1–22).
    pub level: i32,
}

impl ZstdCodec {
    /// Creates a new `ZstdCodec` with the given compression level.
    pub fn new(level: i32) -> Self {
        Self { level }
    }
}

impl Default for ZstdCodec {
    fn default() -> Self {
        Self { level: 3 }
    }
}

impl CompressionCodec for ZstdCodec {
    fn id(&self) -> CodecId {
        CodecId::Named("zstd".into())
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::bulk::compress(data, self.level).map_err(|e| Error::Codec(e.to_string()))
    }

    fn decompress(&self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
        zstd::bulk::decompress(data, uncompressed_size).map_err(|e| Error::Codec(e.to_string()))
    }
}

/// LZ4 compression codec using `lz4_flex`.
///
/// Provides fast compression and decompression at the cost of a slightly
/// lower compression ratio compared to zstd.
#[derive(Debug, Clone, Copy, Default)]
pub struct Lz4Codec;

impl CompressionCodec for Lz4Codec {
    fn id(&self) -> CodecId {
        CodecId::Named("lz4".into())
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(lz4_flex::compress_prepend_size(data))
    }

    fn decompress(&self, data: &[u8], _uncompressed_size: usize) -> Result<Vec<u8>> {
        lz4_flex::decompress_size_prepended(data).map_err(|e| Error::Codec(e.to_string()))
    }
}

/// Decompresses `data` by dispatching on the [`CodecId`] stored in the block footer.
///
/// This allows the reader to decompress blocks without requiring a statically
/// known codec — the codec is inferred from the block metadata at read time.
pub fn decompress_by_id(codec_id: &CodecId, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
    match codec_id {
        CodecId::None => NoCompression.decompress(data, uncompressed_size),
        CodecId::Named(name) => match name.as_str() {
            "zstd" => ZstdCodec::default().decompress(data, uncompressed_size),
            "lz4" => Lz4Codec.decompress(data, uncompressed_size),
            other => Err(Error::Codec(format!("unknown codec: {other}"))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_compression_roundtrip() {
        let codec = NoCompression;
        let data = b"hello world, this is a test payload";
        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn no_compression_id() {
        assert_eq!(NoCompression.id(), CodecId::None);
    }

    #[test]
    fn codec_is_object_safe() {
        // Verify the trait can be used as a trait object.
        let codec: Box<dyn CompressionCodec> = Box::new(NoCompression);
        assert_eq!(codec.id(), CodecId::None);
    }

    #[test]
    fn zstd_roundtrip() {
        let codec = ZstdCodec::default();
        let data = b"aaabbbccc repeated data for compression aaabbbccc";
        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn zstd_id() {
        assert_eq!(ZstdCodec::default().id(), CodecId::Named("zstd".into()));
    }

    #[test]
    fn lz4_roundtrip() {
        let codec = Lz4Codec;
        let data = b"aaabbbccc repeated data for compression aaabbbccc";
        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn lz4_id() {
        assert_eq!(Lz4Codec.id(), CodecId::Named("lz4".into()));
    }
}
