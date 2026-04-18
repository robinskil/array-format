//! Block metadata stored in the footer.
//!
//! Each block in the data region has a [`BlockMeta`] entry that records
//! its physical location in the file and compression information.

use std::ops::Range;

use rkyv::{Archive, Deserialize, Serialize};

use crate::address::BlockId;

/// Identifies which compression codec was used for a block.
///
/// `None` means the block is stored uncompressed.
/// [`Named`](CodecId::Named) allows registering custom codecs by string
/// identifier without modifying this enum.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub enum CodecId {
    /// No compression applied.
    None,
    /// A named compression codec (e.g. `"zstd"`, `"lz4"`).
    Named(String),
}

/// Metadata for a single block in the data region.
///
/// Stored in the footer's block table. Describes where to find the
/// block in the file and how to decompress it.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct BlockMeta {
    /// Block identifier.
    pub id: BlockId,
    /// Byte offset of the compressed block within the file.
    pub file_offset: u64,
    /// Size of the block as stored on disk (compressed).
    pub compressed_size: u64,
    /// Size of the block after decompression.
    pub uncompressed_size: u64,
    /// Compression codec used for this block.
    pub codec: CodecId,
}

impl BlockMeta {
    /// Returns the byte range within the file that contains this block.
    pub fn file_range(&self) -> Range<u64> {
        self.file_offset..self.file_offset + self.compressed_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_range_computation() {
        let meta = BlockMeta {
            id: BlockId(0),
            file_offset: 100,
            compressed_size: 50,
            uncompressed_size: 200,
            codec: CodecId::None,
        };
        assert_eq!(meta.file_range(), 100..150);
    }

    #[test]
    fn named_codec_id() {
        let codec = CodecId::Named("zstd".into());
        assert_eq!(codec, CodecId::Named("zstd".into()));
        assert_ne!(codec, CodecId::None);
    }
}
