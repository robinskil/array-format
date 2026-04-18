//! Block identifiers and chunk address types.
//!
//! A [`ChunkAddress`] locates a slice of array data within a specific
//! block by `(block_id, offset, size)`.

use rkyv::{Archive, Deserialize, Serialize};

/// A block identifier within the file.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, Serialize, Deserialize,
)]
pub struct BlockId(pub u32);

/// Locates a contiguous slice of bytes within a block.
///
/// To read the data: find the block identified by [`block_id`](ChunkAddress::block_id),
/// seek to [`offset`](ChunkAddress::offset) bytes into the decompressed block,
/// and read [`size`](ChunkAddress::size) bytes.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct ChunkAddress {
    /// Which block the data resides in.
    pub block_id: BlockId,
    /// Byte offset within the decompressed block.
    pub offset: u32,
    /// Number of bytes to read from the block.
    pub size: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_id_copy() {
        let a = BlockId(7);
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn chunk_address_fields() {
        let addr = ChunkAddress {
            block_id: BlockId(3),
            offset: 1024,
            size: 4096,
        };
        assert_eq!(addr.block_id, BlockId(3));
        assert_eq!(addr.offset, 1024);
        assert_eq!(addr.size, 4096);
    }
}
