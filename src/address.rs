//! Block identifiers and chunk address types.
//!
//! A [`ChunkAddress`] locates a slice of array data within a specific
//! block by `(block_id, offset, size)`.

use rkyv::{Archive, Deserialize, Serialize};

/// A block-relative allocation: which block, and the byte range within it.
///
/// The write-side counterpart of [`ChunkAddress`], using `u64` offsets/sizes
/// while a layer is being built; it narrows to [`ChunkAddress`] on flush.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct BlockAllocAddress {
    id: BlockId,
    offset: u64,
    size: u64,
}

/// A block identifier within the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
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

impl BlockAllocAddress {
    /// Creates an address for `size` bytes at `offset` within block `id`.
    pub(crate) fn new(id: BlockId, offset: u64, size: u64) -> Self {
        Self { id, offset, size }
    }

    /// The block this allocation lives in.
    pub(crate) fn id(&self) -> BlockId {
        self.id
    }

    /// Byte offset of the allocation within the block.
    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    /// Length of the allocation in bytes.
    pub(crate) fn size(&self) -> u64 {
        self.size
    }
}

impl From<BlockAllocAddress> for ChunkAddress {
    fn from(a: BlockAllocAddress) -> Self {
        ChunkAddress {
            block_id: a.id,
            offset: a.offset as u32,
            size: a.size as u32,
        }
    }
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
