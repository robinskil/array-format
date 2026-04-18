//! Array layout definitions and array metadata.
//!
//! Each array stored in the file has an [`ArrayMeta`] entry in the footer
//! describing its type, dimensions, and how its data is laid out across blocks.

use rkyv::{Archive, Deserialize, Serialize};

use crate::address::ChunkAddress;
use crate::dtype::DType;

/// Describes how an array's data is distributed across blocks.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub enum ArrayLayout {
    /// The array is stored as a single contiguous payload within one block.
    Flat {
        /// Address of the payload within a block.
        address: ChunkAddress,
    },

    /// The array is divided into a grid of chunks, each independently
    /// addressable.
    Chunked {
        /// The shape of each chunk in number of elements per dimension.
        chunk_shape: Vec<u32>,
        /// Mapping from chunk coordinates to their storage address.
        ///
        /// Each entry is `(coordinate_vector, address)`.
        chunks: Vec<(Vec<u32>, ChunkAddress)>,
    },
}

/// Metadata for a single array stored in the file.
///
/// Stored in the footer's array table. Contains the schema, layout
/// information, and a logical deletion flag.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct ArrayMeta {
    /// Unique name of the array within the file.
    pub name: String,
    /// Element data type.
    pub dtype: DType,
    /// Dimension names (e.g. `["x", "y", "time"]`).
    pub dimensions: Vec<String>,
    /// How the array data is laid out across blocks.
    pub layout: ArrayLayout,
    /// Whether this array has been logically deleted.
    ///
    /// Deleted arrays are ignored during reads and removed during compaction.
    pub deleted: bool,
}

impl ArrayLayout {
    /// Looks up the [`ChunkAddress`] for a given chunk coordinate
    /// in a chunked layout. Returns `None` for flat layouts or if
    /// the coordinate is not found.
    pub fn get_chunk(&self, coord: &[u32]) -> Option<&ChunkAddress> {
        match self {
            ArrayLayout::Chunked { chunks, .. } => {
                chunks.iter().find(|(c, _)| c == coord).map(|(_, a)| a)
            }
            ArrayLayout::Flat { .. } => None,
        }
    }

    /// Returns all [`ChunkAddress`]es for this layout.
    pub fn all_addresses(&self) -> Vec<&ChunkAddress> {
        match self {
            ArrayLayout::Flat { address } => vec![address],
            ArrayLayout::Chunked { chunks, .. } => chunks.iter().map(|(_, a)| a).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::BlockId;

    fn sample_addr(block: u32, offset: u32, size: u32) -> ChunkAddress {
        ChunkAddress {
            block_id: BlockId(block),
            offset,
            size,
        }
    }

    #[test]
    fn flat_layout_addresses() {
        let layout = ArrayLayout::Flat {
            address: sample_addr(0, 0, 100),
        };
        assert_eq!(layout.all_addresses().len(), 1);
        assert!(layout.get_chunk(&[0, 0]).is_none());
    }

    #[test]
    fn chunked_layout_lookup() {
        let layout = ArrayLayout::Chunked {
            chunk_shape: vec![64, 64],
            chunks: vec![
                (vec![0, 0], sample_addr(3, 0, 1000)),
                (vec![0, 1], sample_addr(7, 2000, 1000)),
            ],
        };
        let addr = layout.get_chunk(&[0, 1]).unwrap();
        assert_eq!(addr.block_id, BlockId(7));
        assert_eq!(addr.offset, 2000);

        assert!(layout.get_chunk(&[1, 0]).is_none());
    }

    #[test]
    fn chunked_all_addresses() {
        let layout = ArrayLayout::Chunked {
            chunk_shape: vec![10],
            chunks: vec![
                (vec![0], sample_addr(0, 0, 500)),
                (vec![1], sample_addr(0, 500, 500)),
                (vec![2], sample_addr(1, 0, 500)),
            ],
        };
        assert_eq!(layout.all_addresses().len(), 3);
    }
}
