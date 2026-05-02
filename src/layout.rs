//! Array layout definitions and array metadata.
//!
//! Each array stored in the file has an [`ArrayMeta`] entry in the footer
//! describing its type, dimensions, and how its data is laid out across blocks.

use rkyv::{Archive, Deserialize, Serialize};

use crate::address::ChunkAddress;
use crate::dtype::DType;

/// Describes how an array's data is distributed across blocks.
///
/// Carries the array's global shape and dimension names so callers always
/// know the full n-dimensional structure, independent of how the data is
/// stored internally.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct ArrayLayout {
    /// Size of the array in each dimension (number of elements).
    pub shape: Vec<u32>,
    /// Name of each dimension (e.g. `["x", "y", "time"]`).
    pub dimension_names: Vec<String>,
    /// Storage strategy for the array data.
    pub storage: StorageLayout,
}

/// Describes the physical storage strategy for array data.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub enum StorageLayout {
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

/// A scalar fill value for an array.
///
/// Represents the value used for unwritten or missing elements. Supports
/// numeric types and strings; complex types (Binary, List, FixedSizeList)
/// are not representable here.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum FillValue {
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
}

impl PartialEq for FillValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::UInt(a), Self::UInt(b)) => a == b,
            // Compare by bit pattern so NaN == NaN.
            (Self::Float(a), Self::Float(b)) => a.to_bits() == b.to_bits(),
            (Self::String(a), Self::String(b)) => a == b,
            _ => false,
        }
    }
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
    /// How the array data is laid out across blocks (also carries shape and
    /// dimension names).
    pub layout: ArrayLayout,
    /// Fill value for unwritten or missing elements, if any.
    pub fill_value: Option<FillValue>,
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
        match &self.storage {
            StorageLayout::Chunked { chunks, .. } => {
                chunks.iter().find(|(c, _)| c == coord).map(|(_, a)| a)
            }
            StorageLayout::Flat { .. } => None,
        }
    }

    /// Returns all [`ChunkAddress`]es for this layout.
    pub fn all_addresses(&self) -> Vec<&ChunkAddress> {
        match &self.storage {
            StorageLayout::Flat { address } => vec![address],
            StorageLayout::Chunked { chunks, .. } => chunks.iter().map(|(_, a)| a).collect(),
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
        let layout = ArrayLayout {
            shape: vec![100],
            dimension_names: vec!["x".into()],
            storage: StorageLayout::Flat {
                address: sample_addr(0, 0, 100),
            },
        };
        assert_eq!(layout.all_addresses().len(), 1);
        assert!(layout.get_chunk(&[0, 0]).is_none());
    }

    #[test]
    fn chunked_layout_lookup() {
        let layout = ArrayLayout {
            shape: vec![128, 128],
            dimension_names: vec!["x".into(), "y".into()],
            storage: StorageLayout::Chunked {
                chunk_shape: vec![64, 64],
                chunks: vec![
                    (vec![0, 0], sample_addr(3, 0, 1000)),
                    (vec![0, 1], sample_addr(7, 2000, 1000)),
                ],
            },
        };
        let addr = layout.get_chunk(&[0, 1]).unwrap();
        assert_eq!(addr.block_id, BlockId(7));
        assert_eq!(addr.offset, 2000);

        assert!(layout.get_chunk(&[1, 0]).is_none());
    }

    #[test]
    fn chunked_all_addresses() {
        let layout = ArrayLayout {
            shape: vec![30],
            dimension_names: vec!["t".into()],
            storage: StorageLayout::Chunked {
                chunk_shape: vec![10],
                chunks: vec![
                    (vec![0], sample_addr(0, 0, 500)),
                    (vec![1], sample_addr(0, 500, 500)),
                    (vec![2], sample_addr(1, 0, 500)),
                ],
            },
        };
        assert_eq!(layout.all_addresses().len(), 3);
    }
}
