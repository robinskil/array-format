//! Array layout definitions and array metadata.
//!
//! Each array stored in the file has an [`ArrayMeta`] entry in the footer
//! describing its type, dimensions, and how its data is laid out across blocks.

use rkyv::{Archive, Deserialize, Serialize};

use crate::address::ChunkAddress;
use crate::dtype::DType;

/// A scalar value for a per-array key-value attribute.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum AttributeValue {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
}

impl PartialEq for AttributeValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int8(a), Self::Int8(b)) => a == b,
            (Self::Int16(a), Self::Int16(b)) => a == b,
            (Self::Int32(a), Self::Int32(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::UInt8(a), Self::UInt8(b)) => a == b,
            (Self::UInt16(a), Self::UInt16(b)) => a == b,
            (Self::UInt32(a), Self::UInt32(b)) => a == b,
            (Self::UInt64(a), Self::UInt64(b)) => a == b,
            (Self::Float32(a), Self::Float32(b)) => a.to_bits() == b.to_bits(),
            (Self::Float64(a), Self::Float64(b)) => a.to_bits() == b.to_bits(),
            (Self::String(a), Self::String(b)) => a == b,
            _ => false,
        }
    }
}

/// Controls the integer width used for attribute key/value dictionary indices.
#[derive(Debug, Clone, Copy, PartialEq, Default, Archive, Serialize, Deserialize)]
pub enum AttrIndexKind {
    #[default]
    U16,
    U32,
    U64,
}

/// Per-array attribute entries as pairs of dictionary indices.
///
/// The variant determines the storage width per index. All entries are sorted
/// by key index to allow O(log n) binary search. Arrays in the same file may
/// use different variants; each is self-describing.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub enum Attributes {
    U16(Vec<(u16, u16)>),
    U32(Vec<(u32, u32)>),
    U64(Vec<(u64, u64)>),
}

impl Attributes {
    /// Returns an empty attribute set for the given index kind.
    pub fn empty(kind: AttrIndexKind) -> Self {
        match kind {
            AttrIndexKind::U16 => Self::U16(Vec::new()),
            AttrIndexKind::U32 => Self::U32(Vec::new()),
            AttrIndexKind::U64 => Self::U64(Vec::new()),
        }
    }

    /// Returns the value index for `key_idx`, or `None` if absent.
    pub fn get(&self, key_idx: usize) -> Option<usize> {
        match self {
            Self::U16(v) => v
                .binary_search_by_key(&(key_idx as u16), |(k, _)| *k)
                .ok()
                .map(|pos| v[pos].1 as usize),
            Self::U32(v) => v
                .binary_search_by_key(&(key_idx as u32), |(k, _)| *k)
                .ok()
                .map(|pos| v[pos].1 as usize),
            Self::U64(v) => v
                .binary_search_by_key(&(key_idx as u64), |(k, _)| *k)
                .ok()
                .map(|pos| v[pos].1 as usize),
        }
    }

    /// Inserts or replaces the value index for `key_idx`, keeping entries sorted.
    pub fn upsert(&mut self, key_idx: usize, val_idx: usize) {
        match self {
            Self::U16(v) => {
                let (ki, vi) = (key_idx as u16, val_idx as u16);
                match v.binary_search_by_key(&ki, |(k, _)| *k) {
                    Ok(pos) => v[pos].1 = vi,
                    Err(pos) => v.insert(pos, (ki, vi)),
                }
            }
            Self::U32(v) => {
                let (ki, vi) = (key_idx as u32, val_idx as u32);
                match v.binary_search_by_key(&ki, |(k, _)| *k) {
                    Ok(pos) => v[pos].1 = vi,
                    Err(pos) => v.insert(pos, (ki, vi)),
                }
            }
            Self::U64(v) => {
                let (ki, vi) = (key_idx as u64, val_idx as u64);
                match v.binary_search_by_key(&ki, |(k, _)| *k) {
                    Ok(pos) => v[pos].1 = vi,
                    Err(pos) => v.insert(pos, (ki, vi)),
                }
            }
        }
    }

    /// Iterates over all `(key_index, value_index)` pairs as `usize`.
    pub fn iter_entries(&self) -> Box<dyn Iterator<Item = (usize, usize)> + '_> {
        match self {
            Self::U16(v) => Box::new(v.iter().map(|&(k, v)| (k as usize, v as usize))),
            Self::U32(v) => Box::new(v.iter().map(|&(k, v)| (k as usize, v as usize))),
            Self::U64(v) => Box::new(v.iter().map(|&(k, v)| (k as usize, v as usize))),
        }
    }

    /// Returns the maximum index value that fits in this variant.
    pub fn max_index(&self) -> usize {
        match self {
            Self::U16(_) => u16::MAX as usize,
            Self::U32(_) => u32::MAX as usize,
            Self::U64(_) => usize::MAX,
        }
    }
}

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

/// A single entry in the chunk table: a coordinate vector paired with its
/// storage address.
///
/// `coord` identifies the chunk within the array's grid (one index per
/// dimension). `address` locates the chunk's bytes within a compressed block.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct ChunkEntry {
    pub coord: Vec<u32>,
    pub address: ChunkAddress,
}

/// Describes the physical storage strategy for array data.
///
/// All arrays are stored as a grid of chunks. A "flat" array is simply one
/// where `chunk_shape == shape`, yielding a single chunk at the origin.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct StorageLayout {
    /// The shape of each chunk in number of elements per dimension.
    pub chunk_shape: Vec<u32>,
    /// Mapping from chunk coordinates to their storage addresses.
    ///
    /// A layer sidecar carries only the chunks that changed; unchanged chunks
    /// fall through to lower layers.
    pub chunks: Vec<ChunkEntry>,
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
    /// Fill value for [`DType::TimestampNs`](crate::dtype::DType::TimestampNs)
    /// arrays — interpreted as `i64` nanoseconds since the Unix epoch.
    TimestampNs(i64),
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
            (Self::TimestampNs(a), Self::TimestampNs(b)) => a == b,
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
    /// Shape, dimension names, and physical chunk layout.
    pub layout: ArrayLayout,
    /// Fill value for unwritten or missing elements. `None` means zero/empty.
    pub fill_value: Option<FillValue>,
    /// Whether this array has been logically deleted.
    ///
    /// Deleted arrays are ignored during reads and removed during compaction.
    pub deleted: bool,
    /// User-defined key-value attributes.
    ///
    /// Each entry is `(key_index, value_index)` referencing `Footer::attr_keys`
    /// and `Footer::attr_values`. Entries are sorted by key index for O(log n)
    /// lookup. The `Attributes` variant controls the storage width per index.
    pub attributes: Attributes,
}

impl ArrayLayout {
    /// Looks up the [`ChunkAddress`] for a given chunk coordinate.
    pub fn get_chunk(&self, coord: &[u32]) -> Option<&ChunkAddress> {
        self.storage
            .chunks
            .iter()
            .find(|e| e.coord.as_slice() == coord)
            .map(|e| &e.address)
    }

    /// Returns all [`ChunkAddress`]es for this layout.
    pub fn all_addresses(&self) -> Vec<&ChunkAddress> {
        self.storage.chunks.iter().map(|e| &e.address).collect()
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

    fn chunk(coord: Vec<u32>, block: u32, offset: u32, size: u32) -> ChunkEntry {
        ChunkEntry {
            coord,
            address: sample_addr(block, offset, size),
        }
    }

    #[test]
    fn flat_layout_addresses() {
        let layout = ArrayLayout {
            shape: vec![100],
            dimension_names: vec!["x".into()],
            storage: StorageLayout {
                chunk_shape: vec![100],
                chunks: vec![chunk(vec![0], 0, 0, 400)],
            },
        };
        assert_eq!(layout.all_addresses().len(), 1);
        assert!(layout.get_chunk(&[0]).is_some());
        assert!(layout.get_chunk(&[1]).is_none());
    }

    #[test]
    fn chunked_layout_lookup() {
        let layout = ArrayLayout {
            shape: vec![128, 128],
            dimension_names: vec!["x".into(), "y".into()],
            storage: StorageLayout {
                chunk_shape: vec![64, 64],
                chunks: vec![
                    chunk(vec![0, 0], 3, 0, 1000),
                    chunk(vec![0, 1], 7, 2000, 1000),
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
            storage: StorageLayout {
                chunk_shape: vec![10],
                chunks: vec![
                    chunk(vec![0], 0, 0, 500),
                    chunk(vec![1], 0, 500, 500),
                    chunk(vec![2], 1, 0, 500),
                ],
            },
        };
        assert_eq!(layout.all_addresses().len(), 3);
    }
}
