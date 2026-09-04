//! Layout primitives shared by the footer, writer and reader: the chunk table
//! entry and the fill value.

use rkyv::{Archive, Deserialize, Serialize};

use crate::address::ChunkAddress;

/// A single entry in the chunk table: a coordinate vector paired with its
/// storage address.
///
/// `coord` identifies the chunk within the array's grid (one index per
/// dimension). `address` locates the chunk's bytes within a compressed block.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct ChunkEntry {
    /// Chunk coordinate within the array's grid (one index per dimension).
    pub coord: Vec<u32>,
    /// Location of the chunk's bytes within a compressed block.
    pub address: ChunkAddress,
}

/// A scalar fill value for an array.
///
/// Represents the value used for unwritten or missing elements. Supports
/// numeric types and strings; complex types (Binary, List, FixedSizeList)
/// are not representable here.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum FillValue {
    /// Boolean fill value.
    Bool(bool),
    /// Signed integer fill value (for the signed integer dtypes).
    Int(i64),
    /// Unsigned integer fill value (for the unsigned integer dtypes).
    UInt(u64),
    /// Floating-point fill value (for `Float32`/`Float64`).
    Float(f64),
    /// String fill value.
    String(String),
    /// Fill value for [`DType::TimestampNs`](crate::DType::TimestampNs) arrays,
    /// interpreted as `i64` nanoseconds since the Unix epoch.
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
