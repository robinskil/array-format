//! Data type definitions for array elements.
//!
//! The format supports fixed-width primitives, variable-length values
//! (`String`, `Binary`), and list types (`FixedSizeList`, `List`).

use rkyv::{Archive, Deserialize, Serialize};

/// Describes the element type of an array.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(
    __C: rkyv::validation::ArchiveContext,
    <__C as rkyv::rancor::Fallible>::Error: rkyv::rancor::Source,
)))]
pub enum DType {
    /// Boolean stored as 1 byte (`0` or `1`).
    Bool,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer (little-endian).
    Int16,
    /// Signed 32-bit integer (little-endian).
    Int32,
    /// Signed 64-bit integer (little-endian).
    Int64,
    /// Unsigned 8-bit integer.
    UInt8,
    /// Unsigned 16-bit integer (little-endian).
    UInt16,
    /// Unsigned 32-bit integer (little-endian).
    UInt32,
    /// Unsigned 64-bit integer (little-endian).
    UInt64,
    /// 32-bit IEEE 754 floating point (little-endian).
    Float32,
    /// 64-bit IEEE 754 floating point (little-endian).
    Float64,
    /// Variable-length UTF-8 string.
    ///
    /// Encoded as an offsets buffer (`N + 1` entries) followed by a
    /// concatenated values buffer.
    String,
    /// Variable-length binary data (vlen bytes).
    ///
    /// Encoded identically to [`DType::String`] but without UTF-8 semantics.
    Binary,
    /// Fixed-size list where every element contains exactly `size` children.
    ///
    /// No offsets buffer is needed; child values are stored contiguously.
    FixedSizeList {
        /// Element type of the child values.
        #[rkyv(omit_bounds)]
        child: Box<DType>,
        /// Number of child values per list element.
        size: u32,
    },
    /// Variable-length list where each element can have a different number of children.
    ///
    /// Encoded with a parent offsets buffer (`N + 1` entries) and a concatenated
    /// child values buffer.
    List {
        /// Element type of the child values.
        #[rkyv(omit_bounds)]
        child: Box<DType>,
    },
}

impl DType {
    /// Returns the byte size of a single element for fixed-width types,
    /// or `None` for variable-length types.
    pub fn element_size(&self) -> Option<usize> {
        match self {
            DType::Bool | DType::Int8 | DType::UInt8 => Some(1),
            DType::Int16 | DType::UInt16 => Some(2),
            DType::Int32 | DType::UInt32 | DType::Float32 => Some(4),
            DType::Int64 | DType::UInt64 | DType::Float64 => Some(8),
            DType::FixedSizeList { child, size } => {
                child.element_size().map(|cs| cs * (*size as usize))
            }
            DType::String | DType::Binary | DType::List { .. } => None,
        }
    }

    /// Returns `true` if the type requires an offsets buffer
    /// (i.e. it is variable-length).
    pub fn is_variable_length(&self) -> bool {
        self.element_size().is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_element_sizes() {
        assert_eq!(DType::Bool.element_size(), Some(1));
        assert_eq!(DType::Int8.element_size(), Some(1));
        assert_eq!(DType::Int16.element_size(), Some(2));
        assert_eq!(DType::Int32.element_size(), Some(4));
        assert_eq!(DType::Int64.element_size(), Some(8));
        assert_eq!(DType::UInt8.element_size(), Some(1));
        assert_eq!(DType::UInt16.element_size(), Some(2));
        assert_eq!(DType::UInt32.element_size(), Some(4));
        assert_eq!(DType::UInt64.element_size(), Some(8));
        assert_eq!(DType::Float32.element_size(), Some(4));
        assert_eq!(DType::Float64.element_size(), Some(8));
    }

    #[test]
    fn variable_length_types() {
        assert!(DType::String.is_variable_length());
        assert!(DType::Binary.is_variable_length());
        assert!(
            DType::List {
                child: Box::new(DType::Int32)
            }
            .is_variable_length()
        );
    }

    #[test]
    fn fixed_size_list_element_size() {
        let dt = DType::FixedSizeList {
            child: Box::new(DType::Int16),
            size: 3,
        };
        assert_eq!(dt.element_size(), Some(6));
        assert!(!dt.is_variable_length());
    }

    #[test]
    fn nested_fixed_size_list() {
        let inner = DType::FixedSizeList {
            child: Box::new(DType::UInt8),
            size: 4,
        };
        let outer = DType::FixedSizeList {
            child: Box::new(inner),
            size: 2,
        };
        assert_eq!(outer.element_size(), Some(8));
    }

    #[test]
    fn fixed_size_list_with_vlen_child() {
        let dt = DType::FixedSizeList {
            child: Box::new(DType::String),
            size: 3,
        };
        assert!(dt.is_variable_length());
        assert_eq!(dt.element_size(), None);
    }
}
