//! Attribute values: the in-memory form and the on-disk form.
//!
//! [`AttributeValue`] is what callers see. Strings are [`EcoString`], which is
//! 16 bytes, holds up to 15 bytes inline, and shares longer text on clone.
//! Lists are boxed slices. Together that keeps the enum at 24 bytes.
//!
//! [`DiskValue`] is the same enum as stored in the footer. Its strings are
//! indices into the footer's string pool, so a value that many arrays share is
//! written once. Every other variant is stored inline.

use ecow::EcoString;
use indexmap::IndexSet;
use rkyv::{Archive, Deserialize, Serialize};

use crate::error::{Error, Result};

/// A value attached to an array under a string key.
#[derive(Debug, Clone)]
pub enum AttributeValue {
    /// Boolean value.
    Bool(bool),
    /// Signed 8-bit integer.
    Int8(i8),
    /// Signed 16-bit integer.
    Int16(i16),
    /// Signed 32-bit integer.
    Int32(i32),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Unsigned 8-bit integer.
    UInt8(u8),
    /// Unsigned 16-bit integer.
    UInt16(u16),
    /// Unsigned 32-bit integer.
    UInt32(u32),
    /// Unsigned 64-bit integer.
    UInt64(u64),
    /// 32-bit floating point.
    Float32(f32),
    /// 64-bit floating point.
    Float64(f64),
    /// UTF-8 string.
    String(EcoString),
    /// Variable-length binary data.
    Binary(Box<[u8]>),
    /// List of boolean values.
    BoolList(Box<[bool]>),
    /// List of signed 8-bit integers.
    Int8List(Box<[i8]>),
    /// List of signed 16-bit integers.
    Int16List(Box<[i16]>),
    /// List of signed 32-bit integers.
    Int32List(Box<[i32]>),
    /// List of signed 64-bit integers.
    Int64List(Box<[i64]>),
    /// List of unsigned 8-bit integers.
    UInt8List(Box<[u8]>),
    /// List of unsigned 16-bit integers.
    UInt16List(Box<[u16]>),
    /// List of unsigned 32-bit integers.
    UInt32List(Box<[u32]>),
    /// List of unsigned 64-bit integers.
    UInt64List(Box<[u64]>),
    /// List of 32-bit floating point values.
    Float32List(Box<[f32]>),
    /// List of 64-bit floating point values.
    Float64List(Box<[f64]>),
    /// List of UTF-8 strings.
    StringList(Box<[EcoString]>),
    /// List of variable-length binary values.
    BinaryList(Box<[Box<[u8]>]>),
}

fn f32_bits_eq(a: &[f32], b: &[f32]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

fn f64_bits_eq(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

/// Bit-exact equality. Floats compare by bit pattern, so `NaN == NaN` and
/// `0.0 != -0.0`. That keeps the relation reflexive, which [`Eq`] and
/// [`Hash`](std::hash::Hash) require.
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
            (Self::Binary(a), Self::Binary(b)) => a == b,
            (Self::BoolList(a), Self::BoolList(b)) => a == b,
            (Self::Int8List(a), Self::Int8List(b)) => a == b,
            (Self::Int16List(a), Self::Int16List(b)) => a == b,
            (Self::Int32List(a), Self::Int32List(b)) => a == b,
            (Self::Int64List(a), Self::Int64List(b)) => a == b,
            (Self::UInt8List(a), Self::UInt8List(b)) => a == b,
            (Self::UInt16List(a), Self::UInt16List(b)) => a == b,
            (Self::UInt32List(a), Self::UInt32List(b)) => a == b,
            (Self::UInt64List(a), Self::UInt64List(b)) => a == b,
            (Self::Float32List(a), Self::Float32List(b)) => f32_bits_eq(a, b),
            (Self::Float64List(a), Self::Float64List(b)) => f64_bits_eq(a, b),
            (Self::StringList(a), Self::StringList(b)) => a == b,
            (Self::BinaryList(a), Self::BinaryList(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for AttributeValue {}

impl std::hash::Hash for AttributeValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Bool(v) => v.hash(state),
            Self::Int8(v) => v.hash(state),
            Self::Int16(v) => v.hash(state),
            Self::Int32(v) => v.hash(state),
            Self::Int64(v) => v.hash(state),
            Self::UInt8(v) => v.hash(state),
            Self::UInt16(v) => v.hash(state),
            Self::UInt32(v) => v.hash(state),
            Self::UInt64(v) => v.hash(state),
            Self::Float32(v) => v.to_bits().hash(state),
            Self::Float64(v) => v.to_bits().hash(state),
            Self::String(v) => v.hash(state),
            Self::Binary(v) => v.hash(state),
            Self::BoolList(v) => v.hash(state),
            Self::Int8List(v) => v.hash(state),
            Self::Int16List(v) => v.hash(state),
            Self::Int32List(v) => v.hash(state),
            Self::Int64List(v) => v.hash(state),
            Self::UInt8List(v) => v.hash(state),
            Self::UInt16List(v) => v.hash(state),
            Self::UInt32List(v) => v.hash(state),
            Self::UInt64List(v) => v.hash(state),
            Self::Float32List(v) => {
                for x in v.iter() {
                    x.to_bits().hash(state);
                }
            }
            Self::Float64List(v) => {
                for x in v.iter() {
                    x.to_bits().hash(state);
                }
            }
            Self::StringList(v) => v.hash(state),
            Self::BinaryList(v) => v.hash(state),
        }
    }
}

/// The on-disk form of [`AttributeValue`].
///
/// Same variants, except `String` and `StringList` hold indices into the
/// footer's string pool.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum DiskValue {
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
    String(u32),
    Binary(Box<[u8]>),
    BoolList(Box<[bool]>),
    Int8List(Box<[i8]>),
    Int16List(Box<[i16]>),
    Int32List(Box<[i32]>),
    Int64List(Box<[i64]>),
    UInt8List(Box<[u8]>),
    UInt16List(Box<[u16]>),
    UInt32List(Box<[u32]>),
    UInt64List(Box<[u64]>),
    Float32List(Box<[f32]>),
    Float64List(Box<[f64]>),
    StringList(Box<[u32]>),
    BinaryList(Box<[Box<[u8]>]>),
}

/// Interns strings while a footer is built. Insertion order is the on-disk
/// index order.
#[derive(Debug, Default)]
pub(crate) struct StringPool {
    strings: IndexSet<String>,
}

impl StringPool {
    /// Returns the index of `s`, adding it to the pool if absent.
    pub(crate) fn intern(&mut self, s: &str) -> u32 {
        if let Some(i) = self.strings.get_index_of(s) {
            return i as u32;
        }
        let (i, _) = self.strings.insert_full(s.to_owned());
        i as u32
    }

    /// Number of distinct strings in the pool.
    pub(crate) fn len(&self) -> usize {
        self.strings.len()
    }

    /// Consumes the pool and returns the strings in index order.
    pub(crate) fn into_strings(self) -> Vec<String> {
        self.strings.into_iter().collect()
    }
}

fn resolve(strings: &[EcoString], index: u32) -> Result<EcoString> {
    strings.get(index as usize).cloned().ok_or_else(|| {
        Error::InvalidFooter(format!(
            "string index {index} out of range, pool has {} strings",
            strings.len()
        ))
    })
}

impl DiskValue {
    /// Encodes `value`, interning its strings into `pool`.
    pub(crate) fn encode(value: &AttributeValue, pool: &mut StringPool) -> Self {
        match value {
            AttributeValue::Bool(v) => Self::Bool(*v),
            AttributeValue::Int8(v) => Self::Int8(*v),
            AttributeValue::Int16(v) => Self::Int16(*v),
            AttributeValue::Int32(v) => Self::Int32(*v),
            AttributeValue::Int64(v) => Self::Int64(*v),
            AttributeValue::UInt8(v) => Self::UInt8(*v),
            AttributeValue::UInt16(v) => Self::UInt16(*v),
            AttributeValue::UInt32(v) => Self::UInt32(*v),
            AttributeValue::UInt64(v) => Self::UInt64(*v),
            AttributeValue::Float32(v) => Self::Float32(*v),
            AttributeValue::Float64(v) => Self::Float64(*v),
            AttributeValue::String(v) => Self::String(pool.intern(v)),
            AttributeValue::Binary(v) => Self::Binary(v.clone()),
            AttributeValue::BoolList(v) => Self::BoolList(v.clone()),
            AttributeValue::Int8List(v) => Self::Int8List(v.clone()),
            AttributeValue::Int16List(v) => Self::Int16List(v.clone()),
            AttributeValue::Int32List(v) => Self::Int32List(v.clone()),
            AttributeValue::Int64List(v) => Self::Int64List(v.clone()),
            AttributeValue::UInt8List(v) => Self::UInt8List(v.clone()),
            AttributeValue::UInt16List(v) => Self::UInt16List(v.clone()),
            AttributeValue::UInt32List(v) => Self::UInt32List(v.clone()),
            AttributeValue::UInt64List(v) => Self::UInt64List(v.clone()),
            AttributeValue::Float32List(v) => Self::Float32List(v.clone()),
            AttributeValue::Float64List(v) => Self::Float64List(v.clone()),
            AttributeValue::StringList(v) => {
                Self::StringList(v.iter().map(|s| pool.intern(s)).collect())
            }
            AttributeValue::BinaryList(v) => Self::BinaryList(v.clone()),
        }
    }

    /// Decodes into an in-memory value, resolving string indices against
    /// `strings`. Errors if an index is out of range.
    pub(crate) fn decode(&self, strings: &[EcoString]) -> Result<AttributeValue> {
        Ok(match self {
            Self::Bool(v) => AttributeValue::Bool(*v),
            Self::Int8(v) => AttributeValue::Int8(*v),
            Self::Int16(v) => AttributeValue::Int16(*v),
            Self::Int32(v) => AttributeValue::Int32(*v),
            Self::Int64(v) => AttributeValue::Int64(*v),
            Self::UInt8(v) => AttributeValue::UInt8(*v),
            Self::UInt16(v) => AttributeValue::UInt16(*v),
            Self::UInt32(v) => AttributeValue::UInt32(*v),
            Self::UInt64(v) => AttributeValue::UInt64(*v),
            Self::Float32(v) => AttributeValue::Float32(*v),
            Self::Float64(v) => AttributeValue::Float64(*v),
            Self::String(i) => AttributeValue::String(resolve(strings, *i)?),
            Self::Binary(v) => AttributeValue::Binary(v.clone()),
            Self::BoolList(v) => AttributeValue::BoolList(v.clone()),
            Self::Int8List(v) => AttributeValue::Int8List(v.clone()),
            Self::Int16List(v) => AttributeValue::Int16List(v.clone()),
            Self::Int32List(v) => AttributeValue::Int32List(v.clone()),
            Self::Int64List(v) => AttributeValue::Int64List(v.clone()),
            Self::UInt8List(v) => AttributeValue::UInt8List(v.clone()),
            Self::UInt16List(v) => AttributeValue::UInt16List(v.clone()),
            Self::UInt32List(v) => AttributeValue::UInt32List(v.clone()),
            Self::UInt64List(v) => AttributeValue::UInt64List(v.clone()),
            Self::Float32List(v) => AttributeValue::Float32List(v.clone()),
            Self::Float64List(v) => AttributeValue::Float64List(v.clone()),
            Self::StringList(v) => AttributeValue::StringList(
                v.iter()
                    .map(|&i| resolve(strings, i))
                    .collect::<Result<Box<[EcoString]>>>()?,
            ),
            Self::BinaryList(v) => AttributeValue::BinaryList(v.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::mem::size_of;

    use super::*;

    fn hash_of(v: &AttributeValue) -> u64 {
        let mut h = DefaultHasher::new();
        v.hash(&mut h);
        h.finish()
    }

    fn pool_strings(pool: StringPool) -> Vec<EcoString> {
        pool.into_strings()
            .into_iter()
            .map(EcoString::from)
            .collect()
    }

    /// The whole design rests on these two sizes.
    #[test]
    fn sizes_match_the_memory_design() {
        assert_eq!(size_of::<EcoString>(), 16);
        assert_eq!(size_of::<AttributeValue>(), 24);
    }

    #[test]
    fn floats_compare_by_bits() {
        assert_eq!(
            AttributeValue::Float64(f64::NAN),
            AttributeValue::Float64(f64::NAN)
        );
        assert_ne!(AttributeValue::Float32(0.0), AttributeValue::Float32(-0.0));
        assert_eq!(
            AttributeValue::Float64List(Box::new([f64::NAN, 1.0])),
            AttributeValue::Float64List(Box::new([f64::NAN, 1.0]))
        );
        assert_ne!(
            AttributeValue::Float64List(Box::new([1.0, 2.0])),
            AttributeValue::Float64List(Box::new([1.0]))
        );
    }

    #[test]
    fn different_variants_with_equal_payloads_differ() {
        assert_ne!(AttributeValue::Int32(1), AttributeValue::Int64(1));
        assert_ne!(
            AttributeValue::Int32List(Box::new([1])),
            AttributeValue::Int64List(Box::new([1]))
        );
        assert_ne!(
            hash_of(&AttributeValue::Int32(1)),
            hash_of(&AttributeValue::Int64(1))
        );
    }

    #[test]
    fn equal_values_hash_equal() {
        let pairs = [
            (
                AttributeValue::Float64(f64::NAN),
                AttributeValue::Float64(f64::NAN),
            ),
            (
                AttributeValue::String("m/s".into()),
                AttributeValue::String("m/s".into()),
            ),
            (
                AttributeValue::StringList(Box::new(["a".into(), "b".into()])),
                AttributeValue::StringList(Box::new(["a".into(), "b".into()])),
            ),
            (
                AttributeValue::BinaryList(Box::new([
                    Box::new([0u8]) as Box<[u8]>,
                    Box::new([1, 2]),
                ])),
                AttributeValue::BinaryList(Box::new([
                    Box::new([0u8]) as Box<[u8]>,
                    Box::new([1, 2]),
                ])),
            ),
        ];
        for (a, b) in pairs {
            assert_eq!(a, b);
            assert_eq!(hash_of(&a), hash_of(&b));
        }
    }

    #[test]
    fn every_variant_roundtrips_through_the_pool() {
        let values = vec![
            AttributeValue::Bool(true),
            AttributeValue::Int8(-8),
            AttributeValue::Int16(-16),
            AttributeValue::Int32(-32),
            AttributeValue::Int64(-64),
            AttributeValue::UInt8(8),
            AttributeValue::UInt16(16),
            AttributeValue::UInt32(32),
            AttributeValue::UInt64(64),
            AttributeValue::Float32(f32::NAN),
            AttributeValue::Float64(-0.0),
            AttributeValue::String("a string longer than fifteen bytes".into()),
            AttributeValue::Binary(Box::new([0xde, 0xad])),
            AttributeValue::BoolList(Box::new([true, false])),
            AttributeValue::Int8List(Box::new([-1, 1])),
            AttributeValue::Int16List(Box::new([-1, 1])),
            AttributeValue::Int32List(Box::new([-1, 1])),
            AttributeValue::Int64List(Box::new([-1, 1])),
            AttributeValue::UInt8List(Box::new([1, 2])),
            AttributeValue::UInt16List(Box::new([1, 2])),
            AttributeValue::UInt32List(Box::new([1, 2])),
            AttributeValue::UInt64List(Box::new([1, 2])),
            AttributeValue::Float32List(Box::new([f32::NAN, 0.5])),
            AttributeValue::Float64List(Box::new([f64::NAN, 0.5])),
            AttributeValue::StringList(Box::new(["x".into(), "y".into(), "x".into()])),
            AttributeValue::BinaryList(Box::new([Box::new([9u8]) as Box<[u8]>, Box::new([])])),
        ];
        let mut pool = StringPool::default();
        let encoded: Vec<DiskValue> = values
            .iter()
            .map(|v| DiskValue::encode(v, &mut pool))
            .collect();
        let strings = pool_strings(pool);
        for (value, disk) in values.iter().zip(&encoded) {
            assert_eq!(&disk.decode(&strings).unwrap(), value, "{value:?}");
        }
    }

    #[test]
    fn pool_stores_each_string_once() {
        let mut pool = StringPool::default();
        let key = pool.intern("units");
        let a = DiskValue::encode(&AttributeValue::String("units".into()), &mut pool);
        let b = DiskValue::encode(
            &AttributeValue::StringList(Box::new(["units".into(), "m/s".into(), "units".into()])),
            &mut pool,
        );
        assert_eq!(a, DiskValue::String(key));
        assert_eq!(b, DiskValue::StringList(Box::new([key, 1, key])));
        assert_eq!(pool.len(), 2);
        assert_eq!(
            pool.into_strings(),
            vec!["units".to_string(), "m/s".to_string()]
        );
    }

    #[test]
    fn out_of_range_string_index_is_an_error() {
        let strings: Vec<EcoString> = vec!["only".into()];
        assert!(DiskValue::String(1).decode(&strings).is_err());
        assert!(
            DiskValue::StringList(Box::new([0, 7]))
                .decode(&strings)
                .is_err()
        );
        assert!(DiskValue::String(0).decode(&strings).is_ok());
    }
}
